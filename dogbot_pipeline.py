#!/usr/bin/env python3
import argparse
import dataclasses
import datetime as dt
import itertools
import json
import os
import re
import subprocess
import shutil
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from meta_ads_collector import MetaAdsCollector, FilterConfig

try:
    from langdetect import detect as detect_lang
except Exception:
    detect_lang = None

try:
    import pycountry
except Exception:
    pycountry = None

try:
    import google.generativeai as genai
except Exception:
    genai = None

from dotenv import load_dotenv
load_dotenv()


def check_ffmpeg_installed():
    """Checks if ffmpeg is installed and provides installation instructions if not."""
    if shutil.which("ffmpeg"):
        # ffmpeg is found in PATH
        return

    print("---", file=sys.stderr)
    print("ERROR: ffmpeg is not installed or not in your system's PATH.", file=sys.stderr)
    
    platform = sys.platform
    if platform == "linux" or platform == "linux2":
        # Check if it's a Debian-based system by looking for apt-get
        if shutil.which("apt-get"):
            print("This skill requires ffmpeg to process audio.", file=sys.stderr)
            print("To install it on Debian/Ubuntu, please run this command:", file=sys.stderr)
            print("\n    sudo apt-get update && sudo apt-get install -y ffmpeg\n", file=sys.stderr)
        else:
            print("Please install ffmpeg using your system's package manager.", file=sys.stderr)
    elif platform == "darwin": # macOS
        if shutil.which("brew"):
            print("This skill requires ffmpeg to process audio.", file=sys.stderr)
            print("To install it with Homebrew, please run this command:", file=sys.stderr)
            print("\n    brew install ffmpeg\n", file=sys.stderr)
        else:
            print("This skill requires ffmpeg, which can be installed with Homebrew.", file=sys.stderr)
            print("First, install Homebrew (see https://brew.sh/), then run 'brew install ffmpeg'.", file=sys.stderr)
    elif platform == "win32":
        print("This skill requires ffmpeg to process audio.", file=sys.stderr)
        print("Please download it from https://ffmpeg.org/download.html and add it to your system's PATH.", file=sys.stderr)
    else:
        print(f"Unsupported platform '{platform}'. Please install ffmpeg manually.", file=sys.stderr)

    print("---", file=sys.stderr)
    sys.exit(1)


OUTPUT_COLUMNS = [
    "ad_id_full",
    "library_id_full",
    "crawl_date",
    "countries",
    "headline",
    "headline_language",
    "primary_text",
    "primary_text_language",
    "video_url",
    "duration",
    "transcript",
    "transcript_translated",
    "video_language",
    "gender_audience",
    "age_audience",
    "reach (EU)",
    "top3_reach",
    "cta_text",
    "cta_type",
    "app_link",
]

VIDEO_DIR = Path("video_downloaded")
VIDEO_DIR.mkdir(parents=True, exist_ok=True)


def retry_step(step_name: str, fn, retries: int = 3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt >= retries:
                raise RuntimeError(f"{step_name} failed after {attempt} attempts: {e}") from e
            time.sleep(min(2 * attempt, 5))
    raise RuntimeError(f"{step_name} failed: {last_err}")


def extract_page_id(page_link: str) -> Optional[str]:
    # Common Meta Ads Library pattern: ...?view_all_page_id=123456
    m = re.search(r"[?&]view_all_page_id=(\d+)", page_link)
    if m:
        return m.group(1)
    # fallback: last long numeric token
    m2 = re.search(r"(\d{5,})", page_link)
    return m2.group(1) if m2 else None


def all_country_codes() -> List[str]:
    if pycountry is not None:
        return sorted({c.alpha_2 for c in pycountry.countries if getattr(c, "alpha_2", None)})
    # fallback minimal list if pycountry is unavailable
    return ["US", "VN", "GB", "CA", "AU", "DE", "FR", "JP", "KR", "SG"]


def obj_to_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if dataclasses.is_dataclass(x):
        return dataclasses.asdict(x)
    if hasattr(x, "model_dump"):
        try:
            return x.model_dump()
        except Exception:
            pass
    if hasattr(x, "dict"):
        try:
            return x.dict()
        except Exception:
            pass
    if hasattr(x, "__dict__"):
        return dict(vars(x))
    return {}


def get_in(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        if k not in cur:
            return default
        cur = cur[k]
    return cur


def find_first_value(d: Any, candidate_keys: Iterable[str]) -> Optional[Any]:
    keys = set(candidate_keys)

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k in keys and v not in (None, ""):
                    return v
                out = walk(v)
                if out not in (None, ""):
                    return out
        elif isinstance(x, list):
            for it in x:
                out = walk(it)
                if out not in (None, ""):
                    return out
        return None

    return walk(d)


def pick_video_url(ad_dict: Dict[str, Any]) -> Optional[str]:
    # 1) Prefer normalized creatives from meta-ads-collector
    creatives = ad_dict.get("creatives") or []
    if isinstance(creatives, list):
        for c in creatives:
            if isinstance(c, dict):
                # Prefer SD first as requested.
                v = c.get("video_sd_url") or c.get("video_url") or c.get("video_hd_url")
                if v:
                    return v

    # 2) raw snapshot videos
    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    snap = raw.get("snapshot") if isinstance(raw.get("snapshot"), dict) else {}
    vids = snap.get("videos") if isinstance(snap.get("videos"), list) else []
    for v in vids:
        if isinstance(v, dict):
            u = v.get("video_sd_url") or v.get("video_url") or v.get("video_hd_url")
            if u:
                return u

    # 3) generic fallback
    return find_first_value(
        ad_dict,
        [
            "video_url",
            "videoUrl",
            "video_hd_url",
            "video_sd_url",
            "video_uri",
            "source",
            "content_url",
        ],
    )


def pick_cta_text(ad_dict: Dict[str, Any]) -> str:
    creatives = ad_dict.get("creatives") or []
    if isinstance(creatives, list):
        for c in creatives:
            if isinstance(c, dict) and c.get("cta_text"):
                return str(c.get("cta_text"))

    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    snap = raw.get("snapshot") if isinstance(raw.get("snapshot"), dict) else {}
    if snap.get("cta_text"):
        return str(snap.get("cta_text"))
    if raw.get("cta_text"):
        return str(raw.get("cta_text"))

    return str(find_first_value(ad_dict, ["cta_text", "call_to_action_text", "ctaLabel"]) or "")


def pick_cta_type(ad_dict: Dict[str, Any]) -> str:
    creatives = ad_dict.get("creatives") or []
    if isinstance(creatives, list):
        for c in creatives:
            if isinstance(c, dict) and c.get("cta_type"):
                return str(c.get("cta_type"))

    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    snap = raw.get("snapshot") if isinstance(raw.get("snapshot"), dict) else {}
    if snap.get("cta_type"):
        return str(snap.get("cta_type"))
    if raw.get("cta_type"):
        return str(raw.get("cta_type"))

    return str(find_first_value(ad_dict, ["cta_type", "call_to_action_type", "ctaType"]) or "")


def pick_app_link(ad_dict: Dict[str, Any]) -> str:
    creatives = ad_dict.get("creatives") or []
    if isinstance(creatives, list):
        for c in creatives:
            if isinstance(c, dict) and c.get("link_url"):
                return str(c.get("link_url"))

    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    snap = raw.get("snapshot") if isinstance(raw.get("snapshot"), dict) else {}
    if snap.get("link_url"):
        return str(snap.get("link_url"))
    if raw.get("link_url"):
        return str(raw.get("link_url"))

    return str(find_first_value(ad_dict, ["app_link", "app_url", "landing_page_url", "link_url", "url"]) or "")


def detect_text_language_with_gemini(model_names: List[str], text: str) -> str:
    t = (text or "").strip()
    if not t or t == "":
        return ""

    prompt = (
        "Detect the language of this text and return ONLY ISO 639-1 code in lowercase "
        "(e.g., en, vi, id, th, fr). If uncertain, return und. Text: "
        f"{t}"
    )

    def _do():
        last_err = None
        for model_name in model_names:
            try:
                model = genai.GenerativeModel(model_name)
                rsp = model.generate_content(
                    [prompt, uploaded],
                    request_options={"timeout": 300}
                )
                code = (rsp.text or "").strip().lower()
                code = re.sub(r"[^a-z-]", "", code)
                if 2 <= len(code) <= 5:
                    return code
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"Language detect via Gemini failed: {last_err}")

    try:
        return retry_step("detect_language_gemini", _do, retries=2)
    except Exception:
        return ""


def detect_text_language(text: str, gemini_models: Optional[List[str]] = None) -> str:
    t = (text or "").strip()
    if not t or t == "":
        return ""

    word_count = len(t.split())

    # Rule: short text (<10 words) -> Gemini, long text -> langdetect.
    if word_count < 10 and gemini_models:
        return detect_text_language_with_gemini(gemini_models, t)

    if detect_lang is None:
        return ""
    try:
        return detect_lang(t)
    except Exception:
        return ""


def pick_headline(ad_dict: Dict[str, Any]) -> str:
    creatives = ad_dict.get("creatives") or []
    if isinstance(creatives, list):
        for c in creatives:
            if isinstance(c, dict) and c.get("title"):
                return str(c.get("title"))

    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    snap = raw.get("snapshot") if isinstance(raw.get("snapshot"), dict) else {}
    if snap.get("title"):
        return str(snap.get("title"))

    return str(find_first_value(ad_dict, ["title", "headline", "ad_creative_link_titles"]) or "")


def pick_primary_text(ad_dict: Dict[str, Any]) -> str:
    creatives = ad_dict.get("creatives") or []
    if isinstance(creatives, list):
        for c in creatives:
            if isinstance(c, dict) and c.get("body"):
                return str(c.get("body"))

    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    snap = raw.get("snapshot") if isinstance(raw.get("snapshot"), dict) else {}
    if snap.get("body"):
        return str(snap.get("body"))

    return str(find_first_value(ad_dict, ["body", "primary_text", "ad_creative_bodies"]) or "")


def pick_impressions(ad_dict: Dict[str, Any]) -> str:
    # normalized impressions
    val = ad_dict.get("impressions")
    if isinstance(val, dict):
        lo = val.get("lower_bound") or val.get("lower") or val.get("min")
        hi = val.get("upper_bound") or val.get("upper") or val.get("max")
        if lo or hi:
            return f"{lo or ''}-{hi or ''}".strip("-")

    # raw fallback from snapshot/impressions_with_index
    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    iwi = raw.get("impressions_with_index") if isinstance(raw.get("impressions_with_index"), dict) else {}
    txt = iwi.get("impressions_text")
    if txt:
        return str(txt)

    val2 = find_first_value(ad_dict, ["impressions", "impression", "impression_range", "impressions_range"])
    if isinstance(val2, dict):
        lo = val2.get("lower_bound") or val2.get("lower") or val2.get("min")
        hi = val2.get("upper_bound") or val2.get("upper") or val2.get("max")
        if lo or hi:
            return f"{lo or ''}-{hi or ''}".strip("-")
        return ""
    if val2 is None:
        return ""
    return str(val2)


def download_video(url: str, target: Path):
    def _do():
        with requests.get(url, timeout=90, stream=True) as r:
            r.raise_for_status()
            with target.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 512):
                    if chunk:
                        f.write(chunk)

    retry_step("download_video", _do, retries=3)


def probe_duration_seconds(video_path: Path) -> str:
    def _do():
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(video_path),
        ]
        # Use subprocess.run for better error handling and stream management
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
        
        if result.returncode != 0:
            # ffprobe can write to stderr even on success with some formats, so check stdout first.
            if result.stdout.strip():
                 # Try to parse stdout even if exit code is non-zero
                 pass
            else:
                # If no stdout, it's a real error.
                error_message = result.stderr or result.stdout or "ffprobe failed with no output"
                raise RuntimeError(f"ffprobe failed with exit code {result.returncode}: {error_message.strip()}")

        out = result.stdout.strip()
        return str(int(float(out))) if out else ""

    try:
        return retry_step("probe_duration", _do, retries=3)
    except Exception:
        return ""


def setup_gemini_models() -> List[str]:
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("Missing GEMINI_API_KEY env var")
    if genai is None:
        raise RuntimeError("google-generativeai not installed")
    genai.configure(api_key=key)

    # User requirement: prioritize Gemini 2.5 Flash for video analysis.
    # Keep fallbacks to avoid hard failure if temporary model/API issues occur.
    return [
        "gemini-2.5-flash-lite",
        "gemini-2.5-flash",
        "gemini-flash-latest",
        "gemini-2.5-pro",
    ]


def wait_for_uploaded_file_active(file_obj, timeout_seconds: int = 120):
    start = time.time()
    name = getattr(file_obj, "name", None)
    if not name:
        return file_obj

    while True:
        current = genai.get_file(name)
        state = str(getattr(getattr(current, "state", None), "name", ""))
        if state == "ACTIVE":
            return current
        if state in {"FAILED", "STATE_UNSPECIFIED"}:
            raise RuntimeError(f"Gemini file upload failed with state={state}")
        if time.time() - start > timeout_seconds:
            raise RuntimeError(f"Gemini file did not become ACTIVE within {timeout_seconds}s (state={state})")
        time.sleep(2)


def extract_audio_from_video(video_path: Path) -> Path:
    """Extracts audio from a video file using ffmpeg and returns the path to the audio file."""
    audio_path = video_path.with_suffix(".mp3")
    cmd = [
        "ffmpeg",
        "-i", str(video_path),
        "-q:a", "0",  # high quality VBR
        "-vn",  # Bỏ video stream, tự động xử lý nếu có audio
        "-y",         # overwrite output file
        str(audio_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
    except FileNotFoundError:
        raise RuntimeError("ffmpeg not found. Please install ffmpeg and ensure it's in the system's PATH.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"ffmpeg failed to extract audio: {e.stderr}")
    
    if not audio_path.exists() or audio_path.stat().st_size == 0:
        raise RuntimeError("ffmpeg ran but the output audio file is missing or empty.")
        
    return audio_path


def gemini_transcribe_and_analyze(model_names: List[str], video_path: Path) -> Dict[str, Any]:
    prompt = (
        "Transcribe this audio. Return strict JSON with keys: "
        "transcript (ORIGINAL LANGUAGE), "
        "transcript_translated (TRANSLATE TO VIETNAMESE), and "
        "video_language (full language name, e.g., 'English', 'Vietnamese'). "
        "If no speech, all values should be empty string. "
        "REMEMBER TO TRANSLATE IT TO VIETNAMESE AND RETURN THE TRANSLATED TEXT IN THE transcript_translated KEY."
        "Do not include markdown fences."
    )

    audio_path = None
    try:
        audio_path = extract_audio_from_video(video_path)

        def _do():
            uploaded = genai.upload_file(path=str(audio_path))
            uploaded = wait_for_uploaded_file_active(uploaded, timeout_seconds=180)
            last_err = None
            for model_name in model_names:
                try:
                    model = genai.GenerativeModel(model_name)
                    rsp = model.generate_content(
                        [prompt, uploaded],
                        request_options={"timeout": 300}
                    )
                    txt = (rsp.text or "").strip()
                    txt = re.sub(r"^```json\s*|\s*```$", "", txt, flags=re.MULTILINE)
                    data = json.loads(txt)
                    return {
                        "transcript": data.get("transcript", "") or "",
                        "transcript_translated": data.get("transcript_translated", "") or "",
                        "video_language": data.get("video_language", "") or "",
                    }
                except Exception as e:
                    last_err = e
                    continue
            raise RuntimeError(f"All Gemini models failed. Last error: {last_err}")

        return retry_step("gemini_analyze_audio", _do, retries=1)
    finally:
        # Clean up the temporary audio file
        if audio_path and audio_path.exists():
            try:
                audio_path.unlink()
            except OSError:
                pass


def extract_countries_from_ad(ad_dict: Dict[str, Any], fallback_country: str) -> list[str]:
    # 1) Prefer normalized `countries` field from meta_ads_collector.
    countries = ad_dict.get("countries")
    if isinstance(countries, list) and countries:
        vals = [str(x).strip() for x in countries if str(x).strip()]
        if vals:
            return sorted(set(vals))

    # 2) Fallback to raw targeted/reached countries.
    raw = ad_dict.get("raw_data") if isinstance(ad_dict.get("raw_data"), dict) else {}
    tr_countries = raw.get("targeted_or_reached_countries")
    if isinstance(tr_countries, list) and tr_countries:
        vals = [str(x).strip() for x in tr_countries if str(x).strip()]
        if vals:
            return sorted(set(vals))

    # 3) Fallback to region_distribution.
    region_dist = ad_dict.get("region_distribution")
    if isinstance(region_dist, list) and region_dist:
        vals = []
        for r in region_dist:
            if isinstance(r, dict):
                c = r.get("country") or r.get("country_code") or r.get("category")
                if c:
                    vals.append(str(c).strip())
        if vals:
            return sorted(set(vals))

    # 4) No fallback country injection.
    # If collector payload has no country signal, keep countries empty.
    return []


def pick_gender_audience(ad_dict: Dict[str, Any]) -> str:
    v = ad_dict.get("gender_audience")
    if v in (None, ""):
        return ""
    return str(v)


def pick_age_audience(ad_dict: Dict[str, Any]) -> str:
    v = ad_dict.get("age_audience")
    if v in (None, ""):
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def pick_eu_total_reach(ad_dict: Dict[str, Any]) -> str:
    v = ad_dict.get("eu_total_reach")
    if v in (None, ""):
        return ""
    return str(v)


def parse_eu_total_reach_lower_bound(ad_dict: Dict[str, Any]) -> Optional[int]:
    v = ad_dict.get("eu_total_reach")
    if v in (None, "", ""):
        return None

    if isinstance(v, (int, float)):
        try:
            return int(v)
        except Exception:
            return None

    if isinstance(v, dict):
        for k in ["lower_bound", "lower", "min", "from", "start"]:
            x = v.get(k)
            if isinstance(x, (int, float)):
                return int(x)
        return None

    s = str(v).strip()
    if not s or s.upper() == "":
        return None

    nums = re.findall(r"\d+", s)
    if not nums:
        return None
    try:
        return int(nums[0])
    except Exception:
        return None


def pick_top3_reach(ad_dict: Dict[str, Any]) -> str:
    v = ad_dict.get("top3_reach")
    if v in (None, ""):
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def crawl_ads_from_page(page_link: Optional[str], page_id: Optional[str], output_dir: Path, max_ads: Optional[int] = None, country: str = "ALL", status: str = "ACTIVE", start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Tuple[list[str], Any]]:
    if not page_id and page_link:
        page_id = extract_page_id(page_link)
    if not page_id:
        raise ValueError("Cannot resolve page_id. Provide --page-id or a valid page link with id.")

    rows = []
    seen_ad_ids = set()

    with MetaAdsCollector() as collector:
        def _crawl_all():
            tmp_json = output_dir / f"_tmp_collect_{page_id}_{country}.json"
            
            filters = None
            if start_date or end_date:
                filter_kwargs = {}
                if start_date:
                    filter_kwargs["start_date"] = dt.datetime.strptime(start_date, "%Y-%m-%d")
                if end_date:
                    filter_kwargs["end_date"] = dt.datetime.strptime(end_date, "%Y-%m-%d")
                
                filters = FilterConfig(**filter_kwargs)

            try:
                collector.collect_to_json(
                    str(tmp_json),
                    query="",
                    country=country,  
                    page_ids=[str(page_id)],
                    status=status,    
                    filter_config=filters, # <--- Truyền object filters vào đây thay vì chuỗi
                    max_results=None,
                )
                with tmp_json.open("r", encoding="utf-8") as f:
                    payload = json.load(f)
                ads_data = payload.get("ads") if isinstance(payload, dict) else None
                return ads_data if isinstance(ads_data, list) else []
            finally:
                try:
                    tmp_json.unlink(missing_ok=True)
                except Exception:
                    pass

        ads = retry_step(f"crawl_country_{country}", _crawl_all, retries=3)

        for ad in ads:
            ad_dict = obj_to_dict(ad)
            ad_key = str(find_first_value(ad_dict, ["id", "ad_id", "ad_archive_id", "library_id"]) or "")
            if not ad_key or ad_key in seen_ad_ids:
                continue
            seen_ad_ids.add(ad_key)
            derived_countries = extract_countries_from_ad(ad_dict, fallback_country="ALL")
            rows.append((derived_countries, ad))
            if max_ads is not None and len(rows) >= max_ads:
                return rows

    return rows


def canonical_video_key(video_url: Optional[str]) -> str:
    u = str(video_url or "").strip()
    if not u or u.upper() == "":
        return ""
    return u.split("?", 1)[0]


def load_seen_video_keys(output_dir: Path) -> set[str]:
    p = output_dir / "video_seen_keys.json"
    if not p.exists():
        return set()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return {str(x).strip() for x in data if str(x).strip()}
    except Exception:
        pass
    return set()


def save_seen_video_keys(output_dir: Path, seen: set[str]) -> None:
    p = output_dir / "video_seen_keys.json"
    p.write_text(json.dumps(sorted(seen), ensure_ascii=False, indent=2), encoding="utf-8")


def _analyze_video_download_and_gemini(
    video_url: str,
    safe_id: str,
    gemini_models: List[str],
    no_transcript: bool,
) -> Dict[str, Any]:
    """Download 1 video, probe duration, (optional) transcribe qua Gemini.

    Trả về dict {duration, transcript, transcript_translated, video_language}.
    Luôn cleanup file video tạm trong finally. An toàn gọi song song từ nhiều thread
    vì safe_id là duy nhất cho mỗi creative (child_ad_id).
    """
    video_path = VIDEO_DIR / f"{safe_id}.mp4"
    try:
        retry_step("download_video", lambda: download_video(video_url, video_path), retries=2)
        duration = probe_duration_seconds(video_path)

        if no_transcript:
            return {
                "duration": duration,
                "transcript": "",
                "transcript_translated": "",
                "video_language": "",
            }

        gem = retry_step(
            "gemini_transcribe_and_analyze",
            lambda: gemini_transcribe_and_analyze(gemini_models, video_path),
            retries=1,
        )
        return {
            "duration": duration,
            "transcript": gem["transcript"],
            "transcript_translated": gem["transcript_translated"],
            "video_language": gem["video_language"],
        }
    finally:
        if video_path.exists():
            try:
                video_path.unlink()
            except OSError:
                pass


def _get_or_analyze_video(
    video_key: str,
    video_url: str,
    safe_id: str,
    gemini_models: List[str],
    no_transcript: bool,
    video_cache: Dict[str, "Future[Dict[str, Any]]"],
    video_cache_lock: threading.Lock,
) -> Dict[str, Any]:
    """Thread-safe video analysis với dedup qua Future.

    Thread đầu tiên claim video_key sẽ thực sự tải+phân tích; các thread khác đang
    cần cùng video_key sẽ block chờ trên Future. Nếu owner fail, Future bị xóa
    khỏi cache để thread kế tiếp (hoặc retry) có thể thử lại.
    """
    if not video_key:
        return _analyze_video_download_and_gemini(video_url, safe_id, gemini_models, no_transcript)

    with video_cache_lock:
        fut = video_cache.get(video_key)
        if fut is None:
            fut = Future()
            video_cache[video_key] = fut
            owner = True
        else:
            owner = False

    if owner:
        try:
            result = _analyze_video_download_and_gemini(video_url, safe_id, gemini_models, no_transcript)
            fut.set_result(result)
            return result
        except Exception as e:
            with video_cache_lock:
                if video_cache.get(video_key) is fut:
                    del video_cache[video_key]
            fut.set_exception(e)
            raise
    else:
        try:
            return fut.result(timeout=300) 
        except TimeoutError:
            raise RuntimeError(f"Chờ phân tích video {video_key} quá lâu (timeout).")


def build_row(parent_countries: list[str], ad_dict: Dict[str, Any], creative: Dict[str, Any], gemini_models: List[str], video_cache: Dict[str, "Future[Dict[str, Any]]"], video_cache_lock: threading.Lock, no_transcript: bool = False) -> Dict[str, Any]:
    # Lấy ID phân tách rõ ràng cho Database
    parent_id = str(find_first_value(ad_dict, ["id", "ad_id", "ad_archive_id"]) or "")
    child_id = str(creative.get("child_ad_id") or parent_id)

    # Lấy nội dung ưu tiên từ Thẻ Con
    headline = str(creative.get("title") or pick_headline(ad_dict))
    primary_text = str(creative.get("body") or pick_primary_text(ad_dict))
    cta_text = str(creative.get("cta_text") or pick_cta_text(ad_dict))
    cta_type = str(creative.get("cta_type") or pick_cta_type(ad_dict))
    app_link = str(creative.get("link_url") or pick_app_link(ad_dict))

    v_sd = creative.get("video_sd_url")
    v_url = creative.get("video_url")
    v_hd = creative.get("video_hd_url")
    video_url = str(v_sd or v_url or v_hd or pick_video_url(ad_dict))
    if video_url == "None": 
        video_url = ""

    # Lấy Insight TỪ TRONG THẺ CON
    eu_reach = creative.get("eu_total_reach")
    if eu_reach is None: 
        eu_reach = pick_eu_total_reach(ad_dict)
        
    top3 = creative.get("top3_reach")
    if top3 is None: 
        top3 = pick_top3_reach(ad_dict)
    elif isinstance(top3, (dict, list)): 
        top3 = json.dumps(top3, ensure_ascii=False)
        
    gender = creative.get("gender_audience")
    if gender is None: 
        gender = pick_gender_audience(ad_dict)
        
    age = creative.get("age_audience")
    if age is None: 
        age = pick_age_audience(ad_dict)
    elif isinstance(age, (dict, list)): 
        age = json.dumps(age, ensure_ascii=False)

    c_countries = creative.get("countries")
    if isinstance(c_countries, list) and c_countries:
        merged_countries = c_countries
    else:
        merged_countries = parent_countries

    row = {
        "ad_id_full": child_id,        # Đảm bảo Unique Key cho db_ingest
        "library_id_full": parent_id,  # Lưu vết ID Cụm/Chiến dịch gốc
        "crawl_date": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "countries": format_countries_display(merged_countries),
        "headline": headline,
        "headline_language": detect_text_language(headline, gemini_models),
        "primary_text": primary_text,
        "primary_text_language": detect_text_language(primary_text, gemini_models),
        "video_url": video_url or "",
        "duration": "",
        "transcript": "",
        "transcript_translated": "",
        "video_language": "",
        "gender_audience": str(gender),
        "age_audience": str(age),
        "reach (EU)": str(eu_reach),
        "top3_reach": str(top3),
        "cta_text": cta_text,
        "cta_type": cta_type,
        "app_link": app_link,
    }

    if not video_url or video_url == "":
        return row

    video_key = canonical_video_key(video_url)
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", child_id)[:80]

    analysis = _get_or_analyze_video(
        video_key=video_key,
        video_url=video_url,
        safe_id=safe_id,
        gemini_models=gemini_models,
        no_transcript=no_transcript,
        video_cache=video_cache,
        video_cache_lock=video_cache_lock,
    )
    row["duration"] = analysis["duration"]
    row["transcript"] = analysis["transcript"]
    row["transcript_translated"] = analysis["transcript_translated"]
    row["video_language"] = analysis["video_language"]
    return row


def country_code_to_name(code_or_name: str) -> str:
    s = str(code_or_name or "").strip()
    if not s:
        return ""
    # Already looks like a full name.
    if len(s) > 3:
        return s
    cc = s.upper()
    if pycountry is None:
        return cc
    try:
        c = pycountry.countries.get(alpha_2=cc)
        if c and getattr(c, "name", None):
            return c.name
    except Exception:
        pass
    return cc


def format_countries_display(countries: list[str]) -> str:
    vals = []
    for x in countries or []:
        n = country_code_to_name(str(x).strip())
        if n:
            vals.append(n)
    uniq = []
    seen = set()
    for x in vals:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
    return ", ".join(uniq)


def _parse_countries_cell(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v or "").strip()
    if not s or s in {"", "None", "nan", "[]"}:
        return []
    try:
        data = json.loads(s)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    # Support both comma and pipe-delimited inputs.
    if "|" in s:
        return [x.strip() for x in s.split("|") if x.strip()]
    return [x.strip() for x in s.split(",") if x.strip()]


def merge_countries_value(existing: Any, new_countries: list[str]) -> str:
    vals = _parse_countries_cell(existing)
    vals.extend([country_code_to_name(str(x).strip()) for x in (new_countries or []) if str(x).strip()])
    uniq = []
    seen = set()
    for x in vals:
        k = str(x).strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(str(x).strip())
    return ", ".join(uniq)


def format_labels_display(labels: Any) -> str:
    vals = []
    if isinstance(labels, list):
        vals = [str(x).strip() for x in labels if str(x).strip()]
    elif labels not in (None, ""):
        vals = [str(labels).strip()]

    uniq = []
    seen = set()
    for x in vals:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
    return ", ".join(uniq)


def _resolve_input_identity(page_link: Optional[str], page_id: Optional[str]) -> tuple[str, str]:
    resolved_page_id = page_id
    if not resolved_page_id and page_link:
        resolved_page_id = extract_page_id(page_link)
    if resolved_page_id:
        return "page-id", str(resolved_page_id)
    if page_link:
        return "page-link", str(page_link)
    return "unknown", "unknown"


def _checkpoint_path(output_dir: Path, kind: str, value: str, max_ads: Optional[int], country: str) -> Path:
    safe_value = re.sub(r"[^a-zA-Z0-9_-]", "_", str(value))[:120]
    safe_max = "all" if max_ads is None else str(max_ads)
    return output_dir / f"dogbot_video_checkpoint_{kind}_{safe_value}_{safe_max}_{country}.json"


def _load_video_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _save_video_checkpoint(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _build_fallback_row(
    countries_list: list[str],
    ad_dict: Dict[str, Any],
    creative: Dict[str, Any],
    parent_key: str,
    child_key: str,
    gemini_models: List[str],
) -> Dict[str, Any]:
    """Fallback row khi build_row thất bại (lỗi mạng/video/Gemini).

    Giữ nguyên mọi metadata text/audience, chỉ để trống các field cần video.
    """
    headline = str(creative.get("title") or pick_headline(ad_dict))
    primary_text = str(creative.get("body") or pick_primary_text(ad_dict))
    cta_text = str(creative.get("cta_text") or pick_cta_text(ad_dict))
    cta_type = str(creative.get("cta_type") or pick_cta_type(ad_dict))
    app_link = str(creative.get("link_url") or pick_app_link(ad_dict))

    v_sd = creative.get("video_sd_url")
    video_url = str(v_sd or creative.get("video_url") or creative.get("video_hd_url") or pick_video_url(ad_dict))
    if video_url == "None":
        video_url = ""

    eu_reach = creative.get("eu_total_reach")
    if eu_reach is None:
        eu_reach = pick_eu_total_reach(ad_dict)

    top3 = creative.get("top3_reach")
    if top3 is None:
        top3 = pick_top3_reach(ad_dict)
    elif isinstance(top3, (dict, list)):
        top3 = json.dumps(top3, ensure_ascii=False)

    gender = creative.get("gender_audience")
    if gender is None:
        gender = pick_gender_audience(ad_dict)

    age = creative.get("age_audience")
    if age is None:
        age = pick_age_audience(ad_dict)
    elif isinstance(age, (dict, list)):
        age = json.dumps(age, ensure_ascii=False)

    c_countries = creative.get("countries")
    merged_countries = c_countries if (isinstance(c_countries, list) and c_countries) else countries_list

    return {
        "ad_id_full": child_key,
        "library_id_full": parent_key,
        "crawl_date": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "countries": format_countries_display(merged_countries),
        "headline": headline,
        "headline_language": detect_text_language(headline, gemini_models),
        "primary_text": primary_text,
        "primary_text_language": detect_text_language(primary_text, gemini_models),
        "video_url": video_url or "",
        "duration": "",
        "transcript": "",
        "transcript_translated": "",
        "video_language": "",
        "gender_audience": str(gender),
        "age_audience": str(age),
        "reach (EU)": str(eu_reach),
        "top3_reach": str(top3),
        "cta_text": cta_text,
        "cta_type": cta_type,
        "app_link": app_link,
    }


def run(page_link: Optional[str], page_id: Optional[str], output_dir: Path, max_ads: Optional[int] = None, country: str = "ALL", status: str = "ACTIVE", min_impressions: int = 100, no_transcript: bool = False, start_date: Optional[str] = None, end_date: Optional[str] = None, concurrency: int = 4):
    output_dir.mkdir(parents=True, exist_ok=True)
    gemini_models = setup_gemini_models()

    input_kind, input_value = _resolve_input_identity(page_link, page_id)
    ck_state_path = _checkpoint_path(output_dir, input_kind, input_value, max_ads, country)
    ck_state = _load_video_checkpoint(ck_state_path)

    ads = retry_step("crawl_ads", lambda: crawl_ads_from_page(page_link, page_id, output_dir, max_ads=max_ads, country=country, status=status, start_date=start_date, end_date=end_date), retries=2)

    rows = ck_state.get("rows", []) if isinstance(ck_state.get("rows"), list) else []
    failed_rows = int(ck_state.get("failed_rows", 0) or 0)
    skipped_duplicate_videos = int(ck_state.get("skipped_duplicate_videos", 0) or 0)
    skipped_low_reach = int(ck_state.get("skipped_low_reach", 0) or 0)
    completed_ad_keys = set(str(x) for x in (ck_state.get("completed_ad_keys") or []) if str(x).strip())

    seen_video_keys = load_seen_video_keys(output_dir)
    video_key_to_row_idx: Dict[str, int] = {}
    for i, r in enumerate(rows):
        if isinstance(r, dict):
            vk = canonical_video_key(r.get("video_url"))
            if vk and vk not in video_key_to_row_idx:
                video_key_to_row_idx[vk] = i

    # CƠ CHẾ CACHE CHÌA KHÓA (thread-safe): các thẻ con dùng chung 1 video chỉ bị
    # tải + phân tích AI 1 lần; thread sau sẽ chờ trên Future của thread đầu tiên.
    video_cache: Dict[str, "Future[Dict[str, Any]]"] = {}
    video_cache_lock = threading.Lock()

    # -------------------------------------------------------------------------
    # BƯỚC 1: TIỀN XỬ LÝ - QUY ĐỔI NULL = 0 VÀ LỌC MIN_IMPRESSIONS
    # -------------------------------------------------------------------------
    filtered_ads = []
    
    for countries_list, ad in ads:
        ad_dict = obj_to_dict(ad)
        parent_key = str(find_first_value(ad_dict, ["id", "ad_id", "ad_archive_id", "library_id"]) or "")

        creatives = ad_dict.get("creatives")
        if not creatives or not isinstance(creatives, list):
            creatives = [{}]

        valid_creatives = []
        
        # Duyệt qua TẤT CẢ các thẻ con của 1 thẻ cha
        for c_idx, c in enumerate(creatives, start=1):
            if not isinstance(c, dict): c = {}
            
            # CẤP ID ĐỘC LẬP: Tránh việc các thẻ con ghi đè checkpoint
            if not c.get("child_ad_id"):
                c["child_ad_id"] = f"{parent_key}_{c_idx}"

            child_key = str(c.get("child_ad_id"))
            
            if child_key in completed_ad_keys:
                continue

            # Lấy giá trị reach
            eu_reach_lb = parse_eu_total_reach_lower_bound(c)
            if eu_reach_lb is None and "eu_total_reach" not in c:
                eu_reach_lb = parse_eu_total_reach_lower_bound(ad_dict)

            # LOGIC CHUẨN: Coi null (None) là 0
            reach_val = eu_reach_lb if eu_reach_lb is not None else 0

            # Lọc dứt khoát
            if reach_val < min_impressions:
                skipped_low_reach += 1
                completed_ad_keys.add(child_key)
                # Dòng print dưới đây được comment lại để tránh rác console, bạn có thể mở ra nếu muốn debug
                # print(f"[INFO] Bỏ qua {child_key} do reach = {reach_val} < {min_impressions}", file=sys.stderr)
                continue

            valid_creatives.append(c)

        # CHỈ giữ lại thẻ Cha nào còn ít nhất 1 thẻ Con sống sót qua bộ lọc
        if valid_creatives:
            ad_dict["creatives"] = valid_creatives
            filtered_ads.append((countries_list, ad_dict))

    total_child_cards = sum(len(ad_dict.get("creatives", [])) for _, ad_dict in filtered_ads)
    total_filtered = len(filtered_ads)
    
    print(f"[PROGRESS_REPORT] Lọc xong! Còn {total_child_cards} thẻ hợp lệ (thuộc {total_filtered} Thẻ Cha). Bắt đầu tải và phân tích video...", flush=True)

    crawl_records = []
    for countries_list, ad_dict in filtered_ads:
        crawl_records.append({"countries": countries_list, "ad": ad_dict})

    # -------------------------------------------------------------------------
    # BƯỚC 2: CHẠY MAIN LOOP SONG SONG (ThreadPoolExecutor)
    # - Mỗi task = xử lý 1 thẻ con (1 creative).
    # - Video cache dùng Future-pattern để tránh 2 thread cùng tải 1 video.
    # - Rows/completed_ad_keys/failed_rows protected bằng state_lock.
    # - Checkpoint save định kỳ mỗi CHECKPOINT_EVERY task hoàn thành.
    # -------------------------------------------------------------------------
    tasks: List[Tuple[list[str], Dict[str, Any], Dict[str, Any], str, str]] = []
    for countries_list, ad_dict in filtered_ads:
        parent_key = str(find_first_value(ad_dict, ["id", "ad_id", "ad_archive_id", "library_id"]) or "")
        for creative in ad_dict.get("creatives") or []:
            child_key = str(creative.get("child_ad_id") or parent_key)
            if child_key and child_key in completed_ad_keys:
                continue
            tasks.append((countries_list, ad_dict, creative, parent_key, child_key))

    if not tasks:
        print(f"[PROGRESS_REPORT] Không có thẻ nào cần xử lý (tất cả đã hoàn thành ở checkpoint trước).", flush=True)

    effective_concurrency = max(1, int(concurrency or 1))
    state_lock = threading.Lock()
    progress_counter = itertools.count(1)
    CHECKPOINT_EVERY = 10
    EXCEL_CHECKPOINT_EVERY = 40

    def _save_json_checkpoint_snapshot() -> None:
        with state_lock:
            snapshot = {
                "version": 1,
                "input": {"kind": input_kind, "value": input_value},
                "max_ads": max_ads,
                "country": country,
                "completed_ad_keys": sorted(completed_ad_keys),
                "rows": list(rows),
                "failed_rows": failed_rows,
                "skipped_duplicate_videos": skipped_duplicate_videos,
                "skipped_low_reach": skipped_low_reach,
                "updated_at": dt.datetime.now().isoformat(),
            }
        try:
            _save_video_checkpoint(ck_state_path, snapshot)
        except Exception as save_err:
            print(f"[WARN] Không thể lưu JSON checkpoint: {save_err}", file=sys.stderr, flush=True)

    def _save_excel_checkpoint_snapshot() -> None:
        with state_lock:
            snapshot_rows = list(rows)
        if not snapshot_rows:
            return
        try:
            df = pd.DataFrame(snapshot_rows)
            for col in OUTPUT_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            df = df[OUTPUT_COLUMNS]
            ck = output_dir / "meta_ads_checkpoint.xlsx"
            df.to_excel(ck, index=False)
        except Exception as exc_err:
            print(f"[WARN] Không thể lưu Excel checkpoint: {exc_err}", file=sys.stderr, flush=True)

    def _worker(countries_list, ad_dict, creative, parent_key, child_key):
        current = next(progress_counter)
        print(
            f"[PROGRESS_REPORT] Đang xử lý thẻ số {current} / {total_child_cards} (ID: {child_key})",
            flush=True,
        )
        try:
            row = retry_step(
                "build_row",
                lambda: build_row(
                    countries_list,
                    ad_dict,
                    creative,
                    gemini_models,
                    video_cache,
                    video_cache_lock,
                    no_transcript,
                ),
                retries=2,
            )
            return ("success", child_key, row)
        except Exception as e:
            print(
                f"      -> [FALLBACK] Thẻ {child_key} lỗi khi phân tích video/Gemini: {e}",
                file=sys.stderr,
                flush=True,
            )
            fallback = _build_fallback_row(countries_list, ad_dict, creative, parent_key, child_key, gemini_models)
            return ("failed", child_key, fallback)

    completed_count = 0
    if tasks:
        with ThreadPoolExecutor(max_workers=effective_concurrency, thread_name_prefix="dogbot-video") as executor:
            future_map = {executor.submit(_worker, *t): t for t in tasks}
            for fut in as_completed(future_map):
                try:
                    outcome, child_key, row = fut.result()
                except Exception as worker_err:
                    print(f"[ERROR] Worker crash: {worker_err}", file=sys.stderr, flush=True)
                    continue

                with state_lock:
                    rows.append(row)
                    if outcome == "failed":
                        failed_rows += 1
                    if child_key:
                        completed_ad_keys.add(child_key)
                    completed_count += 1
                    should_save_json = (completed_count % CHECKPOINT_EVERY == 0)
                    should_save_excel = (completed_count % EXCEL_CHECKPOINT_EVERY == 0)

                if should_save_json:
                    _save_json_checkpoint_snapshot()
                if should_save_excel:
                    _save_excel_checkpoint_snapshot()

    _save_json_checkpoint_snapshot()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"meta_ads_{ts}.xlsx"
    crawl_json_path = output_dir / f"meta_ads_crawl_{ts}.json"

    def _export():
        df = pd.DataFrame(rows)
        for col in OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[OUTPUT_COLUMNS]
        df.to_excel(out_path, index=False)

    def _export_crawl_json():
        with crawl_json_path.open("w", encoding="utf-8") as f:
            json.dump(crawl_records, f, ensure_ascii=False, default=str, indent=2, sort_keys=True)

    retry_step("export_excel", _export, retries=3)
    retry_step("export_crawl_json", _export_crawl_json, retries=3)
    retry_step("save_seen_video_keys", lambda: save_seen_video_keys(output_dir, seen_video_keys), retries=3)

    try:
        ck_state_path.unlink(missing_ok=True)
    except Exception:
        pass

    return out_path, crawl_json_path, len(rows), failed_rows, skipped_duplicate_videos, skipped_low_reach


def main():
    check_ffmpeg_installed()
    ap = argparse.ArgumentParser(description="DogBot Meta Ads video analyzer")
    ap.add_argument("--page-link", type=str, default=None)
    ap.add_argument("--page-id", type=str, default=None)
    ap.add_argument("--output-dir", type=str, default="outputs")
    ap.add_argument("--max-ads", type=int, default=None)
    ap.add_argument("--country", type=str, default="ALL")
    ap.add_argument("--status", type=str, default="ACTIVE")
    ap.add_argument("--min-impressions", type=int, default=100)
    ap.add_argument("--no-transcript", action="store_true")
    ap.add_argument("--start-date", type=str, default=None)
    ap.add_argument("--end-date", type=str, default=None)
    ap.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Số thread song song xử lý video/Gemini (default 4).",
    )
    args = ap.parse_args()

    if bool(args.page_link) == bool(args.page_id):
        raise SystemExit("Provide exactly one of --page-link or --page-id")

    out_path, crawl_json_path, total, failed_rows, skipped_duplicate_videos, skipped_low_reach = run(
        args.page_link, args.page_id, Path(args.output_dir),
        max_ads=args.max_ads, country=args.country, status=args.status,
        min_impressions=args.min_impressions, no_transcript=args.no_transcript,
        start_date=args.start_date, end_date=args.end_date,
        concurrency=args.concurrency,
    )

    print(json.dumps({
        "status": "success",
        "excel_path": str(out_path),
        "crawl_json_path": str(crawl_json_path),
        "rows_total": total,
        "failed_rows": failed_rows,
        "skipped_duplicate_videos": skipped_duplicate_videos,
        "skipped_low_reach": skipped_low_reach,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
