#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import importlib
import json
import os
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
import shutil
import select
import io
import logging
from typing import Optional

# Set up basic logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

URL_RE = re.compile(r"https?://[^\s<>()\[\]\"']+")
ID_RE = re.compile(r"\d{6,}")

REQUIRED_PACKAGES = [
    ("pandas", "pandas"),
    ("openpyxl", "openpyxl"),
    ("requests", "requests"),
    ("meta_ads_collector", "meta-ads-collector"),
    ("langdetect", "langdetect"),
    ("pycountry", "pycountry"),
    ("google.generativeai", "google-generativeai"),
    ("python-dotenv", "python-dotenv"),
]

def ensure_dependencies() -> None:
    missing = []
    for module_name, package_name in REQUIRED_PACKAGES:
        try:
            importlib.import_module(module_name)
        except Exception:
            missing.append(package_name)

    if not missing:
        return

    cmd = [sys.executable, "-m", "pip", "install", *sorted(set(missing))]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            "Dependency bootstrap failed: "
            + (p.stderr or p.stdout or "pip install failed")
        )

@dataclass(frozen=True, order=True)
class InputItem:
    kind: str = field(compare=True)
    value: str = field(compare=True)


def get_meta_ads_workspace() -> Path:
    return Path(__file__).resolve().parent


def extract_inputs(raw: str) -> list[InputItem]:
    urls = URL_RE.findall(raw or "")
    ids = ID_RE.findall(raw or "")
    seen: set[tuple[str, str]] = set()
    out: list[InputItem] = []

    for u in urls:
        key = ("page-link", u)
        if key not in seen:
            seen.add(key)
            out.append(InputItem("page-link", u))

    for x in ids:
        key = ("page-id", x)
        if key not in seen:
            seen.add(key)
            out.append(InputItem("page-id", x))
            
    # Sort the list to ensure deterministic order for fingerprinting
    return sorted(out)


def _fingerprint_inputs(inputs: list[InputItem], max_ads: int | None, country: str, status: str, start_date: str | None, end_date: str | None) -> str:
    payload = {
        "inputs": [{"kind": x.kind, "value": x.value} for x in inputs],
        "max_ads": max_ads,
        "country": country,
        "status": status,
        "start_date": start_date,
        "end_date": end_date,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _state_file_path(run_dir: Path, fingerprint: str) -> Path:
    return run_dir / "outputs" / f"meta_ads_pipeline_state_{fingerprint}.json"


def _load_state(state_file: Path) -> dict:
    if not state_file.exists():
        return {"version": 2, "runs": []}
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("runs"), list):
            return data
    except Exception:
        pass
    return {"version": 2, "runs": []}


def _save_state(state_file: Path, state: dict) -> None:
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(state_file)


def _input_key(item: InputItem | dict) -> str:
    if isinstance(item, InputItem):
        return f"{item.kind}:{item.value}"
    ii = item.get("input", {}) if isinstance(item, dict) else {}
    return f"{ii.get('kind','')}:{ii.get('value','')}"


def _resolve_path(base_dir: Path, p: str | None) -> Path | None:
    if not p:
        return None
    pp = Path(p)
    if not pp.is_absolute():
        pp = base_dir / pp
    return pp


def _result_artifacts_exist(output_dir: Path, run: dict) -> bool:
    rr = run.get("result", {}) if isinstance(run, dict) else {}
    if not str(rr.get("status")).startswith("success"):
        return False
    ep = _resolve_path(output_dir, rr.get("excel_path"))
    jp = _resolve_path(output_dir, rr.get("crawl_json_path"))
    return bool(ep and ep.exists() and jp and jp.exists())


def run_dogbot(run_dir: Path, dogbot_script: Path, item: InputItem, max_ads: int | None = None, country: str = "ALL", status: str = "ACTIVE", min_impressions: int = 100, no_transcript: bool = False, start_date: str = None, end_date: str = None) -> dict:
    output_dir = run_dir / "outputs"
    cmd = ["python3", str(dogbot_script), "--output-dir", str(output_dir), "--country", country, "--status", status, "--min-impressions", str(min_impressions)]
    if no_transcript:
        cmd.append("--no-transcript")
    if start_date:
        cmd.extend(["--start-date", start_date])
    if end_date:
        cmd.extend(["--end-date", end_date])
        
    if item.kind == "page-link":
        cmd += ["--page-link", item.value]
    else:
        cmd += ["--page-id", item.value]
    if max_ads is not None:
        cmd += ["--max-ads", str(max_ads)]

    # This logic must match the checkpoint naming scheme in dogbot_pipeline.py
    safe_value = re.sub(r"[^a-zA-Z0-9_-]", "_", str(item.value))[:120]
    safe_max = "all" if max_ads is None else str(max_ads)
    checkpoint_filename = f"dogbot_video_checkpoint_{item.kind}_{safe_value}_{safe_max}_{country}.json"
    dogbot_checkpoint_path = output_dir / checkpoint_filename

    process = None
    full_stdout, full_stderr = "", ""
    try:
        # IMPORTANT: CWD is the isolated run directory to contain all file operations.
        process = subprocess.Popen(
            cmd,
            cwd=str(run_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout_reader = io.TextIOWrapper(process.stdout, encoding='utf-8', errors='replace')
        stderr_reader = io.TextIOWrapper(process.stderr, encoding='utf-8', errors='replace')

        last_output_time = time.time()
        inactivity_timeout = 1800  # 30 minutes
        max_runtime_timeout = 5400 # 90 minutes

        while True:
            if time.time() - last_output_time > max_runtime_timeout:
                raise subprocess.TimeoutExpired(cmd, max_runtime_timeout, "Exceeded maximum total runtime.")
            if time.time() - last_output_time > inactivity_timeout:
                raise subprocess.TimeoutExpired(cmd, inactivity_timeout, "Process hung due to inactivity.")

            ready_to_read, _, _ = select.select([stdout_reader, stderr_reader], [], [], 1.0)
            if ready_to_read:
                for stream in ready_to_read:
                    line = stream.readline()
                    if line:
                        last_output_time = time.time()
                        if stream is stdout_reader:
                            full_stdout += line
                            if "[PROGRESS_REPORT]" in line:
                                print(line.strip(), flush=True)
                            logging.info(f"[DOGBOT STDOUT] {line.strip()}")
                        else:
                            full_stderr += line
                            logging.warning(f"[DOGBOT STDERR] {line.strip()}")
            if process.poll() is not None:
                break
        
        remaining_stdout, remaining_stderr = process.communicate()
        full_stdout += remaining_stdout.decode('utf-8', 'replace')
        full_stderr += remaining_stderr.decode('utf-8', 'replace')
        exit_code = process.returncode
        output_to_parse = full_stdout

    except subprocess.TimeoutExpired as e:
        logging.error(f"Timeout expired for dogbot process: {e.reason}")
        if process:
            process.terminate()
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill()
        
        if dogbot_checkpoint_path.exists():
            logging.info(f"Salvaging partial results from checkpoint: {dogbot_checkpoint_path}")
            try:
                import pandas as pd
                ck_data = json.loads(dogbot_checkpoint_path.read_text(encoding="utf-8"))
                rows = ck_data.get("rows", [])
                if rows and isinstance(rows, list):
                    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                    salvaged_excel = output_dir / f"meta_ads_salvaged_{ts}.xlsx"
                    pd.DataFrame(rows).to_excel(salvaged_excel, index=False)
                    parsed = {
                        "status": "success_partial_timeout",
                        "excel_path": str(salvaged_excel),
                        "crawl_json_path": "N/A",
                        "rows_total": len(rows),
                        "error": {"message": f"Process timed out due to inactivity after {e.timeout}s but salvaged {len(rows)} rows."}
                    }
                    return {"input": {"kind": item.kind, "value": item.value}, "exit_code": -1, "result": parsed}
            except Exception as salvage_err:
                logging.error(f"Failed to salvage partial results: {salvage_err}", exc_info=True)

        parsed = {"status": "failed", "error": {"step": "dogbot_pipeline_timeout", "message": f"Process timed out after {e.timeout}s. No partial results.", "stderr": full_stderr}}
        return {"input": {"kind": item.kind, "value": item.value}, "exit_code": -1, "result": parsed}
    
    except Exception as e:
        logging.error(f"An unexpected error occurred while running dogbot: {e}", exc_info=True)
        if process: process.terminate()
        parsed = {"status": "failed", "error": { "step": "dogbot_pipeline_unhandled", "message": str(e), "stderr": full_stderr }}
        return {"input": {"kind": item.kind, "value": item.value}, "exit_code": 1, "result": parsed}

    lines = [ln.strip() for ln in output_to_parse.splitlines() if ln.strip()]
    parsed = None
    for ln in reversed(lines):
        try:
            p = json.loads(ln)
            if isinstance(p, dict) and "status" in p:
                parsed = p
                break
        except Exception:
            continue

    if parsed is None:
        parsed = {"status": "failed", "error": {"step": "dogbot_pipeline_json_parse", "message": (full_stderr or full_stdout or "DogBot execution failed").strip()}}

    return {"input": {"kind": item.kind, "value": item.value}, "exit_code": exit_code, "result": parsed}


def _canonical_video_key(v: object) -> str:
    s = str(v or "").strip()
    if not s or s.upper() == "N/A" or s.lower() == "nan":
        return ""
    return s.split("?", 1)[0]


def merge_artifacts(output_dir: Path, success_runs: list[dict]) -> tuple[Optional[Path], Optional[Path], int]:
    import pandas as pd

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    merged_excel = output_dir / f"meta_ads_merged_{ts}.xlsx"
    merged_json = output_dir / f"meta_ads_crawl_merged_{ts}.json"
    frames, crawls = [], []

    for r in success_runs:
        rr = r.get("result", {})
        excel_p = _resolve_path(output_dir, rr.get("excel_path"))
        json_p = _resolve_path(output_dir, rr.get("crawl_json_path"))

        if excel_p and excel_p.exists():
            df = pd.read_excel(excel_p)
            df["source_input_kind"] = r["input"]["kind"]
            df["source_input_value"] = r["input"]["value"]
            frames.append(df)

        if json_p and json_p.exists():
            with json_p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        item.setdefault("source_input_kind", r["input"]["kind"])
                        item.setdefault("source_input_value", r["input"]["value"])
                crawls.extend(data)

    if not frames:
        return None, None, 0

    merged_df = pd.concat(frames, ignore_index=True)
    if "video_url" in merged_df.columns:
        merged_df["_video_key"] = merged_df["video_url"].map(_canonical_video_key)
        
        # 1. Đếm tổng số lần xuất hiện của video
        merged_df['duplicate_count'] = merged_df.groupby('_video_key')['_video_key'].transform('size')
        
        # 2. Tạo mã nhóm (GROUP_001, GROUP_002) cho các video trùng
        dup_videos = merged_df[merged_df['duplicate_count'] > 1]['_video_key'].unique()
        group_mapping = {vk: f"GROUP_{i+1:03d}" for i, vk in enumerate(dup_videos) if vk != ""}
        
        def assign_group(row):
            vk = row['_video_key']
            if vk == "" or row['duplicate_count'] == 1:
                return "UNIQUE"
            return group_mapping.get(vk, "UNIQUE")
            
        merged_df['duplicate_group'] = merged_df.apply(assign_group, axis=1)
        
        # Xóa cột tạm
        merged_df = merged_df.drop(columns=["_video_key"])
    else:
        merged_df["duplicate_count"] = 0
        merged_df["duplicate_group"] = "UNIQUE"

    if "ad_id_full" in merged_df.columns:
        merged_df = merged_df.drop_duplicates(subset=["ad_id_full"], keep="first")

    cols = merged_df.columns.tolist()
    if "duplicate_count" in cols and "video_url" in cols:
        cols.remove("duplicate_count")
        cols.insert(cols.index("video_url") + 1, "duplicate_count")
        
    if "duplicate_group" in cols and "duplicate_count" in cols:
        cols.remove("duplicate_group")
        cols.insert(cols.index("duplicate_count") + 1, "duplicate_group") 

    merged_df = merged_df[cols]
    
    merged_df.to_excel(merged_excel, index=False)
    rows_total = len(merged_df)

    with merged_json.open("w", encoding="utf-8") as f:
        json.dump(crawls, f, ensure_ascii=False, indent=2, sort_keys=True)

    return merged_excel, merged_json, rows_total


def run_pipeline_in_isolated_dir(
    run_dir: Path,
    dogbot_script: Path,
    inputs: list[InputItem],
    max_ads: int | None,
    fingerprint: str,
    country: str = "ALL",    
    status: str = "ACTIVE",
    min_impressions: int = 100,
    no_transcript: bool = False,
    start_date: str = None,
    end_date: str = None
) -> dict:
    
    output_dir = run_dir / "outputs"
    (run_dir / "video_downloaded").mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    state_file = _state_file_path(run_dir, fingerprint)
    state = _load_state(state_file)
    state.update({"version": 2, "fingerprint": fingerprint, "updated_at": dt.datetime.now().isoformat()})

    runs: list[dict] = []
    completed_success_by_key: dict[str, dict] = {}

    for prev in state.get("runs", []):
        k = _input_key(prev)
        if k and _result_artifacts_exist(output_dir, prev):
            completed_success_by_key[k] = prev

    for i, item in enumerate(inputs):
        k = _input_key(item)
        if k in completed_success_by_key:
            reused = dict(completed_success_by_key[k])
            reused["reused_from_checkpoint"] = True
            runs.append(reused)
            continue

        r = run_dogbot(run_dir, dogbot_script, item, max_ads=max_ads, country=country, status=status, min_impressions=min_impressions, no_transcript=no_transcript, start_date=start_date, end_date=end_date)
        runs.append(r)

        state["runs"] = runs
        state["updated_at"] = dt.datetime.now().isoformat()
        _save_state(state_file, state)
        
        # If this is not the last item, sleep for a random duration
        if i < len(inputs) - 1:
            delay = random.randint(5, 15)
            logging.info(f"Finished processing '{item.value}'. Waiting for {delay} seconds before next input...")
            time.sleep(delay)

    success_runs = [r for r in runs if str(r.get("result", {}).get("status", "")).startswith("success")]
    failed_runs = [r for r in runs if r not in success_runs]
    status_runs = "success" if not failed_runs else ("failed" if not success_runs else "partial")

    merged_excel, merged_json, rows_total = None, None, 0
    if success_runs:
        merged_excel, merged_json, rows_total = merge_artifacts(output_dir, success_runs)

    state.update({"runs": runs, "final_status": status_runs, "updated_at": dt.datetime.now().isoformat()})
    _save_state(state_file, state)

    return {
        "status": status_runs,
        "summary": f"Completed {len(success_runs)}/{len(runs)} run(s). Rows merged: {rows_total}.",
        "artifacts_transient": {
            "excel_path": str(merged_excel) if merged_excel else None,
            "crawl_json_path": str(merged_json) if merged_json else None,
        },
        "error": None if not failed_runs else {"failed_inputs": [r['input'] for r in failed_runs]},
        "runs": runs,
        "checkpoint": {
            "fingerprint": fingerprint,
            "state_file": str(state_file),
            "reused_runs": sum(1 for r in runs if r.get("reused_from_checkpoint")),
        },
    }

def main() -> None:
    ap = argparse.ArgumentParser(description="Run full Meta Ads pipeline from raw user input")
    ap.add_argument("--input", required=True, help="Raw user text containing links or IDs")
    ap.add_argument("--max-ads", type=int, default=None)
    ap.add_argument("--country", type=str, default="ALL")
    ap.add_argument("--status", type=str, default="ACTIVE")
    ap.add_argument("--min-impressions", type=int, default=100)
    ap.add_argument("--no-transcript", action="store_true")
    ap.add_argument("--start-date", type=str, default=None)
    ap.add_argument("--end-date", type=str, default=None)
    args = ap.parse_args()

    ensure_dependencies()

    meta_ads_workspace = get_meta_ads_workspace()
    base_run_dir = meta_ads_workspace / "dogbot" / "dogbot_runs"
    final_output_dir = meta_ads_workspace / "dogbot" / "dogbot_final_outputs"
    send_staging_dir = meta_ads_workspace / "file_send_meta"
    dogbot_script = Path(__file__).resolve().parent / "dogbot_pipeline.py"
    
    base_run_dir.mkdir(parents=True, exist_ok=True)
    final_output_dir.mkdir(parents=True, exist_ok=True)
    send_staging_dir.mkdir(parents=True, exist_ok=True)

    inputs = extract_inputs(args.input)
    if not inputs:
        print(json.dumps({
            "status": "failed",
            "summary": "No valid Meta link or numeric ID found in input.",
            "error": {"message": "No valid input extracted."},
        }, ensure_ascii=False))
        return

    fingerprint = _fingerprint_inputs(inputs, args.max_ads, args.country, args.status, args.start_date, args.end_date)
    run_dir = base_run_dir / fingerprint

    try:
        result = run_pipeline_in_isolated_dir(
            run_dir=run_dir,
            dogbot_script=dogbot_script,
            inputs=inputs,
            max_ads=args.max_ads,
            fingerprint=fingerprint,
            country=args.country,  
            status=args.status,
            min_impressions=args.min_impressions,
            no_transcript=args.no_transcript,
            start_date=args.start_date,
            end_date=args.end_date
        )
    except Exception as e:
        logging.error(f"Pipeline execution failed with unhandled exception: {e}", exc_info=True)
        result = {
            "status": "failed",
            "summary": "Pipeline failed with an unexpected error.",
            "error": {"message": str(e)},
            "runs": [],
        }

    # --- Final artifact handling ---
    final_excel_path, final_json_path = None, None
    transient_artifacts = result.pop("artifacts_transient", {})
    
    if result.get("status") != "failed" and transient_artifacts.get("excel_path"):
        try:
            transient_excel = Path(transient_artifacts["excel_path"])
            transient_json = Path(transient_artifacts["crawl_json_path"])

            # 1. Move final artifacts to persistent storage
            final_excel_path = final_output_dir / transient_excel.name
            final_json_path = final_output_dir / transient_json.name
            shutil.move(transient_excel, final_excel_path)
            shutil.move(transient_json, final_json_path)

            # 2. Stage Excel file for sending
            staged_for_send_path = send_staging_dir / final_excel_path.name
            shutil.copy(final_excel_path, staged_for_send_path)
            
            result["artifacts"] = {
                "excel_path": str(final_excel_path),
                "crawl_json_path": str(final_json_path),
                "staged_for_send_path": str(staged_for_send_path)
            }
            
            # 3. Clean up the isolated run directory
            logging.info(f"Cleaning up run directory: {run_dir}")
            shutil.rmtree(run_dir)

        except Exception as e:
            logging.error(f"Failed during final artifact handling: {e}", exc_info=True)
            result["status"] = "partial" # Success but failed cleanup/move
            result["error"] = result.get("error", {})
            result["error"]["artifact_handling"] = f"Failed to move/stage final files: {e}"
            # Keep run_dir for debugging if artifact handling fails
    
    elif result.get("status") == "failed":
        logging.warning(f"Pipeline status is 'failed'. Preserving run directory for debugging: {run_dir}")

    result["paths"] = {
        "run_dir": str(run_dir),
        "final_output_dir": str(final_output_dir),
        "send_staging_dir": str(send_staging_dir),
    }

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
