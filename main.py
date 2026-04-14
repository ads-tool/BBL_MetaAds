import uuid
import subprocess
import asyncio
import sys
import json
import os
import psycopg2
from db_ingest import ingest_excel_to_postgres
from const import COUNTRY_MAPPING, STATUS_MAPPING
from typing import Dict
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv 
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import time
load_dotenv() 

app = FastAPI(title="Meta Ads Pipeline API")

executor = ThreadPoolExecutor(max_workers=3)
tasks_db: Dict[str, dict] = {}

class PipelineInput(BaseModel):
    raw_text: str
    country: str = "Tất cả"      
    status: str = "Đang chạy"     
    min_impressions: int = 100    

def run_pipeline_worker(task_id: str, raw_input: str, iso_country: str, meta_status: str, min_impressions: int):
    tasks_db[task_id]["status"] = "processing"
    tasks_db[task_id]["current_action"] = "Đang khởi tạo trình duyệt và kết nối Meta Library..."
    
    cmd = [
        sys.executable, "run_meta_ads_pipeline.py",
        "--input", raw_input,
        "--country", iso_country,
        "--status", meta_status,
        "--min-impressions", str(min_impressions) 
    ]
    
    try:
        # Thay subprocess.run bằng Popen
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8')
        
        full_stdout = ""
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            full_stdout += line
            
            if "[PROGRESS_REPORT]" in line:
                msg = line.split("[PROGRESS_REPORT]")[-1].strip()
                tasks_db[task_id]["current_action"] = msg 
                    
        process.wait()
        
        if process.returncode == 0:
            tasks_db[task_id]["status"] = "completed"
            tasks_db[task_id]["current_action"] = "Đã hoàn thành toàn bộ Pipeline."
            tasks_db[task_id]["result_logs"] = full_stdout

            try:
                lines = full_stdout.strip().split('\n')
                for line in reversed(lines):
                    if line.startswith('{'):
                        result_json = json.loads(line)
                        excel_path = result_json.get("artifacts", {}).get("excel_path")
                        staged_path = result_json.get("artifacts", {}).get("staged_for_send_path")
                        if excel_path:
                            db_config = {
                                "host": os.getenv("DB_HOST", "localhost"),
                                "port": int(os.getenv("DB_PORT", 5432)),
                                "dbname": os.getenv("DB_NAME", "postgres"),
                                "user": os.getenv("DB_USER", "postgres"),
                                "password": os.getenv("DB_PASSWORD", "")
                            }
                            ingest_excel_to_postgres(excel_path, db_config)
                            staging_dir = os.path.dirname(staged_path)
                            new_staged_path = os.path.join(staging_dir, f"meta_ads_result_{task_id}.xlsx")
                            
                            if os.path.exists(staged_path):
                                os.rename(staged_path, new_staged_path)
                            else:
                                new_staged_path = staged_path
                            tasks_db[task_id]["message"] = f"Pipeline chạy và Insert DB thành công (Quốc gia: {iso_country})."
                            tasks_db[task_id]["download_path"] = new_staged_path
                        break
            except Exception as db_err:
                tasks_db[task_id]["status"] = "partial"
                tasks_db[task_id]["error"] = f"Cào file thành công nhưng lỗi lưu Postgres: {str(db_err)}"
        else:
            tasks_db[task_id]["status"] = "failed"
            tasks_db[task_id]["error"] = full_stdout
            tasks_db[task_id]["current_action"] = "Tiến trình gặp lỗi."
            
    except Exception as e:
        tasks_db[task_id]["status"] = "error"
        tasks_db[task_id]["error"] = str(e)
        tasks_db[task_id]["current_action"] = "Lỗi hệ thống nghiêm trọng."

def cleanup_downloaded_file(file_path: str):
    """Xóa file ngay sau khi người dùng đã tải xuống thành công."""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            print(f"[CLEANUP] Đã xóa file sau khi tải: {file_path}")
    except Exception as e:
        print(f"[CLEANUP ERROR] Không thể xóa file {file_path}: {e}")

@app.post("/api/v1/run-pipeline")
async def trigger_pipeline(payload: PipelineInput):
    if not payload.raw_text.strip():
        raise HTTPException(status_code=400, detail="Input không được để trống")
    
    iso_country = COUNTRY_MAPPING.get(payload.country.strip(), "ALL")
    meta_status = STATUS_MAPPING.get(payload.status.strip(), "ACTIVE")
        
    task_id = str(uuid.uuid4())
    
    tasks_db[task_id] = {
        "status": "pending",
        "current_action": "Đang đưa vào hàng đợi...",
        "input_text": payload.raw_text,
        "mapped_country": iso_country,
        "mapped_status": meta_status,
        "min_impressions": payload.min_impressions
    }
    
    loop = asyncio.get_running_loop()
    loop.run_in_executor(executor, run_pipeline_worker, task_id, payload.raw_text, iso_country, meta_status, payload.min_impressions)
    
    return {
        "task_id": task_id, 
        "status": "pending", 
        "message": "Đã đưa vào hàng đợi.",
        "debug_info": f"Hệ thống ghi nhận: {iso_country} - {meta_status}"
    }

@app.get("/api/v1/status/{task_id}")
async def get_task_status(task_id: str):
    task = tasks_db.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Không tìm thấy task")
    return task

@app.get("/api/v1/download/{task_id}")
async def download_result_file(task_id: str, background_tasks: BackgroundTasks):
    """API để Frontend gọi khi người dùng bấm nút 'Tải File'."""
    file_path = None
    
    # --- LUỒNG 1: KIỂM TRA NHANH TRÊN RAM ---
    if task_id in tasks_db:
        task = tasks_db[task_id]
        if task.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Task chưa hoàn thành, chưa có file.")
        file_path = task.get("download_path")
        
    # --- LUỒNG 2: FALLBACK XUỐNG DATABASE VÀ GHÉP TÊN FILE ---
    else:
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", 5432)),
                dbname=os.getenv("DB_NAME", "postgres"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", "")
            )
            cursor = conn.cursor()
            
            cursor.execute("SELECT status FROM crawl_tasks WHERE upstream_task_id = %s", (task_id,))
            result = cursor.fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail="Không tìm thấy task_id này trong hệ thống.")
            
            db_status = str(result[0]).strip().upper()
            if db_status != 'COMPLETED':
                raise HTTPException(status_code=400, detail="Task hiện tại chưa hoàn thành, không có file.")
            
            file_path = os.path.join(os.getcwd(), "file_send_meta", f"meta_ads_result_{task_id}.xlsx")
            
        except psycopg2.Error as e:
            raise HTTPException(status_code=500, detail=f"Lỗi truy xuất database: {str(e)}")
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Dữ liệu báo đã hoàn thành nhưng file vật lý không tồn tại hoặc đã bị xóa.")

    file_name = os.path.basename(file_path)

    # background_tasks.add_task(cleanup_downloaded_file, file_path)

    return FileResponse(
        path=file_path,
        filename=file_name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={file_name}"}
    )

# uvicorn main:app --host 192.168.1.68 --port 8000 
# nohup uvicorn main:app --host 192.168.1.68 --port 8000 > api.log 2>&1 &

