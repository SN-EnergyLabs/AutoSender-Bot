"""
FastAPI Server for WhatsApp Automation Bot Dashboard.
Provides REST endpoints and real-time Server-Sent Events (SSE) streaming for
campaign logs, queue progress, and browser controls.
"""

import os
import sys
import glob
import json
import csv
import io
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

import re
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Add current directory to path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CURRENT_DIR)

from engine import WhatsAppEngine, clean_phone_number, normalize_message_text

app = FastAPI(title="SN Energy Labs - WhatsApp Auto Sender")

# Global engine instance
engine = WhatsAppEngine()

# Mount static files
STATIC_DIR = os.path.join(CURRENT_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# SSE event queues for connected clients
connected_clients: List[asyncio.Queue] = []
async_loop: Optional[asyncio.AbstractEventLoop] = None


@app.on_event("startup")
async def on_startup():
    global async_loop
    async_loop = asyncio.get_running_loop()

    # Restore active campaign queue if backup exists
    backup_file = os.path.join(CURRENT_DIR, "active_campaign_backup.csv")
    if os.path.isfile(backup_file):
        try:
            with open(backup_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                items = []
                sent_count = 0
                sample_msg = ""
                for row in reader:
                    st = row.get("status")
                    if st in ["failed", "sending"]:
                        st = "pending"
                    if st == "sent":
                        sent_count += 1
                    if not sample_msg and row.get("message"):
                        sample_msg = row.get("message")
                    items.append({
                        "id": int(row.get("id") or len(items) + 1),
                        "name": row.get("name") or "Customer",
                        "raw_phone": row.get("phone") or "",
                        "clean_phone": row.get("clean_phone") or "",
                        "address": row.get("address") or "",
                        "category": row.get("category") or "",
                        "rating": row.get("rating") or "",
                        "reviews": "",
                        "website": "",
                        "status": st,
                        "message": row.get("message") or "",
                        "sent_at": row.get("sent_at") or None,
                        "error": None
                    })
            if items:
                engine.queue = items
                if sample_msg:
                    engine.message_template = sample_msg
                engine.stats["total"] = len(items)
                engine.stats["sent"] = sent_count
                engine.stats["failed"] = 0
                engine.stats["not_on_whatsapp"] = 0
                engine.stats["pending"] = sum(1 for c in items if c["status"] == "pending")
                print(f"[STARTUP] Restored active queue: {len(items)} contacts ({sent_count} sent, {engine.stats['pending']} pending).")
        except Exception as e:
            print(f"[STARTUP] Backup restoration note: {e}")

    # Auto-attach to open Chrome session
    try:
        engine.launch_browser()
    except Exception as e:
        print(f"[STARTUP] Chrome attach note: {e}")


def forward_engine_event(event_type: str, data: Dict[str, Any]):
    """Thread-safe forwarder from Selenium thread to async SSE queues."""
    global async_loop
    if not async_loop or not connected_clients:
        return

    payload = {
        "event": event_type,
        "data": data,
        "timestamp": datetime.now().strftime("%H:%M:%S")
    }
    json_data = json.dumps(payload)

    for q in list(connected_clients):
        try:
            async_loop.call_soon_threadsafe(q.put_nowait, json_data)
        except Exception:
            pass


# Subscribe engine to forward events
engine.subscribe(forward_engine_event)


# ==========================================
# ==========================================
# Universal File & Leads Parser Helpers
# ==========================================
def _records_to_contact_items(records: List[Dict[str, Any]], default_country_code: str = "91") -> List[Dict[str, Any]]:
    """Intelligently maps list of dict records (from CSV, JSON, or Excel) to standardized contact items."""
    if not records:
        return []

    # Map keys from first few records
    field_map = {}
    sample = {}
    for r in records[:15]:
        if isinstance(r, dict):
            sample.update(r)

    for k in sample.keys():
        if not k:
            continue
        lower_k = str(k).strip().lower()
        if any(w in lower_k for w in ["phone", "mobile", "contact", "tel", "whatsapp"]):
            field_map["phone"] = k
        elif any(w in lower_k for w in ["name", "title", "business", "clinic", "company", "store", "shop"]):
            field_map["name"] = k
        elif any(w in lower_k for w in ["address", "loc", "city", "place"]):
            field_map["address"] = k
        elif "category" in lower_k:
            field_map["category"] = k
        elif "rating" in lower_k:
            field_map["rating"] = k
        elif "review" in lower_k:
            field_map["reviews"] = k
        elif "website" in lower_k:
            field_map["website"] = k

    items = []
    for row in records:
        if not isinstance(row, dict):
            continue

        raw_phone = str(row.get(field_map.get("phone", "phone")) or "").strip()
        if not raw_phone:
            # Fallback: look for phone-like value across columns
            for val in row.values():
                val_str = str(val or "").strip()
                if val_str and re.search(r"\d{8,}", val_str):
                    raw_phone = val_str
                    break

        if not raw_phone or raw_phone.lower() in ["nan", "none", "null"]:
            continue

        clean_phone = clean_phone_number(raw_phone, default_country_code=default_country_code)
        name_val = row.get(field_map.get("name", "name"))
        name = str(name_val).strip() if name_val is not None and str(name_val).strip() and str(name_val).lower() != "nan" else "Business"

        def get_field_val(key_name):
            val = row.get(field_map.get(key_name, key_name))
            if val is None or str(val).lower() == "nan":
                return ""
            return str(val).strip()

        items.append({
            "name": name,
            "phone": raw_phone,
            "clean_phone": clean_phone,
            "address": get_field_val("address"),
            "category": get_field_val("category"),
            "rating": get_field_val("rating"),
            "reviews": get_field_val("reviews"),
            "website": get_field_val("website")
        })
    return items


def parse_csv_text(csv_text: str, default_country_code: str = "91") -> List[Dict[str, Any]]:
    """Intelligently maps CSV headers and extracts cleaned phone numbers safely."""
    reader = csv.DictReader(io.StringIO(csv_text))
    return _records_to_contact_items(list(reader), default_country_code=default_country_code)


def parse_any_file_content(content_bytes: bytes, filename: str, default_country_code: str = "91") -> List[Dict[str, Any]]:
    """Universal lead file parser supporting .csv, .xlsx, .xls, .json, and .txt files."""
    fn_lower = filename.lower()

    # 1. Excel files (.xlsx, .xls)
    if fn_lower.endswith((".xlsx", ".xls")):
        import pandas as pd
        df = pd.read_excel(io.BytesIO(content_bytes))
        records = df.fillna("").to_dict(orient="records")
        return _records_to_contact_items(records, default_country_code=default_country_code)

    # 2. JSON files (.json)
    if fn_lower.endswith(".json"):
        text = content_bytes.decode("utf-8-sig", errors="replace")
        data = json.loads(text)
        if isinstance(data, dict):
            for k in ["leads", "results", "businesses", "contacts", "data", "items"]:
                if k in data and isinstance(data[k], list):
                    data = data[k]
                    break
            else:
                data = [data]
        if isinstance(data, list):
            return _records_to_contact_items(data, default_country_code=default_country_code)
        return []

    # 3. Plain text files (.txt)
    if fn_lower.endswith(".txt"):
        text = content_bytes.decode("utf-8-sig", errors="replace")
        if "," in text:
            return parse_csv_text(text, default_country_code=default_country_code)
        return engine.parse_raw_numbers(text, default_country_code=default_country_code)

    # 4. Default: CSV
    try:
        text = content_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content_bytes.decode("latin-1", errors="replace")
    return parse_csv_text(text, default_country_code=default_country_code)


# ==========================================
# Pydantic Request Models
# ==========================================
class QueueRequest(BaseModel):
    items: List[Dict[str, Any]]
    message_template: str
    delay_min: int = 7
    delay_max: int = 14
    dispatch_mode: str = "new_chat"  # 'new_chat' or 'direct_url'
    default_country_code: str = "91"


class ParseRawRequest(BaseModel):
    raw_text: str
    default_country_code: str = "91"


class LoadFileRequest(BaseModel):
    filepath: str
    default_country_code: str = "91"


# ==========================================
# Routes: Web UI & Server Status
# ==========================================
@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def serve_dashboard():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>WhatsApp Bot Dashboard: static/index.html not found</h1>", status_code=404)


@app.get("/api/status")
async def get_status():
    return {
        "state": engine.state,
        "is_ready": engine.is_browser_ready(),
        "stats": engine.stats,
        "current_index": engine.current_index,
        "current_contact": engine.current_contact,
        "queue_count": len(engine.queue),
        "dispatch_mode": engine.dispatch_mode,
        "delay_range": [engine.delay_min, engine.delay_max],
        "default_country_code": engine.default_country_code
    }


# ==========================================
# Routes: Browser Management
# ==========================================
@app.post("/api/browser/launch")
async def launch_browser(background_tasks: BackgroundTasks):
    background_tasks.add_task(engine.launch_browser)
    return {"status": "ok", "message": "Browser launch initiated"}


@app.post("/api/browser/close")
async def close_browser():
    engine.close_browser()
    return {"status": "ok", "message": "Browser closed"}


# ==========================================
# Routes: Leads & Numbers Parsing
# ==========================================
@app.get("/api/leads-files")
async def list_available_lead_files():
    """Finds all lead files (.csv, .xlsx, .json) in current workspace and business_scraper exports, newest first."""
    workspace_root = os.path.dirname(CURRENT_DIR)
    exports_dir = os.path.join(workspace_root, "business_scraper", "exports")

    found_files = []
    patterns = [
        os.path.join(workspace_root, "*.csv"),
        os.path.join(workspace_root, "*.xlsx"),
        os.path.join(workspace_root, "*.xls"),
        os.path.join(workspace_root, "*lead*.json"),
        os.path.join(workspace_root, "*dentist*.json"),
        os.path.join(exports_dir, "*.csv"),
        os.path.join(exports_dir, "*.xlsx"),
        os.path.join(exports_dir, "*.json")
    ]

    seen_paths = set()
    for pat in patterns:
        for f in glob.glob(pat):
            if f in seen_paths or not os.path.isfile(f):
                continue
            seen_paths.add(f)
            fname = os.path.basename(f)
            size_kb = round(os.path.getsize(f) / 1024, 1)
            mtime_val = os.path.getmtime(f)
            mod_time = datetime.fromtimestamp(mtime_val).strftime("%Y-%m-%d %H:%M")
            found_files.append({
                "filename": fname,
                "filepath": f,
                "size_kb": size_kb,
                "modified": mod_time,
                "mtime": mtime_val
            })

    # Sort descending by newest file first
    found_files.sort(key=lambda x: x["mtime"], reverse=True)
    return {"files": found_files}


@app.post("/api/leads-files/load")
async def load_lead_file(req: LoadFileRequest):
    """Loads contacts from any custom file path on the machine (CSV, Excel, JSON, TXT)."""
    raw_path = req.filepath.strip().strip("'\"").replace(r"\ ", " ")
    resolved_path = os.path.expanduser(raw_path)
    if not os.path.isabs(resolved_path):
        # Check workspace root first, then current dir
        workspace_root = os.path.dirname(CURRENT_DIR)
        test_ws = os.path.normpath(os.path.join(workspace_root, resolved_path))
        if os.path.exists(test_ws):
            resolved_path = test_ws
        else:
            resolved_path = os.path.normpath(os.path.join(CURRENT_DIR, resolved_path))

    # Defensive Security Validation: Allowed file extensions only
    allowed_exts = {".csv", ".xlsx", ".xls", ".json", ".txt"}
    ext = os.path.splitext(resolved_path)[1].lower()
    if ext not in allowed_exts:
        raise HTTPException(status_code=400, detail=f"Unsupported file format '{ext}'. Only CSV, Excel, JSON, and TXT files are allowed.")

    # Defensive Security Validation: Block sensitive credentials, keys, and profiles
    sensitive_markers = [".ssh", ".aws", ".env", ".pem", ".key", "id_rsa", "/etc/", "credentials", "chrome_profile"]
    if any(m in resolved_path.lower() for m in sensitive_markers):
        raise HTTPException(status_code=403, detail="Access to sensitive files or credentials is strictly prohibited.")

    if not os.path.exists(resolved_path):
        raise HTTPException(status_code=404, detail=f"File not found on system: {raw_path}")

    # Maximum file size limit: 50MB
    if os.path.getsize(resolved_path) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds maximum permitted limit (50MB).")

    try:
        with open(resolved_path, mode="rb") as f:
            raw_bytes = f.read()
        items = parse_any_file_content(
            content_bytes=raw_bytes,
            filename=os.path.basename(resolved_path),
            default_country_code=req.default_country_code
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read lead file: {str(e)}")

    return {
        "status": "ok",
        "total": len(items),
        "items": items,
        "filepath": resolved_path,
        "filename": os.path.basename(resolved_path)
    }


@app.post("/api/upload-file")
@app.post("/api/upload-csv")
async def upload_lead_file(
    file: UploadFile = File(...),
    default_country_code: str = Form("91")
):
    """Uploads and parses ANY custom CSV, Excel (.xlsx/.xls), JSON, or TXT file selected by the user."""
    filename = file.filename or "leads.csv"
    allowed_exts = {".csv", ".xlsx", ".xls", ".json", ".txt"}
    ext = os.path.splitext(filename)[1].lower()
    if ext not in allowed_exts:
        raise HTTPException(status_code=400, detail=f"Unsupported file format '{ext}'. Only CSV, Excel, JSON, and TXT files are allowed.")

    try:
        raw_bytes = await file.read()
        if len(raw_bytes) > 50 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Uploaded file exceeds maximum limit of 50MB.")
        items = parse_any_file_content(
            content_bytes=raw_bytes,
            filename=filename,
            default_country_code=default_country_code
        )
        return {
            "status": "ok",
            "filename": filename,
            "total": len(items),
            "items": items
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to process selected file: {str(e)}")



@app.post("/api/parse-raw-numbers")
async def parse_raw_numbers(req: ParseRawRequest):
    """
    Parses manually pasted numbers.
    Supports:
      - Line by line phone numbers
      - 'Name, Number' or 'Name \t Number' or 'Name; Number' formats
      - Comma / tab separated entries
    """
    lines = req.raw_text.splitlines()
    items = []
    seen = set()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        name = "Customer"
        raw_phone = line

        # Split on tab, comma, or semicolon
        sep = None
        if "\t" in line:
            sep = "\t"
        elif "," in line:
            sep = ","
        elif ";" in line:
            sep = ";"

        if sep:
            parts = [p.strip() for p in line.split(sep, 1)]
            p1_clean = clean_phone_number(parts[0], default_country_code=req.default_country_code)
            p2_clean = clean_phone_number(parts[1], default_country_code=req.default_country_code)

            if p2_clean and not p1_clean:
                name, raw_phone = parts[0], parts[1]
            elif p1_clean and not p2_clean:
                raw_phone, name = parts[0], parts[1]
            else:
                raw_phone = parts[0]
                name = parts[1]

        clean = clean_phone_number(raw_phone, default_country_code=req.default_country_code)
        if clean and clean not in seen:
            seen.add(clean)
            items.append({
                "name": name or "Customer",
                "phone": raw_phone,
                "clean_phone": clean,
                "address": "",
                "category": ""
            })

    return {"total": len(items), "items": items}


# ==========================================
# Routes: Campaign Controls
# ==========================================
@app.post("/api/campaign/queue")
async def setup_campaign_queue(req: QueueRequest):
    engine.set_queue(
        items=req.items,
        template=req.message_template,
        delay_min=req.delay_min,
        delay_max=req.delay_max,
        mode=req.dispatch_mode,
        default_cc=req.default_country_code
    )
    return {
        "status": "ok",
        "total": len(engine.queue),
        "queue": engine.queue,
        "stats": engine.stats
    }


class StartCampaignRequest(BaseModel):
    message_template: Optional[str] = None
    delay_min: Optional[int] = None
    delay_max: Optional[int] = None
    dispatch_mode: Optional[str] = None
    default_country_code: Optional[str] = None


@app.post("/api/campaign/start")
async def start_campaign(req: Optional[StartCampaignRequest] = None):
    if req:
        if req.message_template is not None:
            engine.message_template = normalize_message_text(req.message_template)
        if req.delay_min is not None:
            engine.delay_min = max(3, req.delay_min)
        if req.delay_max is not None:
            engine.delay_max = max(engine.delay_min, req.delay_max)
        if req.dispatch_mode is not None:
            engine.dispatch_mode = req.dispatch_mode
        if req.default_country_code is not None:
            engine.default_country_code = req.default_country_code
    engine.start_campaign()
    return {"status": "ok", "state": engine.state}



@app.post("/api/campaign/pause")
async def pause_campaign():
    engine.pause()
    return {"status": "ok", "state": engine.state}


@app.post("/api/campaign/resume")
async def resume_campaign():
    engine.resume()
    return {"status": "ok", "state": engine.state}


@app.post("/api/campaign/stop")
async def stop_campaign():
    engine.stop()
    return {"status": "ok", "state": engine.state}


@app.post("/api/campaign/skip")
async def skip_campaign():
    engine.skip_current()
    return {"status": "ok"}


@app.post("/api/campaign/retry-failed")
async def retry_failed_contacts():
    count = engine.retry_failed()
    engine.emit("queue_updated", {"queue": engine.queue, "stats": engine.stats})
    return {"status": "ok", "reset_count": count, "stats": engine.stats}


@app.get("/api/export-results")
async def export_results():
    """Exports campaign execution results to CSV."""
    if not engine.queue:
        raise HTTPException(status_code=400, detail="Queue is empty")

    output = io.StringIO()
    fieldnames = ["id", "name", "phone", "clean_phone", "status", "sent_at", "category", "address", "rating", "error", "message"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for item in engine.queue:
        writer.writerow({
            "id": item.get("id"),
            "name": item.get("name"),
            "phone": item.get("raw_phone"),
            "clean_phone": item.get("clean_phone"),
            "status": item.get("status"),
            "sent_at": item.get("sent_at", ""),
            "category": item.get("category", ""),
            "address": item.get("address", ""),
            "rating": item.get("rating", ""),
            "error": item.get("error", ""),
            "message": item.get("message", "")
        })

    csv_data = output.getvalue()
    filename = f"whatsapp_campaign_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ==========================================
# Routes: Real-Time SSE Stream
# ==========================================
@app.get("/api/stream")
async def event_stream(request: Request):
    """Server-Sent Events endpoint pushing live updates to the frontend."""
    global async_loop
    async_loop = asyncio.get_running_loop()

    client_queue = asyncio.Queue()
    connected_clients.append(client_queue)

    async def event_generator():
        try:
            # Send initial state snapshot
            initial_event = {
                "event": "init_snapshot",
                "data": {
                    "state": engine.state,
                    "is_ready": engine.is_browser_ready(),
                    "stats": engine.stats,
                    "queue": engine.queue,
                    "dispatch_mode": engine.dispatch_mode,
                    "delay_min": engine.delay_min,
                    "delay_max": engine.delay_max,
                    "default_country_code": engine.default_country_code,
                    "message_template": engine.message_template
                }
            }
            yield f"data: {json.dumps(initial_event)}\n\n"

            while True:
                if await request.is_disconnected():
                    break

                try:
                    # Wait for next event with a 15-second heartbeat timeout
                    raw_data = await asyncio.wait_for(client_queue.get(), timeout=15.0)
                    yield f"data: {raw_data}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive ping
                    yield f": heartbeat\n\n"

        except asyncio.CancelledError:
            pass
        finally:
            if client_queue in connected_clients:
                connected_clients.remove(client_queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
