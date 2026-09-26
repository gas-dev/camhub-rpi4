from __future__ import annotations

import json
import shutil
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

app = FastAPI(title="CamHub V1", version="0.2.0")
config_lock = threading.RLock()

DEFAULT_CONFIG = {
    "server_name": "CamHub-RPI4",
    "camera_id": "CAM01",
    "camera_name": "Camera 1",
    "upload_token": "change-me-now",
    "snapshot_interval_sec": 60,
    "event_video_sec": 30,
    "motion_enabled": False,
    "drive_enabled": False,
    "drive_remote": "gdrive",
    "drive_root": "CamHub",
    "retention_days": 7,
}


class ConfigModel(BaseModel):
    server_name: str
    camera_id: str
    camera_name: str
    upload_token: str
    snapshot_interval_sec: int = 60
    event_video_sec: int = 30
    motion_enabled: bool = False
    drive_enabled: bool = False
    drive_remote: str = "gdrive"
    drive_root: str = "CamHub"
    retention_days: int = 7


def save_config(cfg: dict[str, Any]) -> None:
    with config_lock:
        tmp = CONFIG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        tmp.replace(CONFIG_PATH)


def load_config() -> dict[str, Any]:
    with config_lock:
        if not CONFIG_PATH.exists():
            save_config(DEFAULT_CONFIG)
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def now_local() -> datetime:
    return datetime.now().astimezone()


def camera_day_dir(camera_id: str, dt: datetime) -> Path:
    path = DATA_DIR / camera_id / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
    path.mkdir(parents=True, exist_ok=True)
    return path


def metadata_path(image_path: Path) -> Path:
    return image_path.with_suffix(".json")


def write_metadata(path: Path, data: dict[str, Any]) -> None:
    metadata_path(path).write_text(json.dumps(data, indent=2), encoding="utf-8")


def update_metadata(path: Path, **updates: Any) -> None:
    meta_path = metadata_path(path)
    data: dict[str, Any] = {}
    if meta_path.exists():
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    data.update(updates)
    write_metadata(path, data)


def cloud_target_for(path: Path, cfg: dict[str, Any]) -> str:
    relative = path.relative_to(DATA_DIR)
    parent = relative.parent.as_posix()
    return f"{cfg['drive_remote']}:{cfg['drive_root'].strip('/')}/{parent}/{path.name}"


def upload_to_drive(path: Path) -> None:
    cfg = load_config()
    if not cfg.get("drive_enabled"):
        update_metadata(path, cloud_status="DISABLED")
        return

    target = cloud_target_for(path, cfg)
    update_metadata(path, cloud_status="UPLOADING", cloud_target=target)

    try:
        result = subprocess.run(
            [
                "rclone",
                "copyto",
                str(path),
                target,
                "--retries",
                "3",
                "--low-level-retries",
                "5",
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode == 0:
            update_metadata(
                path,
                cloud_status="SYNCED",
                cloud_synced_at=now_local().isoformat(),
                cloud_target=target,
            )
        else:
            update_metadata(
                path,
                cloud_status="ERROR",
                cloud_error=(result.stderr or result.stdout)[-2000:],
                cloud_target=target,
            )
    except Exception as exc:
        update_metadata(path, cloud_status="ERROR", cloud_error=str(exc), cloud_target=target)


def latest_jpg(camera_id: str | None = None) -> Path | None:
    base = DATA_DIR / camera_id if camera_id else DATA_DIR
    if not base.exists():
        return None
    files = list(base.rglob("*.jpg"))
    return max(files, key=lambda p: p.stat().st_mtime) if files else None


def recent_items(limit: int = 30) -> list[dict[str, Any]]:
    jpgs = sorted(
        DATA_DIR.rglob("*.jpg"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:limit]

    items: list[dict[str, Any]] = []
    for path in jpgs:
        meta: dict[str, Any] = {}
        meta_file = metadata_path(path)
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        items.append(
            {
                "file": path.name,
                "relative": path.relative_to(DATA_DIR).as_posix(),
                "camera_id": meta.get("camera_id"),
                "event_type": meta.get("event_type"),
                "captured_at": meta.get("captured_at"),
                "cloud_status": meta.get("cloud_status", "PENDING"),
                "size": path.stat().st_size,
            }
        )
    return items


DASHBOARD = r"""
<!doctype html>
<html lang="it">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CamHub V1</title>
<style>
body{font-family:Arial,sans-serif;background:#111;color:#eee;margin:0}
header{padding:18px 22px;background:#1b1b1b;border-bottom:1px solid #333}
main{padding:20px;max-width:1200px;margin:auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}
.card{background:#1b1b1b;border:1px solid #333;border-radius:12px;padding:16px}
h1,h2{margin-top:0}
img{width:100%;border-radius:8px;background:#000;min-height:180px;object-fit:contain}
label{display:block;margin-top:8px;font-size:13px;color:#bbb}
input{width:100%;box-sizing:border-box;padding:9px;margin-top:4px;background:#0d0d0d;color:#eee;border:1px solid #444;border-radius:6px}
button{margin-top:12px;padding:10px 14px;border:0;border-radius:7px;cursor:pointer}
.ok{color:#6f6}.bad{color:#f66}.muted{color:#999}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:7px;border-bottom:1px solid #333;text-align:left}
a{color:#8cc8ff}
</style>
</head>
<body>
<header><h1>CamHub V1</h1><div class="muted">Raspberry Pi camera acquisition server</div></header>
<main>
<div class="grid">
  <div class="card">
    <h2>Ultima immagine</h2>
    <img id="latest" alt="Nessuna immagine">
    <div id="latestInfo" class="muted"></div>
  </div>
  <div class="card">
    <h2>Stato</h2>
    <div id="status">Caricamento...</div>
    <button onclick="syncPending()">Sincronizza file pendenti</button>
  </div>
  <div class="card">
    <h2>Configurazione</h2>
    <label>Camera ID<input id="camera_id"></label>
    <label>Nome camera<input id="camera_name"></label>
    <label>Token upload<input id="upload_token"></label>
    <label>Snapshot ogni secondi<input id="snapshot_interval_sec" type="number"></label>
    <label>Durata video evento futura, secondi<input id="event_video_sec" type="number"></label>
    <label>Rclone remote<input id="drive_remote"></label>
    <label>Cartella Drive<input id="drive_root"></label>
    <label><input id="drive_enabled" type="checkbox" style="width:auto"> Google Drive attivo</label>
    <button onclick="saveConfig()">Salva configurazione</button>
    <div id="saveResult"></div>
  </div>
</div>

<div class="card" style="margin-top:16px">
<h2>Ultime acquisizioni</h2>
<table>
<thead><tr><th>Ora</th><th>Camera</th><th>Tipo</th><th>File</th><th>Cloud</th></tr></thead>
<tbody id="events"></tbody>
</table>
</div>
</main>
<script>
async function refresh(){
  const st=await fetch('/api/status').then(r=>r.json());
  document.getElementById('status').innerHTML =
    'Server: <b>'+st.server_name+'</b><br>'+
    'Foto archiviate: '+st.photo_count+'<br>'+
    'Spazio dati: '+st.data_mb+' MB<br>'+
    'Rclone: '+(st.rclone_available?'<span class="ok">OK</span>':'<span class="bad">NON TROVATO</span>')+'<br>'+
    'Drive: '+(st.drive_enabled?'ATTIVO':'DISATTIVO')+'<br>'+
    'Pendenti cloud: '+st.pending_cloud;

  if(st.latest_url){
    document.getElementById('latest').src=st.latest_url+'?t='+Date.now();
    document.getElementById('latestInfo').textContent=st.latest_name || '';
  }

  const cfg=await fetch('/api/config').then(r=>r.json());
  for(const k of ['camera_id','camera_name','upload_token','snapshot_interval_sec','event_video_sec','drive_remote','drive_root']){
    document.getElementById(k).value=cfg[k] ?? '';
  }
  document.getElementById('drive_enabled').checked=!!cfg.drive_enabled;

  const items=await fetch('/api/recent').then(r=>r.json());
  document.getElementById('events').innerHTML=items.map(x =>
    '<tr><td>'+(x.captured_at||'')+'</td><td>'+(x.camera_id||'')+'</td><td>'+(x.event_type||'')+
    '</td><td><a href="/data/'+x.relative+'" target="_blank">'+x.file+'</a></td><td>'+x.cloud_status+'</td></tr>'
  ).join('');
}

async function saveConfig(){
  const old=await fetch('/api/config').then(r=>r.json());
  const cfg={...old};
  for(const k of ['camera_id','camera_name','upload_token','drive_remote','drive_root']){
    cfg[k]=document.getElementById(k).value;
  }
  cfg.snapshot_interval_sec=parseInt(document.getElementById('snapshot_interval_sec').value);
  cfg.event_video_sec=parseInt(document.getElementById('event_video_sec').value);
  cfg.drive_enabled=document.getElementById('drive_enabled').checked;

  const response=await fetch('/api/config',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify(cfg)
  });

  document.getElementById('saveResult').textContent=response.ok?'Salvato':'Errore';
  refresh();
}

async function syncPending(){
  await fetch('/api/cloud/sync-pending',{method:'POST'});
  setTimeout(refresh,1000);
}

refresh();
setInterval(refresh,10000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD


@app.get("/api/config")
def get_config():
    return load_config()


@app.post("/api/config")
def set_config(cfg: ConfigModel):
    data = cfg.model_dump()
    if data["snapshot_interval_sec"] < 1:
        raise HTTPException(400, "snapshot_interval_sec must be >= 1")
    if data["event_video_sec"] < 1:
        raise HTTPException(400, "event_video_sec must be >= 1")
    save_config(data)
    return {"ok": True}


@app.post("/api/upload")
async def upload_image(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    camera_id: str | None = None,
    event_type: str = "periodic",
    x_cam_token: str | None = Header(default=None),
):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")

    camera = camera_id or cfg["camera_id"]

    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(415, "Only image uploads are accepted in V1")

    dt = now_local()
    out_dir = camera_day_dir(camera, dt)
    safe_event = "".join(
        char for char in event_type.upper() if char.isalnum() or char in "_-"
    )[:24] or "PHOTO"

    filename = f"{camera}_{safe_event}_{dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}.jpg"
    out_path = out_dir / filename

    with out_path.open("wb") as output:
        shutil.copyfileobj(file.file, output)

    meta = {
        "camera_id": camera,
        "event_type": event_type,
        "captured_at": dt.isoformat(),
        "received_at": now_local().isoformat(),
        "filename": filename,
        "size": out_path.stat().st_size,
        "cloud_status": "PENDING" if cfg.get("drive_enabled") else "DISABLED",
    }
    write_metadata(out_path, meta)

    if cfg.get("drive_enabled"):
        background_tasks.add_task(upload_to_drive, out_path)

    return {
        "ok": True,
        "file": filename,
        "relative": out_path.relative_to(DATA_DIR).as_posix(),
    }


@app.post("/api/upload/raw")
async def upload_raw_image(
    request: Request,
    background_tasks: BackgroundTasks,
    camera_id: str | None = None,
    event_type: str = "periodic",
    x_cam_token: str | None = Header(default=None),
):
    """Receive a raw JPEG body from low-resource camera nodes such as ESP32-CAM."""
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")

    content_type = request.headers.get("content-type", "")
    if not content_type.lower().startswith("image/jpeg"):
        raise HTTPException(415, "Content-Type must be image/jpeg")

    payload = await request.body()
    if not payload:
        raise HTTPException(400, "Empty JPEG body")
    if len(payload) > 8 * 1024 * 1024:
        raise HTTPException(413, "Image too large")

    camera = camera_id or cfg["camera_id"]
    dt = now_local()
    out_dir = camera_day_dir(camera, dt)
    safe_event = "".join(
        char for char in event_type.upper() if char.isalnum() or char in "_-"
    )[:24] or "PHOTO"

    filename = f"{camera}_{safe_event}_{dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}.jpg"
    out_path = out_dir / filename
    out_path.write_bytes(payload)

    meta = {
        "camera_id": camera,
        "event_type": event_type,
        "captured_at": dt.isoformat(),
        "received_at": now_local().isoformat(),
        "filename": filename,
        "size": len(payload),
        "source": "raw_jpeg",
        "cloud_status": "PENDING" if cfg.get("drive_enabled") else "DISABLED",
    }
    write_metadata(out_path, meta)

    if cfg.get("drive_enabled"):
        background_tasks.add_task(upload_to_drive, out_path)

    return {
        "ok": True,
        "file": filename,
        "relative": out_path.relative_to(DATA_DIR).as_posix(),
        "size": len(payload),
    }


@app.get("/api/recent")
def api_recent(limit: int = 30):
    return recent_items(max(1, min(limit, 200)))


@app.get("/api/status")
def api_status():
    cfg = load_config()
    jpgs = list(DATA_DIR.rglob("*.jpg"))
    total_bytes = sum(path.stat().st_size for path in jpgs)

    pending = 0
    for path in jpgs:
        meta_file = metadata_path(path)
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                if meta.get("cloud_status") in ("PENDING", "ERROR"):
                    pending += 1
            except Exception:
                pass

    latest = latest_jpg()

    return {
        "server_name": cfg["server_name"],
        "photo_count": len(jpgs),
        "data_mb": round(total_bytes / 1024 / 1024, 2),
        "rclone_available": shutil.which("rclone") is not None,
        "drive_enabled": cfg.get("drive_enabled", False),
        "pending_cloud": pending,
        "latest_name": latest.name if latest else None,
        "latest_url": (
            f"/data/{latest.relative_to(DATA_DIR).as_posix()}" if latest else None
        ),
    }


@app.post("/api/cloud/sync-pending")
def sync_pending(background_tasks: BackgroundTasks):
    cfg = load_config()
    if not cfg.get("drive_enabled"):
        raise HTTPException(400, "Google Drive sync is disabled")

    count = 0
    for path in DATA_DIR.rglob("*.jpg"):
        meta_file = metadata_path(path)
        status = "PENDING"

        if meta_file.exists():
            try:
                status = json.loads(
                    meta_file.read_text(encoding="utf-8")
                ).get("cloud_status", "PENDING")
            except Exception:
                pass

        if status in ("PENDING", "ERROR"):
            background_tasks.add_task(upload_to_drive, path)
            count += 1

    return {"queued": count}


@app.get("/data/{file_path:path}")
def serve_data(file_path: str):
    candidate = (DATA_DIR / file_path).resolve()
    data_root = DATA_DIR.resolve()

    if data_root not in candidate.parents and candidate != data_root:
        raise HTTPException(403, "Forbidden")

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(404, "File not found")

    if candidate.suffix.lower() not in (".jpg", ".jpeg", ".json"):
        raise HTTPException(403, "File type not served")

    return FileResponse(candidate)
