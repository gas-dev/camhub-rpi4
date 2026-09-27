from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
GOOGLE_OAUTH_PATH = BASE_DIR / "google_oauth.json"
DATA_DIR = BASE_DIR / "data"
NODES_DIR = BASE_DIR / "nodes"
FIRMWARE_DIR = BASE_DIR / "firmware"
OTA_MANIFEST_PATH = FIRMWARE_DIR / "camnode-manifest.json"
OTA_FIRMWARE_PATH = FIRMWARE_DIR / "camnode-current.bin"
OTA_JOBS_PATH = FIRMWARE_DIR / "ota-jobs.json"
ERROR_STATE_PATH = BASE_DIR / "error-state.json"
OTA_SLOT_SIZE = 0x1E0000

DATA_DIR.mkdir(exist_ok=True)
NODES_DIR.mkdir(exist_ok=True)
FIRMWARE_DIR.mkdir(exist_ok=True)
(FIRMWARE_DIR / "archive").mkdir(exist_ok=True)

app = FastAPI(title="CamHub", version="1.0.1")
config_lock = threading.RLock()
cloud_lock = threading.Lock()
cloud_event = threading.Event()
cloud_worker_started = False
cloud_backoff_until = 0.0
cloud_backoff_reason = ""
cloud_backoff_lock = threading.RLock()

stream_frame_lock = threading.RLock()
stream_frame_cache: dict[str, dict[str, Any]] = {}
stream_thread_lock = threading.RLock()
stream_threads: dict[str, threading.Thread] = {}
stream_stop_events: dict[str, threading.Event] = {}
alarm_last_event: dict[str, float] = {}

ota_job_lock = threading.RLock()
ota_jobs: dict[str, dict[str, Any]] = {}

camera_operation_worker_started = False
automatic_scheduler_started = False
camera_operation_lock = threading.RLock()
camera_operation_event = threading.Event()
camera_operation_queue: deque[dict[str, Any]] = deque()
camera_operation_active: dict[str, Any] | None = None
camera_operation_history: list[dict[str, Any]] = []
camera_operation_seq = 0
automatic_next_due = 0.0
automatic_last_interval = 0
runtime_error_lock = threading.RLock()
runtime_errors: list[dict[str, Any]] = []
google_oauth_lock = threading.RLock()
google_oauth_state: dict[str, Any] = {
    "status": "idle",
    "message": "",
    "user_code": "",
    "verification_url": "",
    "expires_at": 0.0,
}

DEFAULT_CONFIG = {
    "server_name": "CamHub-RPI4",
    "camera_id": "CAM01",
    "camera_name": "Camera 1",
    "upload_token": "change-me-now",
    "snapshot_interval_sec": 60,
    "camera_mode": "automatic",
    "event_video_sec": 10,
    "alarm_video_sec": 10,
    "motion_threshold_pct": 8,
    "motion_pixel_delta": 24,
    "motion_sample_ms": 500,
    "motion_cooldown_sec": 15,
    "motion_enabled": False,
    "drive_enabled": False,
    "drive_remote": "gdrive",
    "drive_root": "CamHub",
    "retention_days": 7,
    "camera_frame_size": "SXGA",
    "jpeg_quality": 8,
    "horizontal_mirror": False,
    "vertical_flip": False,
    "brightness": 0,
    "contrast": 0,
    "saturation": 0,
    "stream_max_fps": 5,
    "cloud_batch_delay_sec": 5,
    "cloud_rate_limit_backoff_sec": 600,
    "cloud_tps_limit": 8,
    "google_oauth_client_id": "",
}

FRAME_SIZES = {"VGA", "SVGA", "XGA", "HD", "SXGA", "UXGA"}
MEDIA_SUFFIXES = {".jpg", ".jpeg", ".mp4", ".txt"}


class ConfigModel(BaseModel):
    server_name: str = "CamHub-RPI4"
    camera_id: str = "CAM01"
    camera_name: str = "Camera 1"
    upload_token: str = "change-me-now"
    snapshot_interval_sec: int = 60
    camera_mode: str = "automatic"
    event_video_sec: int = 10
    alarm_video_sec: int = 10
    motion_threshold_pct: int = 8
    motion_pixel_delta: int = 24
    motion_sample_ms: int = 500
    motion_cooldown_sec: int = 15
    motion_enabled: bool = False
    drive_enabled: bool = False
    drive_remote: str = "gdrive"
    drive_root: str = "CamHub"
    retention_days: int = 7
    camera_frame_size: str = "SXGA"
    jpeg_quality: int = 8
    horizontal_mirror: bool = False
    vertical_flip: bool = False
    brightness: int = 0
    contrast: int = 0
    saturation: int = 0
    stream_max_fps: int = 5
    cloud_batch_delay_sec: int = 5
    cloud_rate_limit_backoff_sec: int = 600
    cloud_tps_limit: int = 8
    google_oauth_client_id: str = ""


def save_config(cfg: dict[str, Any]) -> None:
    with config_lock:
        tmp = CONFIG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        tmp.replace(CONFIG_PATH)


def load_google_oauth_credentials() -> dict[str, str]:
    if not GOOGLE_OAUTH_PATH.exists():
        return {"client_id": "", "client_secret": ""}
    try:
        data = json.loads(GOOGLE_OAUTH_PATH.read_text(encoding="utf-8"))
        return {
            "client_id": str(data.get("client_id") or "").strip(),
            "client_secret": str(data.get("client_secret") or "").strip(),
        }
    except Exception:
        return {"client_id": "", "client_secret": ""}


def save_google_oauth_credentials(client_id: str, client_secret: str) -> None:
    tmp = GOOGLE_OAUTH_PATH.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(
            {
                "client_id": client_id.strip(),
                "client_secret": client_secret.strip(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    tmp.chmod(0o600)
    tmp.replace(GOOGLE_OAUTH_PATH)
    GOOGLE_OAUTH_PATH.chmod(0o600)


def load_config() -> dict[str, Any]:
    with config_lock:
        existing: dict[str, Any] = {}
        if CONFIG_PATH.exists():
            try:
                existing = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            except Exception:
                existing = {}
        if "camera_frame_size" not in existing and existing.get("event_video_sec") == 30:
            existing["event_video_sec"] = 10
        if existing.get("camera_mode") == "manual":
            existing["camera_mode"] = "streaming"
        merged = {**DEFAULT_CONFIG, **existing}
        if merged != existing:
            save_config(merged)
        return merged


def now_local() -> datetime:
    return datetime.now().astimezone()


def load_ota_manifest() -> dict[str, Any] | None:
    if not OTA_MANIFEST_PATH.exists() or not OTA_FIRMWARE_PATH.exists():
        return None
    try:
        data = json.loads(OTA_MANIFEST_PATH.read_text(encoding="utf-8"))
        if int(data.get("size") or 0) != OTA_FIRMWARE_PATH.stat().st_size:
            return None
        return data
    except Exception:
        return None


def load_ota_jobs() -> None:
    global ota_jobs
    if not OTA_JOBS_PATH.exists():
        return
    try:
        data = json.loads(OTA_JOBS_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            with ota_job_lock:
                ota_jobs = data
    except Exception:
        pass


def save_ota_jobs() -> None:
    with ota_job_lock:
        snapshot = dict(ota_jobs)
    tmp = OTA_JOBS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    tmp.replace(OTA_JOBS_PATH)


def ota_camera_busy(camera_id: str) -> bool:
    with ota_job_lock:
        job = ota_jobs.get(camera_id)
        if not job:
            return False
        return str(job.get("state") or "") in {
            "requested",
            "preparing",
            "downloading",
            "verifying",
            "rebooting",
            "waiting_heartbeat",
        }


def set_ota_job(camera_id: str, **updates: Any) -> dict[str, Any]:
    with ota_job_lock:
        current = dict(ota_jobs.get(camera_id) or {})
        current.update(updates)
        current["camera_id"] = camera_id
        ota_jobs[camera_id] = current
        result = dict(current)
    save_ota_jobs()
    return result


def camera_day_dir(camera_id: str, dt: datetime) -> Path:
    path = DATA_DIR / camera_id / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
    path.mkdir(parents=True, exist_ok=True)
    return path


def metadata_path(media_path: Path) -> Path:
    manifest_dir = media_path.parent / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    return manifest_dir / f"{media_path.stem}.json"


def legacy_metadata_path(media_path: Path) -> Path:
    return media_path.with_suffix(".json")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_metadata(path: Path, data: dict[str, Any]) -> None:
    metadata_path(path).write_text(json.dumps(data, indent=2), encoding="utf-8")


def read_metadata(path: Path) -> dict[str, Any]:
    meta = metadata_path(path)
    if not meta.exists():
        legacy = legacy_metadata_path(path)
        if legacy.exists():
            meta = legacy
        else:
            return {}
    try:
        return json.loads(meta.read_text(encoding="utf-8"))
    except Exception:
        return {}


def update_metadata(path: Path, **updates: Any) -> None:
    data = read_metadata(path)
    data.update(updates)
    write_metadata(path, data)


def make_filename(camera: str, event_type: str, dt: datetime, suffix: str) -> str:
    safe_event = "".join(char for char in event_type.upper() if char.isalnum() or char in "_-")[:24] or "MEDIA"
    return f"{camera}_{safe_event}_{dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}{suffix}"


def register_media(path: Path, camera: str, event_type: str, captured_at: datetime, source: str) -> dict[str, Any]:
    cfg = load_config()
    suffix = path.suffix.lower()
    media_type = "video" if suffix == ".mp4" else ("error" if suffix == ".txt" else "image")
    data = {
        "camera_id": camera,
        "event_type": event_type,
        "captured_at": captured_at.isoformat(),
        "received_at": now_local().isoformat(),
        "filename": path.name,
        "size": path.stat().st_size,
        "sha256": sha256_file(path),
        "source": source,
        "media_type": media_type,
        "cloud_status": "PENDING" if cfg.get("drive_enabled") else "DISABLED",
    }
    write_metadata(path, data)
    if cfg.get("drive_enabled"):
        queue_cloud_sync()
    return data


def record_runtime_error(
    source: str,
    detail: str,
    camera_id: str | None = None,
    category: str | None = None,
) -> None:
    item = {
        "time": now_local().isoformat(),
        "source": source,
        "camera_id": camera_id,
        "category": category,
        "detail": str(detail)[-6000:],
    }
    with runtime_error_lock:
        runtime_errors.insert(0, item)
        del runtime_errors[100:]


def create_camera_error_file(camera_id: str, category: str, message: str, source: str = "camera") -> Path:
    dt = now_local()
    safe_category = "".join(ch for ch in category.upper() if ch.isalnum() or ch in "_-")[:32] or "ERROR"
    out_path = camera_day_dir(camera_id, dt) / make_filename(camera_id, f"camera_error_{safe_category}", dt, ".txt")
    node = get_node(camera_id) or {}
    body = (
        "CamNode error report\n"
        f"timestamp: {dt.isoformat()}\n"
        f"camera_id: {camera_id}\n"
        f"category: {category}\n"
        f"source: {source}\n"
        f"firmware: {node.get('firmware', 'unknown')}\n"
        f"ip: {node.get('ip', 'unknown')}\n"
        f"rssi: {node.get('rssi', 'unknown')}\n"
        "\nDETAILS\n"
        f"{message}\n"
    )
    out_path.write_text(body, encoding="utf-8")
    register_media(out_path, camera_id, f"camera_error_{safe_category}", dt, source)
    update_metadata(
        out_path,
        error_message=str(message)[-6000:],
        error_category=category,
    )
    record_runtime_error(source, message, camera_id, category)
    return out_path


def load_error_state() -> dict[str, Any]:
    if not ERROR_STATE_PATH.exists():
        return {"cleared_before": None}
    try:
        data = json.loads(ERROR_STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"cleared_before": None}
        return {"cleared_before": data.get("cleared_before")}
    except Exception:
        return {"cleared_before": None}


def save_error_state(state: dict[str, Any]) -> None:
    tmp = ERROR_STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(ERROR_STATE_PATH)


def _parse_error_time(value: Any) -> datetime | None:
    try:
        if not value:
            return None
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _error_title(detail: Any) -> str:
    text = str(detail or "").strip()
    if not text:
        return "Errore senza dettaglio"
    for line in text.splitlines():
        clean = line.strip()
        if not clean:
            continue
        if clean.lower().startswith("message:"):
            clean = clean.split(":", 1)[1].strip()
        if clean:
            return clean[:240]
    return text[:240]


def _error_domain(source: Any, category: Any, detail: Any) -> str:
    src = str(source or "").lower()
    cat = str(category or "").lower()
    txt = str(detail or "").lower()

    if (
        "google_drive" in src
        or "google_oauth" in src
        or "cloud" in src
        or "rclone" in src
        or "googleapi" in txt
        or "drive.googleapis.com" in txt
    ):
        return "cloud"

    if src == "camera_firmware" or src.startswith("camera_firmware"):
        return "camera"

    protocol_sources = {
        "streaming_mode",
        "camera_proxy",
        "node_protocol",
        "camera_protocol",
    }
    transport_terms = (
        "http error",
        "http 4",
        "http 5",
        "connection refused",
        "connection reset",
        "connection error",
        "timed out",
        "timeout",
        "urlopen",
        "remote end closed",
        "broken pipe",
        "network is unreachable",
    )
    if src in protocol_sources or any(term in txt for term in transport_terms):
        return "protocol"

    if cat in {
        "camera_init",
        "capture",
        "manual_capture",
        "stream_capture",
        "alarm_camera",
        "alarm_capture",
        "camera_mutex",
    } and src.startswith("camera"):
        return "camera"

    return "camhub"


def _error_base_severity(
    domain: str,
    source: Any,
    category: Any,
    detail: Any,
) -> str:
    src = str(source or "").lower()
    cat = str(category or "").lower()
    txt = str(detail or "").lower()

    critical_terms = (
        "nameerror",
        "undefined name",
        "not defined",
        "syntaxerror",
        "importerror",
        "modulenotfounderror",
        "traceback",
        "segmentation fault",
        "out of memory",
    )
    if domain == "camhub" and any(term in txt for term in critical_terms):
        return "critical"

    if domain == "camera":
        if (
            cat == "camera_init"
            or "initialization failed" in txt
            or "driver recovery failed" in txt
            or "recovery_failures:" in txt and not "recovery_failures: 0" in txt
        ):
            return "critical"
        if (
            "failed even after camera-driver recovery" in txt
            or "failed after driver recovery" in txt
            or "framebuffer unavailable" in txt
        ):
            return "error"
        return "warning"

    if domain == "protocol":
        if "401" in txt or "403" in txt or "invalid camera token" in txt:
            return "error"
        return "warning"

    if domain == "cloud":
        if (
            "service_disabled" in txt
            or "accessnotconfigured" in txt
            or "invalid_grant" in txt
            or "unauthorized" in txt
        ):
            return "error"
        return "warning"

    if "camera_operation" in src:
        return "error"

    return "error"


def _error_fingerprint(item: dict[str, Any]) -> tuple[str, str, str]:
    domain = _error_domain(
        item.get("source"),
        item.get("category"),
        item.get("detail"),
    )
    category = str(item.get("category") or "").lower()
    title = _error_title(item.get("detail")).lower()
    return (domain, category, title)


def _raw_error_items(include_cleared: bool = False) -> list[dict[str, Any]]:
    state = load_error_state()
    cutoff = _parse_error_time(state.get("cleared_before"))
    rows: list[dict[str, Any]] = []

    # Durable media/error metadata first.
    for path in media_files():
        meta = read_metadata(path)
        detail = meta.get("cloud_error") or meta.get("error_message")
        if not detail:
            continue

        item = {
            "time": (
                meta.get("cloud_error_at")
                or meta.get("captured_at")
                or meta.get("received_at")
            ),
            "source": (
                "google_drive"
                if meta.get("cloud_error")
                else meta.get("source", "media")
            ),
            "category": meta.get("error_category"),
            "camera_id": meta.get("camera_id"),
            "detail": str(detail)[-6000:],
            "file": path.name,
        }
        when = _parse_error_time(item["time"])
        if not include_cleared and cutoff and when and when <= cutoff:
            continue
        rows.append(item)

    # Runtime-only errors. Suppress duplicates of a durable error occurring
    # within five seconds with the same source/title.
    with runtime_error_lock:
        runtime_copy = [dict(item) for item in runtime_errors]

    for item in runtime_copy:
        when = _parse_error_time(item.get("time"))
        if not include_cleared and cutoff and when and when <= cutoff:
            continue

        title = _error_title(item.get("detail"))
        duplicate = False
        for existing in rows:
            if str(existing.get("source") or "") != str(item.get("source") or ""):
                continue
            if _error_title(existing.get("detail")) != title:
                continue
            t2 = _parse_error_time(existing.get("time"))
            if when and t2 and abs((when - t2).total_seconds()) <= 5:
                duplicate = True
                break
        if not duplicate:
            rows.append(item)

    rows.sort(key=lambda item: str(item.get("time") or ""), reverse=True)
    return rows


def grouped_errors(
    include_cleared: bool = False,
    domain_filter: str | None = None,
    severity_filter: str | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}

    for item in _raw_error_items(include_cleared=include_cleared):
        domain = _error_domain(
            item.get("source"),
            item.get("category"),
            item.get("detail"),
        )
        key = _error_fingerprint(item)
        current = grouped.get(key)

        if current is None:
            current = {
                "domain": domain,
                "category": item.get("category"),
                "source": item.get("source"),
                "camera_id": item.get("camera_id"),
                "title": _error_title(item.get("detail")),
                "detail": item.get("detail"),
                "file": item.get("file"),
                "first_seen": item.get("time"),
                "last_seen": item.get("time"),
                "count": 1,
            }
            grouped[key] = current
        else:
            current["count"] += 1
            current["first_seen"] = min(
                str(current.get("first_seen") or ""),
                str(item.get("time") or ""),
            )
            if str(item.get("time") or "") >= str(current.get("last_seen") or ""):
                current["last_seen"] = item.get("time")
                current["detail"] = item.get("detail")
                current["file"] = item.get("file") or current.get("file")
                current["camera_id"] = (
                    item.get("camera_id") or current.get("camera_id")
                )

    results: list[dict[str, Any]] = []
    for item in grouped.values():
        severity = _error_base_severity(
            item["domain"],
            item.get("source"),
            item.get("category"),
            item.get("detail"),
        )

        # A transient protocol/cloud warning becomes important only if it repeats.
        if severity == "warning" and int(item.get("count") or 0) >= 3:
            severity = "error"

        item["severity"] = severity

        if domain_filter and item["domain"] != domain_filter:
            continue
        if severity_filter and item["severity"] != severity_filter:
            continue
        results.append(item)

    order = {"critical": 0, "error": 1, "warning": 2, "info": 3}
    results.sort(
        key=lambda item: (
            order.get(str(item.get("severity") or "info"), 9),
            str(item.get("last_seen") or ""),
        ),
        reverse=False,
    )

    # Keep newest first inside the same severity.
    def sort_timestamp(item: dict[str, Any]) -> float:
        parsed = _parse_error_time(item.get("last_seen"))
        return parsed.timestamp() if parsed else 0.0

    results = sorted(
        results,
        key=lambda item: (
            order.get(str(item.get("severity") or "info"), 9),
            -sort_timestamp(item),
        ),
    )
    return results


def error_summary() -> dict[str, Any]:
    items = grouped_errors()
    by_domain = {"camera": 0, "protocol": 0, "camhub": 0, "cloud": 0}
    by_severity = {"critical": 0, "error": 0, "warning": 0}

    for item in items:
        domain = str(item.get("domain") or "camhub")
        severity = str(item.get("severity") or "warning")
        by_domain[domain] = by_domain.get(domain, 0) + 1
        by_severity[severity] = by_severity.get(severity, 0) + 1

    important = [
        item for item in items
        if item.get("severity") in ("critical", "error")
    ][:5]

    return {
        "by_domain": by_domain,
        "by_severity": by_severity,
        "important_count": (
            by_severity.get("critical", 0) + by_severity.get("error", 0)
        ),
        "important": important,
        "cleared_before": load_error_state().get("cleared_before"),
    }


def node_path(camera_id: str) -> Path:
    safe = "".join(c for c in camera_id if c.isalnum() or c in "_-")[:64]
    return NODES_DIR / f"{safe}.json"


def save_node(camera_id: str, data: dict[str, Any]) -> None:
    node_path(camera_id).write_text(json.dumps(data, indent=2), encoding="utf-8")


def get_node(camera_id: str) -> dict[str, Any] | None:
    path = node_path(camera_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def set_stream_frame(camera_id: str, frame: bytes) -> None:
    now_mono = time.monotonic()
    with stream_frame_lock:
        previous = stream_frame_cache.get(camera_id, {})
        previous_at = float(previous.get("received_mono") or 0.0)
        previous_fps = float(previous.get("source_fps") or 0.0)
        instant_fps = 0.0
        if previous_at > 0 and now_mono > previous_at:
            instant_fps = 1.0 / (now_mono - previous_at)
        source_fps = (
            instant_fps
            if previous_fps <= 0
            else previous_fps * 0.8 + instant_fps * 0.2
        )
        stream_frame_cache[camera_id] = {
            "frame": frame,
            "seq": int(previous.get("seq") or 0) + 1,
            "received_mono": now_mono,
            "received_at": time.time(),
            "source_fps": min(source_fps, 60.0),
        }


def get_stream_frame(
    camera_id: str,
    max_age: float = 5.0,
) -> dict[str, Any] | None:
    with stream_frame_lock:
        item = stream_frame_cache.get(camera_id)
        if not item:
            return None
        if (
            time.monotonic()
            - float(item.get("received_mono") or 0.0)
            > max_age
        ):
            return None
        return dict(item)


def clear_stream_frame(camera_id: str) -> None:
    with stream_frame_lock:
        stream_frame_cache.pop(camera_id, None)


def _stream_reader(camera_id: str, stop_event: threading.Event) -> None:
    try:
        node = get_node(camera_id)
        stream_url = str((node or {}).get("stream_url") or "")
        if not stream_url:
            raise RuntimeError("Camera stream URL unavailable")

        request = urllib.request.Request(
            stream_url,
            headers={"User-Agent": "CamHub/0.9-stream", "Connection": "close"},
        )

        with urllib.request.urlopen(request, timeout=15) as response:
            buffer = b""

            while not stop_event.is_set():
                if load_config().get("camera_mode") != "streaming":
                    break

                chunk = response.read(8192)
                if not chunk:
                    break
                buffer += chunk

                while True:
                    start = buffer.find(b"\xff\xd8")
                    if start < 0:
                        if len(buffer) > 2:
                            buffer = buffer[-2:]
                        break

                    end = buffer.find(b"\xff\xd9", start + 2)
                    if end < 0:
                        if start > 0:
                            buffer = buffer[start:]
                        if len(buffer) > 8 * 1024 * 1024:
                            buffer = b""
                        break

                    frame = buffer[start:end + 2]
                    buffer = buffer[end + 2:]
                    if len(frame) > 1024:
                        set_stream_frame(camera_id, frame)

    except Exception as exc:
        if not stop_event.is_set() and load_config().get("camera_mode") == "streaming":
            record_runtime_error("streaming_mode", str(exc), camera_id)
    finally:
        clear_stream_frame(camera_id)
        with stream_thread_lock:
            current = stream_threads.get(camera_id)
            if current is threading.current_thread():
                stream_threads.pop(camera_id, None)
                stream_stop_events.pop(camera_id, None)


def start_streaming_mode(camera_id: str) -> None:
    with stream_thread_lock:
        existing = stream_threads.get(camera_id)
        if existing and existing.is_alive():
            return

        stop_event = threading.Event()
        worker = threading.Thread(
            target=_stream_reader,
            args=(camera_id, stop_event),
            daemon=True,
            name=f"camhub-streaming-{camera_id}",
        )
        stream_stop_events[camera_id] = stop_event
        stream_threads[camera_id] = worker
        worker.start()


def stop_streaming_mode(camera_id: str, wait_sec: float = 4.0) -> None:
    with stream_thread_lock:
        stop_event = stream_stop_events.get(camera_id)
        worker = stream_threads.get(camera_id)
        if stop_event:
            stop_event.set()

    if worker and worker.is_alive():
        worker.join(timeout=wait_sec)

    clear_stream_frame(camera_id)


def streaming_mode_status(camera_id: str) -> dict[str, Any]:
    with stream_thread_lock:
        worker = stream_threads.get(camera_id)
        running = bool(worker and worker.is_alive())

    frame = get_stream_frame(camera_id, max_age=5.0)
    return {
        "running": running,
        "frame_available": frame is not None,
        "source_fps": round(float((frame or {}).get("source_fps") or 0.0), 1),
    }


def camera_operation_status() -> dict[str, Any]:
    with camera_operation_lock:
        active = dict(camera_operation_active) if camera_operation_active else None
        queued = [dict(item) for item in list(camera_operation_queue)[:25]]
        history = [dict(item) for item in camera_operation_history[:10]]

    cfg = load_config()
    remaining = 0
    if cfg.get("camera_mode") == "automatic" and automatic_next_due > 0:
        remaining = max(0, int(automatic_next_due - time.monotonic()))

    return {
        "mode": cfg.get("camera_mode", "automatic"),
        "active": active,
        "queued": queued,
        "queue_length": len(queued),
        "history": history,
        "next_automatic_in_sec": remaining,
    }


def enqueue_camera_operation(
    kind: str,
    camera_id: str,
    duration: int = 0,
    origin: str = "manual",
    priority: bool = False,
) -> dict[str, Any]:
    global camera_operation_seq

    with camera_operation_lock:
        camera_operation_seq += 1
        item = {
            "id": camera_operation_seq,
            "kind": kind,
            "camera_id": camera_id,
            "duration": duration,
            "origin": origin,
            "status": "queued",
            "queued_at": now_local().isoformat(),
        }
        if priority:
            camera_operation_queue.appendleft(item)
            position = 1
        else:
            camera_operation_queue.append(item)
            position = len(camera_operation_queue)

    camera_operation_event.set()
    result = dict(item)
    result["position"] = position
    return result


def _direct_camera_photo(camera_id: str, event_type: str) -> dict[str, Any]:
    url = _node_url(camera_id, "capture_url", "/capture")
    last_error = ""

    for attempt in range(1, 4):
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CamHub/0.8", "Connection": "close"},
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = response.read(8 * 1024 * 1024)

            if not payload:
                raise RuntimeError("Camera returned an empty JPEG")

            dt = now_local()
            out_path = camera_day_dir(camera_id, dt) / make_filename(
                camera_id,
                event_type,
                dt,
                ".jpg",
            )
            out_path.write_bytes(payload)
            meta = register_media(
                out_path,
                camera_id,
                event_type,
                dt,
                "exclusive_direct_capture",
            )
            return {
                "file": out_path.name,
                "sha256": meta["sha256"],
                "size": len(payload),
                "attempt": attempt,
            }

        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            last_error = f"HTTP {exc.code}: {detail or exc.reason}"

            if exc.code not in (409, 423, 429, 503) or attempt >= 3:
                break

        except urllib.error.URLError as exc:
            last_error = f"connection error: {exc}"
            if attempt >= 3:
                break

        except Exception as exc:
            last_error = str(exc)
            if attempt >= 3:
                break

        time.sleep(0.6 * attempt)

    raise RuntimeError(
        f"Exclusive photo failed after 3 attempts: {last_error}"
    )


def _streaming_photo(camera_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 5.0
    frame = get_stream_frame(camera_id, max_age=3.0)

    while frame is None and time.monotonic() < deadline:
        time.sleep(0.05)
        frame = get_stream_frame(camera_id, max_age=3.0)

    if frame is None:
        raise RuntimeError("Streaming mode has no recent frame")

    payload = bytes(frame["frame"])
    dt = now_local()
    out_path = camera_day_dir(camera_id, dt) / make_filename(
        camera_id,
        "manual",
        dt,
        ".jpg",
    )
    out_path.write_bytes(payload)
    meta = register_media(
        out_path,
        camera_id,
        "manual",
        dt,
        "shared_stream",
    )
    return {
        "file": out_path.name,
        "sha256": meta["sha256"],
        "size": len(payload),
        "source_fps": round(float(frame.get("source_fps") or 0.0), 1),
    }


def _streaming_video(camera_id: str, seconds: int) -> dict[str, Any]:
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg is not installed")

    cfg = load_config()
    fps = max(1, min(int(cfg.get("stream_max_fps", 5)), 15))
    dt = now_local()
    event_name = f"video_{seconds}s"
    out_path = camera_day_dir(camera_id, dt) / make_filename(
        camera_id,
        event_name,
        dt,
        ".mp4",
    )

    command = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "mjpeg",
        "-framerate", str(fps),
        "-i", "pipe:0",
        "-an",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(out_path),
    ]

    proc = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    started = time.monotonic()
    last_seq = -1
    frames_written = 0
    error_text = ""

    try:
        while time.monotonic() - started < seconds:
            if load_config().get("camera_mode") != "streaming":
                raise RuntimeError("Streaming mode was stopped during recording")

            frame = get_stream_frame(camera_id, max_age=3.0)
            if frame is None:
                time.sleep(0.03)
                continue

            seq = int(frame.get("seq") or 0)
            if seq == last_seq:
                time.sleep(0.01)
                continue

            last_seq = seq
            payload = bytes(frame["frame"])
            if proc.stdin is None:
                raise RuntimeError("FFmpeg input pipe unavailable")
            proc.stdin.write(payload)
            frames_written += 1

        if proc.stdin is not None:
            proc.stdin.close()
            proc.stdin = None

        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            raise RuntimeError("FFmpeg did not finish after streaming video")

        if proc.stderr is not None:
            error_text = proc.stderr.read().decode("utf-8", errors="replace")[-2000:]

    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    if (
        proc.returncode != 0
        or frames_written < 2
        or not out_path.exists()
        or out_path.stat().st_size == 0
    ):
        out_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Streaming video failed after {frames_written} frames: {error_text}"
        )

    meta = register_media(
        out_path,
        camera_id,
        f"video_{seconds}s",
        dt,
        "shared_stream",
    )
    return {
        "file": out_path.name,
        "duration": seconds,
        "frames": frames_written,
        "fps": fps,
        "size": out_path.stat().st_size,
        "sha256": meta["sha256"],
    }


def _exclusive_camera_video(
    camera_id: str,
    seconds: int,
    event_type: str | None = None,
    source: str = "exclusive_mjpeg_ffmpeg",
) -> dict[str, Any]:
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg is not installed")

    cfg = load_config()
    requested_fps = max(1, min(int(cfg.get("stream_max_fps", 5)), 15))
    stream_url = _node_url(camera_id, "stream_url", "/stream")
    dt = now_local()
    event_name = event_type or f"video_{seconds}s"
    out_path = camera_day_dir(camera_id, dt) / make_filename(
        camera_id,
        event_name,
        dt,
        ".mp4",
    )

    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-use_wallclock_as_timestamps", "1",
        "-i", stream_url,
        "-t", str(seconds),
        "-an",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-r", str(requested_fps),
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(out_path),
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(45, seconds + 35),
        )
    except subprocess.TimeoutExpired as exc:
        out_path.unlink(missing_ok=True)
        raise RuntimeError("Video recording timed out") from exc

    if (
        result.returncode != 0
        or not out_path.exists()
        or out_path.stat().st_size == 0
    ):
        out_path.unlink(missing_ok=True)
        detail = (result.stderr or result.stdout or "unknown ffmpeg error")[-2000:]
        raise RuntimeError("Video recording failed: " + detail)

    meta = register_media(
        out_path,
        camera_id,
        event_name,
        dt,
        source,
    )

    return {
        "file": out_path.name,
        "duration": seconds,
        "requested_fps": requested_fps,
        "resolution": cfg.get("camera_frame_size"),
        "size": out_path.stat().st_size,
        "sha256": meta["sha256"],
    }


def camera_operation_worker() -> None:
    global camera_operation_active

    while True:
        camera_operation_event.wait(timeout=1.0)
        camera_operation_event.clear()

        while True:
            with camera_operation_lock:
                if not camera_operation_queue:
                    camera_operation_active = None
                    break
                item = camera_operation_queue.popleft()
                item["status"] = "running"
                item["started_at"] = now_local().isoformat()
                camera_operation_active = item

            try:
                if item["kind"] == "photo":
                    result = _direct_camera_photo(
                        str(item["camera_id"]),
                        "periodic",
                    )
                elif item["kind"] == "stream_photo":
                    result = _streaming_photo(
                        str(item["camera_id"]),
                    )
                elif item["kind"] == "stream_video":
                    result = _streaming_video(
                        str(item["camera_id"]),
                        max(1, int(item.get("duration") or 10)),
                    )
                elif item["kind"] == "alarm_video":
                    seconds = max(1, int(item.get("duration") or 10))
                    result = _exclusive_camera_video(
                        str(item["camera_id"]),
                        seconds,
                        event_type=f"alarm_video_{seconds}s",
                        source="alarm_triggered_stream",
                    )
                else:
                    raise RuntimeError(
                        f"Unsupported camera operation: {item['kind']}"
                    )

                item["status"] = "completed"
                item["result"] = result
            except Exception as exc:
                detail = str(getattr(exc, "detail", exc))
                item["status"] = "error"
                item["error"] = detail[-4000:]
                record_runtime_error(
                    "camera_operation",
                    detail,
                    str(item.get("camera_id") or ""),
                )
            finally:
                item["finished_at"] = now_local().isoformat()
                with camera_operation_lock:
                    camera_operation_history.insert(0, dict(item))
                    del camera_operation_history[25:]
                    camera_operation_active = None

                # Let the ESP32 camera/HTTP stack settle before the next
                # serialized operation. No camera operation overlaps another.
                time.sleep(0.75)


def ensure_camera_operation_worker() -> None:
    global camera_operation_worker_started
    if camera_operation_worker_started:
        return
    camera_operation_worker_started = True
    threading.Thread(
        target=camera_operation_worker,
        daemon=True,
        name="camhub-camera-operations",
    ).start()


def automatic_snapshot_scheduler() -> None:
    global automatic_next_due, automatic_last_interval

    last_mode = ""
    while True:
        try:
            cfg = load_config()
            mode = str(cfg.get("camera_mode") or "automatic")
            interval = max(1, int(cfg.get("snapshot_interval_sec", 60)))
            camera_id = str(cfg.get("camera_id") or "CAM01")
            now_mono = time.monotonic()

            if mode != "automatic":
                automatic_next_due = 0.0
                automatic_last_interval = interval
                last_mode = mode
                time.sleep(0.25)
                continue

            if (
                last_mode != "automatic"
                or automatic_next_due <= 0
                or automatic_last_interval != interval
            ):
                automatic_next_due = now_mono + interval
                automatic_last_interval = interval
                last_mode = "automatic"

            if now_mono >= automatic_next_due:
                if ota_camera_busy(camera_id):
                    automatic_next_due = now_mono + interval
                    time.sleep(0.25)
                    continue

                with camera_operation_lock:
                    camera_idle = (
                        camera_operation_active is None
                        and not camera_operation_queue
                    )

                if camera_idle:
                    enqueue_camera_operation(
                        "photo",
                        camera_id,
                        origin="automatic",
                    )

                automatic_next_due = now_mono + interval
        except Exception as exc:
            record_runtime_error("automatic_snapshot_scheduler", str(exc))

        time.sleep(0.25)


def ensure_automatic_snapshot_scheduler() -> None:
    global automatic_scheduler_started
    if automatic_scheduler_started:
        return
    automatic_scheduler_started = True
    threading.Thread(
        target=automatic_snapshot_scheduler,
        daemon=True,
        name="camhub-automatic-snapshot",
    ).start()

def media_files() -> list[Path]:
    return [p for p in DATA_DIR.rglob("*") if p.is_file() and p.suffix.lower() in MEDIA_SUFFIXES]


def latest_jpg(camera_id: str | None = None) -> Path | None:
    base = DATA_DIR / camera_id if camera_id else DATA_DIR
    if not base.exists():
        return None
    files = [p for p in base.rglob("*.jpg") if p.is_file()]
    return max(files, key=lambda p: p.stat().st_mtime) if files else None


def recent_items(limit: int = 50) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    files = sorted(media_files(), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
    for path in files:
        meta = read_metadata(path)
        items.append({
            "file": path.name,
            "relative": path.relative_to(DATA_DIR).as_posix(),
            "camera_id": meta.get("camera_id"),
            "event_type": meta.get("event_type"),
            "captured_at": meta.get("captured_at"),
            "cloud_status": meta.get("cloud_status", "PENDING"),
            "size": path.stat().st_size,
            "sha256": meta.get("sha256"),
            "media_type": meta.get(
                "media_type",
                "video" if path.suffix.lower() == ".mp4" else ("error" if path.suffix.lower() == ".txt" else "image"),
            ),
            "source": meta.get("source"),
            "cloud_error": meta.get("cloud_error"),
            "cloud_error_at": meta.get("cloud_error_at"),
            "error_message": meta.get("error_message"),
            "error_category": meta.get("error_category"),
        })
    return items


def _rclone_batch(files: list[Path], cfg: dict[str, Any]) -> tuple[bool, str]:
    if not files:
        return True, ""
    target = f"{cfg['drive_remote']}:{cfg['drive_root'].strip('/')}"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as temp:
        temp_path = Path(temp.name)
        for path in files:
            temp.write(path.relative_to(DATA_DIR).as_posix() + "\n")
    try:
        result = subprocess.run(
            [
                "rclone", "copy", str(DATA_DIR), target,
                "--files-from", str(temp_path),
                "--transfers", "2", "--checkers", "2",
                "--retries", "3", "--low-level-retries", "5",
                "--no-traverse",
                "--tpslimit", str(max(1, int(cfg.get("cloud_tps_limit", 8)))),
                "--tpslimit-burst", str(max(1, int(cfg.get("cloud_tps_limit", 8)))),
            ],
            capture_output=True, text=True, timeout=900,
        )
        return result.returncode == 0, (result.stderr or result.stdout)[-3000:]
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass


def sync_pending_batch() -> int:
    cfg = load_config()
    if not cfg.get("drive_enabled"):
        return 0
    if shutil.which("rclone") is None:
        return 0

    status = cloud_backoff_status()
    if status["active"]:
        return 0

    with cloud_lock:
        pending: list[Path] = []
        for path in media_files():
            status = read_metadata(path).get("cloud_status", "PENDING")
            if status in ("PENDING", "ERROR"):
                pending.append(path)

        if not pending:
            return 0

        for path in pending:
            update_metadata(path, cloud_status="UPLOADING")

        first_batch: list[Path] = []
        for path in pending:
            first_batch.append(path)
            meta = metadata_path(path)
            if meta.exists():
                first_batch.append(meta)

        ok, error = _rclone_batch(first_batch, cfg)
        if not ok:
            failed_at = now_local().isoformat()
            record_runtime_error("google_drive", error)

            if is_rate_limit_error(error):
                backoff_sec = max(60, int(cfg.get("cloud_rate_limit_backoff_sec", 600)))
                set_cloud_backoff(
                    backoff_sec,
                    f"Google Drive rate limit exceeded. Automatic sync paused for {backoff_sec} seconds.",
                )

            for path in pending:
                meta = read_metadata(path)
                update_metadata(
                    path,
                    cloud_status="ERROR",
                    cloud_error=error,
                    cloud_error_at=failed_at,
                    cloud_attempts=int(meta.get("cloud_attempts") or 0) + 1,
                )
            return 0

        synced_at = now_local().isoformat()
        meta_files: list[Path] = []
        for path in pending:
            update_metadata(
                path,
                cloud_status="SYNCED",
                cloud_synced_at=synced_at,
                cloud_error=None,
                cloud_error_at=None,
            )
            meta_files.append(metadata_path(path))

        _rclone_batch(meta_files, cfg)
        clear_cloud_backoff()
        return len(pending)


def cloud_worker() -> None:
    while True:
        signaled = cloud_event.wait(timeout=60.0)
        cloud_event.clear()

        cfg = load_config()
        if signaled:
            time.sleep(max(0, min(int(cfg.get("cloud_batch_delay_sec", 5)), 60)))

        try:
            sync_pending_batch()
        except Exception as exc:
            record_runtime_error("cloud_worker", str(exc))


def ensure_cloud_worker() -> None:
    global cloud_worker_started
    if cloud_worker_started:
        return
    cloud_worker_started = True
    threading.Thread(target=cloud_worker, daemon=True, name="camhub-cloud").start()


def cloud_backoff_status() -> dict[str, Any]:
    with cloud_backoff_lock:
        remaining = max(0.0, cloud_backoff_until - time.time())
        return {
            "active": remaining > 0,
            "remaining_sec": int(remaining),
            "until_epoch": cloud_backoff_until if remaining > 0 else None,
            "reason": cloud_backoff_reason if remaining > 0 else "",
        }


def set_cloud_backoff(seconds: int, reason: str) -> None:
    global cloud_backoff_until, cloud_backoff_reason
    with cloud_backoff_lock:
        cloud_backoff_until = max(cloud_backoff_until, time.time() + max(1, seconds))
        cloud_backoff_reason = str(reason)[-3000:]


def clear_cloud_backoff() -> None:
    global cloud_backoff_until, cloud_backoff_reason
    with cloud_backoff_lock:
        cloud_backoff_until = 0.0
        cloud_backoff_reason = ""


def is_rate_limit_error(message: str) -> bool:
    text = str(message).lower()
    return (
        "ratelimitexceeded" in text
        or "rate_limit_exceeded" in text
        or "quota exceeded" in text
        or "requests per minute" in text
    )


def google_oauth_public_status() -> dict[str, Any]:
    with google_oauth_lock:
        state = dict(google_oauth_state)
    state.pop("device_code", None)
    state.pop("client_id", None)
    return state


def google_form_post(url: str, data: dict[str, str], timeout: int = 20) -> dict[str, Any]:
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"error": f"HTTP {exc.code}", "error_description": body}
        payload["_http_status"] = exc.code
        return payload


def configure_rclone_device_token(client_id: str, client_secret: str, token_payload: dict[str, Any]) -> tuple[bool, str]:
    cfg = load_config()
    remote = str(cfg.get("drive_remote") or "gdrive")
    root = str(cfg.get("drive_root") or "CamHub").strip("/")
    refresh_token = str(token_payload.get("refresh_token") or "")
    access_token = str(token_payload.get("access_token") or "")
    expires_in = int(token_payload.get("expires_in") or 3600)

    if not refresh_token or not access_token:
        return False, "Google did not return both access_token and refresh_token."

    expiry = (
        datetime.now(timezone.utc) + timedelta(seconds=max(60, expires_in))
    ).isoformat().replace("+00:00", "Z")

    rclone_token = json.dumps(
        {
            "access_token": access_token,
            "token_type": str(token_payload.get("token_type") or "Bearer"),
            "refresh_token": refresh_token,
            "expiry": expiry,
        },
        separators=(",", ":"),
    )

    config_path = rclone_config_path()
    if config_path and config_path.exists():
        backup = config_path.with_name(
            config_path.name + ".backup." + now_local().strftime("%Y%m%d_%H%M%S")
        )
        try:
            shutil.copy2(config_path, backup)
            backup.chmod(0o600)
        except Exception:
            pass

    exists = False
    try:
        listed = subprocess.run(
            ["rclone", "listremotes"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        exists = f"{remote}:" in (listed.stdout or "").splitlines()
    except Exception:
        pass

    if exists:
        command = [
            "rclone", "config", "update", remote,
            "client_id", client_id,
            "client_secret", client_secret,
            "scope", "drive.file",
            "token", rclone_token,
            "config_refresh_token", "false",
            "--non-interactive",
        ]
    else:
        command = [
            "rclone", "config", "create", remote, "drive",
            "client_id", client_id,
            "client_secret", client_secret,
            "scope", "drive.file",
            "token", rclone_token,
            "config_refresh_token", "false",
            "--non-interactive",
        ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    except Exception as exc:
        return False, f"Unable to update rclone configuration: {exc}"

    if result.returncode != 0:
        return False, ((result.stderr or "") or (result.stdout or ""))[-4000:]

    try:
        mk = subprocess.run(
            [
                "rclone", "mkdir", f"{remote}:{root}",
                "--tpslimit", str(max(1, int(cfg.get("cloud_tps_limit", 8)))),
                "--tpslimit-burst", str(max(1, int(cfg.get("cloud_tps_limit", 8)))),
            ],
            capture_output=True,
            text=True,
            timeout=45,
        )
        if mk.returncode != 0:
            return False, ((mk.stderr or "") or (mk.stdout or ""))[-4000:]
    except Exception as exc:
        return False, f"OAuth saved but Drive root setup failed: {exc}"

    clear_cloud_backoff()
    queue_cloud_sync(force=True)
    return True, "Google Drive connected. Token stored locally in rclone configuration."


def google_device_oauth_worker(client_id: str, client_secret: str, device_code: str, interval: int, expires_at: float) -> None:
    wait_seconds = max(2, interval)

    while time.time() < expires_at:
        time.sleep(wait_seconds)
        payload = google_form_post(
            "https://oauth2.googleapis.com/token",
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            },
            timeout=20,
        )

        if payload.get("access_token"):
            ok, message = configure_rclone_device_token(client_id, client_secret, payload)
            with google_oauth_lock:
                google_oauth_state.update({
                    "status": "connected" if ok else "error",
                    "message": message,
                    "user_code": "",
                    "verification_url": "",
                    "expires_at": 0.0,
                })
            if not ok:
                record_runtime_error("google_oauth", message)
            return

        error = str(payload.get("error") or "")
        description = str(payload.get("error_description") or error)

        if error == "authorization_pending":
            continue
        if error == "slow_down":
            wait_seconds += 5
            continue
        if error in ("access_denied", "expired_token"):
            with google_oauth_lock:
                google_oauth_state.update({
                    "status": "error",
                    "message": description,
                    "expires_at": 0.0,
                })
            record_runtime_error("google_oauth", description)
            return

        with google_oauth_lock:
            google_oauth_state.update({
                "status": "error",
                "message": description or "Unknown Google OAuth error",
                "expires_at": 0.0,
            })
        record_runtime_error("google_oauth", description or "Unknown Google OAuth error")
        return

    with google_oauth_lock:
        google_oauth_state.update({
            "status": "error",
            "message": "Google authorization code expired. Start the connection again.",
            "expires_at": 0.0,
        })


def rclone_config_path() -> Path | None:
    if shutil.which("rclone") is None:
        return None
    try:
        result = subprocess.run(
            ["rclone", "config", "file"],
            capture_output=True,
            text=True,
            timeout=8,
        )
        output = (result.stdout or "") + "\n" + (result.stderr or "")
        for line in output.splitlines():
            line = line.strip()
            if line.startswith("/") and line.endswith(".conf"):
                return Path(line)
    except Exception:
        return None
    return None


def has_custom_drive_oauth(remote: str) -> bool:
    path = rclone_config_path()
    if not path or not path.exists():
        return False
    try:
        import configparser
        parser = configparser.RawConfigParser()
        parser.read(path, encoding="utf-8")
        if not parser.has_section(remote):
            return False
        return bool((parser.get(remote, "client_id", fallback="") or "").strip())
    except Exception:
        return False


def drive_health() -> dict[str, Any]:
    cfg = load_config()
    remote = str(cfg.get("drive_remote") or "gdrive")
    target = f"{remote}:{str(cfg.get('drive_root') or '').strip('/')}"
    custom_oauth = has_custom_drive_oauth(remote)

    if shutil.which("rclone") is None:
        return {
            "ok": False,
            "remote": remote,
            "target": target,
            "custom_oauth": custom_oauth,
            "detail": "rclone is not installed",
        }

    try:
        result = subprocess.run(
            [
                "rclone", "lsd", target,
                "--max-depth", "1",
                "--tpslimit", str(max(1, int(cfg.get("cloud_tps_limit", 8)))),
                "--tpslimit-burst", str(max(1, int(cfg.get("cloud_tps_limit", 8)))),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        detail = ((result.stderr or "") or (result.stdout or ""))[-5000:]
        return {
            "ok": result.returncode == 0,
            "remote": remote,
            "target": target,
            "custom_oauth": custom_oauth,
            "detail": detail,
        }
    except Exception as exc:
        return {
            "ok": False,
            "remote": remote,
            "target": target,
            "custom_oauth": custom_oauth,
            "detail": str(exc),
        }


def queue_cloud_sync(force: bool = False) -> None:
    ensure_cloud_worker()
    status = cloud_backoff_status()
    if status["active"] and not force:
        return
    cloud_event.set()


@app.on_event("startup")
def startup_event() -> None:
    cfg = load_config()
    load_ota_jobs()
    ensure_cloud_worker()
    ensure_camera_operation_worker()
    ensure_automatic_snapshot_scheduler()
    if cfg.get("drive_enabled"):
        queue_cloud_sync()


DASHBOARD_PATH = BASE_DIR / "web" / "dashboard.html"


@app.get("/", response_class=HTMLResponse)
def dashboard():
    try:
        return HTMLResponse(
            DASHBOARD_PATH.read_text(encoding="utf-8")
        )
    except Exception as exc:
        return HTMLResponse(
            f"<h1>CamHub</h1><p>Dashboard unavailable: {exc}</p>",
            status_code=500,
        )


DEBUG_PAGE = r"""
<!doctype html>
<html lang="it">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CamHub Errori / Debug</title>
<style>
:root{color-scheme:dark;--bg:#0b0e13;--panel:#131820;--border:#2a3442;--text:#edf2f7;--muted:#9aa8b8;--ok:#77df91;--bad:#ff8585;--warn:#ffd27a;--accent:#6eb6ff}
*{box-sizing:border-box}body{font-family:Arial,sans-serif;background:var(--bg);color:var(--text);margin:0}
header{padding:16px 22px;background:#10151c;border-bottom:1px solid var(--border)}
main{padding:18px;max-width:1450px;margin:auto}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:15px;margin-bottom:14px}
.metric{font-size:25px;font-weight:700;margin-top:5px}.muted{color:var(--muted)}.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--warn)}
a{color:var(--accent)}button,select{font:inherit;padding:9px 11px;border:1px solid var(--border);border-radius:8px;background:#202938;color:var(--text);cursor:pointer}
.actions{display:flex;flex-wrap:wrap;gap:8px;align-items:center}.mono{font-family:Consolas,monospace;white-space:pre-wrap;word-break:break-word;font-size:12px}
.issue{border-top:1px solid var(--border);padding:12px 0}.issue:first-child{border-top:0}.row{display:flex;gap:10px;align-items:flex-start;justify-content:space-between}
.tag{display:inline-block;padding:3px 7px;border-radius:99px;background:#202938;font-size:11px;margin-right:5px}.tag.camera{color:#ffbf70}.tag.protocol{color:#8cc8ff}.tag.camhub{color:#ff8cb8}.tag.cloud{color:#a4d58b}
.sev-critical{color:#ff6464}.sev-error{color:#ff9a7c}.sev-warning{color:#ffd27a}
details{margin-top:7px}summary{cursor:pointer;color:var(--muted)}
@media(max-width:900px){.grid{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<header>
  <h1 style="margin:0 0 4px">Errori / Debug</h1>
  <div class="muted"><a href="/">← Dashboard</a> · problemi raggruppati per origine e gravità</div>
</header>
<main>
  <div class="grid">
    <div class="card"><div class="muted">Camera</div><div id="countCamera" class="metric">0</div></div>
    <div class="card"><div class="muted">Protocollo / rete</div><div id="countProtocol" class="metric">0</div></div>
    <div class="card"><div class="muted">CamHub</div><div id="countCamHub" class="metric">0</div></div>
    <div class="card"><div class="muted">Cloud</div><div id="countCloud" class="metric">0</div></div>
  </div>

  <div class="card">
    <div class="actions">
      <select id="domainFilter" onchange="refresh()">
        <option value="">Tutte le origini</option>
        <option value="camera">Camera</option>
        <option value="protocol">Protocollo / rete</option>
        <option value="camhub">CamHub</option>
        <option value="cloud">Cloud</option>
      </select>
      <select id="severityFilter" onchange="refresh()">
        <option value="">Tutte le gravità</option>
        <option value="critical">Critici</option>
        <option value="error">Errori</option>
        <option value="warning">Avvisi</option>
      </select>
      <label class="muted"><input id="includeCleared" type="checkbox" onchange="refresh()"> mostra anche puliti</label>
      <button onclick="refresh()">Aggiorna</button>
      <button onclick="clearErrors()">Pulisci vista</button>
      <button onclick="testDrive()">Test Google Drive</button>
    </div>
    <div id="clearInfo" class="muted" style="margin-top:8px">
      “Pulisci vista” azzera l'elenco operativo; non cancella media, manifest o file storici di errore.
    </div>
  </div>

  <div class="card">
    <h2 style="margin-top:0">Stato tecnico</h2>
    <div id="status" class="muted">Caricamento...</div>
    <div id="testResult" class="mono" style="margin-top:8px"></div>
  </div>

  <div class="card">
    <h2 style="margin-top:0">Problemi raggruppati</h2>
    <div class="muted" style="margin-bottom:8px">
      Eventi identici vengono raggruppati: così cinquanta errori uguali non nascondono un problema nuovo.
    </div>
    <div id="errors">Caricamento...</div>
  </div>
</main>
<script>
function esc(s){return String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function labelDomain(d){return({camera:'CAMERA',protocol:'PROTOCOLLO',camhub:'CAMHUB',cloud:'CLOUD'})[d]||String(d||'ERRORE').toUpperCase()}
function labelSeverity(s){return({critical:'CRITICO',error:'ERRORE',warning:'AVVISO'})[s]||String(s||'').toUpperCase()}

async function refresh(){
  const [st,sum]=await Promise.all([
    fetch('/api/status').then(r=>r.json()),
    fetch('/api/errors/summary').then(r=>r.json())
  ]);

  document.getElementById('countCamera').textContent=(sum.by_domain||{}).camera||0;
  document.getElementById('countProtocol').textContent=(sum.by_domain||{}).protocol||0;
  document.getElementById('countCamHub').textContent=(sum.by_domain||{}).camhub||0;
  document.getElementById('countCloud').textContent=(sum.by_domain||{}).cloud||0;

  const bo=st.cloud_backoff||{};
  const sev=sum.by_severity||{};
  document.getElementById('status').innerHTML=
    'Critici: <b class="bad">'+Number(sev.critical||0)+'</b> · Errori: <b>'+Number(sev.error||0)+'</b> · Avvisi: '+Number(sev.warning||0)+'<br>'+
    'Rclone: '+(st.rclone_available?'<span class="ok">OK</span>':'<span class="bad">NON TROVATO</span>')+' · '+
    'Drive: '+(st.drive_enabled?'ATTIVO':'DISATTIVO')+' · pendenti '+st.pending_cloud+
    (bo.active?'<br><span class="warn">Backoff cloud: '+bo.remaining_sec+' s</span>':'')+
    (sum.cleared_before?'<br>Vista pulita fino a: '+esc(sum.cleared_before):'');

  const domain=document.getElementById('domainFilter').value;
  const severity=document.getElementById('severityFilter').value;
  const include=document.getElementById('includeCleared').checked;
  const qs=new URLSearchParams({limit:'100'});
  if(domain)qs.set('domain',domain);
  if(severity)qs.set('severity',severity);
  if(include)qs.set('include_cleared','true');

  const errs=await fetch('/api/errors?'+qs.toString()).then(r=>r.json());
  const box=document.getElementById('errors');

  if(!errs.length){
    box.innerHTML='<div class="ok">Nessun problema nella vista corrente.</div>';
    return;
  }

  box.innerHTML=errs.map(er=>{
    const cls='sev-'+esc(er.severity||'warning');
    const repeat=Number(er.count||1)>1?' · <b>×'+Number(er.count)+'</b>':'';
    const range=Number(er.count||1)>1?'<div class="muted">Prima: '+esc(er.first_seen||'')+' · Ultima: '+esc(er.last_seen||'')+'</div>':'<div class="muted">'+esc(er.last_seen||'')+'</div>';
    return '<div class="issue">'+
      '<div class="row"><div>'+
        '<span class="tag '+esc(er.domain)+'">'+labelDomain(er.domain)+'</span>'+
        '<span class="'+cls+'"><b>'+labelSeverity(er.severity)+'</b></span>'+repeat+
        (er.camera_id?' · '+esc(er.camera_id):'')+
      '</div><div class="muted">'+esc(er.category||er.source||'')+'</div></div>'+
      '<div style="margin-top:7px"><b>'+esc(er.title||'')+'</b></div>'+
      range+
      '<details><summary>Dettaglio tecnico</summary><pre class="mono">'+esc(er.detail||'')+'</pre></details>'+
    '</div>';
  }).join('');
}

async function clearErrors(){
  if(!confirm('Pulire la vista errori? I file storici e i media non verranno cancellati.'))return;
  const r=await fetch('/api/errors/clear',{method:'POST'});
  const j=await r.json();
  document.getElementById('clearInfo').textContent=j.message||'Vista pulita.';
  document.getElementById('includeCleared').checked=false;
  await refresh();
}

async function testDrive(){
  const box=document.getElementById('testResult');
  box.textContent='Test in corso...';
  const r=await fetch('/api/cloud/test',{method:'POST'});
  const j=await r.json();
  box.textContent=(j.ok?'OK\n':'ERRORE\n')+(j.detail||'');
  setTimeout(refresh,500);
}

refresh();
setInterval(refresh,15000);
</script>
</body>
</html>
"""

@app.get("/debug", response_class=HTMLResponse)
def debug_page():
    return DEBUG_PAGE


@app.get("/api/config")
def get_config():
    return load_config()


@app.post("/api/config")
def set_config(cfg: ConfigModel):
    data = cfg.model_dump()
    target_camera_id = str(data.get("camera_id") or "CAM01")
    if ota_camera_busy(target_camera_id):
        raise HTTPException(
            409,
            "Camera firmware update is in progress; wait before changing configuration",
        )
    if not 1 <= data["snapshot_interval_sec"] <= 86400:
        raise HTTPException(400, "snapshot_interval_sec must be 1..86400")
    if data["camera_mode"] == "manual":
        data["camera_mode"] = "streaming"
    if data["camera_mode"] not in ("automatic", "streaming", "alarm"):
        raise HTTPException(400, "camera_mode must be automatic, streaming or alarm")
    if not 1 <= data["event_video_sec"] <= 60:
        raise HTTPException(400, "event_video_sec must be 1..60")
    if not 1 <= data["alarm_video_sec"] <= 60:
        raise HTTPException(400, "alarm_video_sec must be 1..60")
    if not 1 <= data["motion_threshold_pct"] <= 80:
        raise HTTPException(400, "motion_threshold_pct must be 1..80")
    if not 5 <= data["motion_pixel_delta"] <= 100:
        raise HTTPException(400, "motion_pixel_delta must be 5..100")
    if not 150 <= data["motion_sample_ms"] <= 5000:
        raise HTTPException(400, "motion_sample_ms must be 150..5000")
    if not 3 <= data["motion_cooldown_sec"] <= 300:
        raise HTTPException(400, "motion_cooldown_sec must be 3..300")
    if data["camera_frame_size"] not in FRAME_SIZES:
        raise HTTPException(400, "Unsupported camera_frame_size")
    if not 4 <= data["jpeg_quality"] <= 30:
        raise HTTPException(400, "jpeg_quality must be 4..30")
    if not 1 <= data["stream_max_fps"] <= 15:
        raise HTTPException(400, "stream_max_fps must be 1..15")
    if not 0 <= data["cloud_batch_delay_sec"] <= 60:
        raise HTTPException(400, "cloud_batch_delay_sec must be 0..60")
    if not 60 <= data["cloud_rate_limit_backoff_sec"] <= 86400:
        raise HTTPException(400, "cloud_rate_limit_backoff_sec must be 60..86400")
    if not 1 <= data["cloud_tps_limit"] <= 100:
        raise HTTPException(400, "cloud_tps_limit must be 1..100")
    for name in ("brightness", "contrast", "saturation"):
        if not -2 <= data[name] <= 2:
            raise HTTPException(400, f"{name} must be -2..2")
    old_cfg = load_config()
    old_mode = str(old_cfg.get("camera_mode") or "automatic")
    camera_id = str(data.get("camera_id") or "CAM01")

    if old_mode == "streaming":
        stop_streaming_mode(camera_id)

    save_config(data)
    if data.get("drive_enabled"):
        queue_cloud_sync()

    camera_applied = False
    camera_message = "camera not yet online"
    try:
        camera_applied, camera_message = push_config_to_node(data["camera_id"], data)
    except Exception as exc:
        camera_message = str(exc)

    if data.get("camera_mode") == "streaming" and camera_applied:
        time.sleep(0.25)
        start_streaming_mode(camera_id)

    return {
        "ok": True,
        "camera_applied": camera_applied,
        "camera_message": camera_message,
    }


@app.get("/api/node/config/{camera_id}")
def node_config(camera_id: str, x_cam_token: str | None = Header(default=None)):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")
    return {
        "camera_id": camera_id,
        "snapshot_interval_sec": cfg["snapshot_interval_sec"],
        "snapshot_source": "server_exclusive_queue",
        "camera_mode": cfg["camera_mode"],
        "alarm_video_sec": cfg["alarm_video_sec"],
        "motion_threshold_pct": cfg["motion_threshold_pct"],
        "motion_pixel_delta": cfg["motion_pixel_delta"],
        "motion_sample_ms": cfg["motion_sample_ms"],
        "motion_cooldown_sec": cfg["motion_cooldown_sec"],
        "camera_frame_size": cfg["camera_frame_size"],
        "jpeg_quality": cfg["jpeg_quality"],
        "horizontal_mirror": cfg["horizontal_mirror"],
        "vertical_flip": cfg["vertical_flip"],
        "brightness": cfg["brightness"],
        "contrast": cfg["contrast"],
        "saturation": cfg["saturation"],
        "stream_max_fps": cfg["stream_max_fps"],
    }


@app.post("/api/node/error")
async def node_error(
    request: Request,
    camera_id: str | None = None,
    category: str = "camera",
    x_cam_token: str | None = Header(default=None),
):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")

    camera = camera_id or cfg["camera_id"]
    payload = await request.body()
    message = payload[:16384].decode("utf-8", errors="replace").strip()
    if not message:
        raise HTTPException(400, "Empty error report")

    path = create_camera_error_file(camera, category, message, "camera_firmware")
    return {
        "ok": True,
        "file": path.name,
        "relative": path.relative_to(DATA_DIR).as_posix(),
    }


@app.post("/api/node/heartbeat")
async def node_heartbeat(request: Request, x_cam_token: str | None = Header(default=None)):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")
    payload = await request.json()
    camera_id = str(payload.get("camera_id") or cfg["camera_id"])
    payload["camera_id"] = camera_id
    payload["last_seen"] = now_local().isoformat()
    payload["observed_ip"] = request.client.host if request.client else None
    save_node(camera_id, payload)

    if cfg.get("camera_mode") == "streaming":
        start_streaming_mode(camera_id)

    return {"ok": True}

@app.post("/api/node/alarm")
async def node_alarm(
    request: Request,
    x_cam_token: str | None = Header(default=None),
):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")
    if cfg.get("camera_mode") != "alarm":
        raise HTTPException(409, "CamHub is not in alarm mode")

    payload = await request.json()
    camera_id = str(payload.get("camera_id") or cfg["camera_id"])
    if ota_camera_busy(camera_id):
        raise HTTPException(409, "Camera firmware update is in progress")
    seconds = max(
        1,
        min(
            int(payload.get("video_sec") or cfg.get("alarm_video_sec", 10)),
            60,
        ),
    )

    now_mono = time.monotonic()
    last = float(alarm_last_event.get(camera_id) or 0.0)
    if now_mono - last < 3.0:
        return {
            "ok": True,
            "deduplicated": True,
            "message": "Alarm already received recently",
        }

    alarm_last_event[camera_id] = now_mono
    item = enqueue_camera_operation(
        "alarm_video",
        camera_id,
        duration=seconds,
        origin="alarm",
        priority=True,
    )

    return {
        "ok": True,
        "deduplicated": False,
        "operation": item,
    }



@app.post("/api/ota/upload")
async def ota_upload(
    file: UploadFile = File(...),
    version: str = "",
):
    clean_version = "".join(
        ch for ch in version.strip()
        if ch.isalnum() or ch in "._-"
    )[:31]

    if not clean_version:
        raise HTTPException(400, "Firmware version is required")

    filename = (file.filename or "").lower()
    if not filename.endswith(".bin"):
        raise HTTPException(400, "Upload a PlatformIO firmware.bin file")

    temp_path = FIRMWARE_DIR / "camnode-upload.tmp"
    digest = hashlib.sha256()
    total = 0
    first_byte: bytes | None = None

    try:
        with temp_path.open("wb") as output:
            while True:
                chunk = await file.read(64 * 1024)
                if not chunk:
                    break

                if first_byte is None:
                    first_byte = chunk[:1]

                total += len(chunk)
                if total >= OTA_SLOT_SIZE:
                    raise HTTPException(
                        413,
                        (
                            f"Firmware is too large for the OTA slot "
                            f"({OTA_SLOT_SIZE} bytes)"
                        ),
                    )

                digest.update(chunk)
                output.write(chunk)

        if total < 64 * 1024:
            raise HTTPException(400, "Firmware file is unexpectedly small")

        if first_byte != b"\xE9":
            raise HTTPException(
                400,
                "File does not look like an ESP32 application image",
            )

        sha256 = digest.hexdigest()
        uploaded_at = now_local().isoformat()

        archive_name = (
            f"camnode-{clean_version}-{sha256[:12]}.bin"
        )
        archive_path = FIRMWARE_DIR / "archive" / archive_name
        shutil.copy2(temp_path, archive_path)
        temp_path.replace(OTA_FIRMWARE_PATH)

        manifest = {
            "product": "CamNode ESP32-CAM",
            "version": clean_version,
            "sha256": sha256,
            "size": total,
            "uploaded_at": uploaded_at,
            "filename": archive_name,
            "ota_slot_size": OTA_SLOT_SIZE,
        }

        manifest_tmp = OTA_MANIFEST_PATH.with_suffix(".tmp")
        manifest_tmp.write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )
        manifest_tmp.replace(OTA_MANIFEST_PATH)

        return {
            "ok": True,
            "manifest": manifest,
        }

    finally:
        temp_path.unlink(missing_ok=True)


@app.get("/api/ota/firmware.bin")
def ota_firmware_download(
    x_cam_token: str | None = Header(default=None),
):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")

    manifest = load_ota_manifest()
    if manifest is None:
        raise HTTPException(404, "No OTA firmware has been uploaded")

    return FileResponse(
        OTA_FIRMWARE_PATH,
        media_type="application/octet-stream",
        filename="firmware.bin",
        headers={
            "Cache-Control": "no-store",
            "X-Firmware-Version": str(manifest["version"]),
            "X-Firmware-SHA256": str(manifest["sha256"]),
        },
    )


def _refresh_ota_job(camera_id: str, node: dict[str, Any]) -> dict[str, Any] | None:
    with ota_job_lock:
        job = dict(ota_jobs.get(camera_id) or {})

    if not job:
        return None

    target = str(job.get("target_version") or "")
    installed = str(node.get("firmware") or "")
    state = str(job.get("state") or "")

    if target and installed == target and state != "completed":
        job = set_ota_job(
            camera_id,
            state="completed",
            progress_pct=100,
            completed_at=now_local().isoformat(),
            error="",
        )

        cfg = load_config()
        if cfg.get("camera_mode") == "streaming":
            start_streaming_mode(camera_id)

        return job

    if state in {"completed", "error"}:
        return job

    started_at = str(job.get("started_at") or "")
    if started_at:
        try:
            started = datetime.fromisoformat(started_at)
            if (now_local() - started).total_seconds() > 600:
                return set_ota_job(
                    camera_id,
                    state="error",
                    error="OTA confirmation timed out after 10 minutes",
                    failed_at=now_local().isoformat(),
                )
        except Exception:
            pass

    ota_status_url = str(node.get("ota_status_url") or "")
    if ota_status_url:
        try:
            req = urllib.request.Request(
                ota_status_url,
                headers={"User-Agent": "CamHub/1.0"},
            )
            with urllib.request.urlopen(req, timeout=1.5) as response:
                payload = json.loads(
                    response.read(8192).decode(
                        "utf-8",
                        errors="replace",
                    )
                )

            camera_state = str(payload.get("state") or state)
            camera_error = str(payload.get("error") or "")
            progress = int(payload.get("progress_pct") or 0)

            if camera_state == "error":
                return set_ota_job(
                    camera_id,
                    state="error",
                    progress_pct=progress,
                    error=camera_error or "Camera OTA failed",
                    failed_at=now_local().isoformat(),
                )

            mapped_state = camera_state
            if camera_state == "rebooting":
                mapped_state = "waiting_heartbeat"

            return set_ota_job(
                camera_id,
                state=mapped_state,
                progress_pct=progress,
                bytes_written=int(payload.get("bytes_written") or 0),
                total_bytes=int(payload.get("total_bytes") or 0),
                running_partition=payload.get("running_partition"),
                error=camera_error,
            )
        except Exception:
            if state in {
                "downloading",
                "verifying",
                "rebooting",
                "waiting_heartbeat",
                "requested",
                "preparing",
            }:
                return set_ota_job(
                    camera_id,
                    state="waiting_heartbeat",
                    progress_pct=int(job.get("progress_pct") or 0),
                )

    return job


@app.get("/api/ota/status")
def ota_status():
    manifest = load_ota_manifest()
    now = now_local()
    cameras: list[dict[str, Any]] = []

    for path in NODES_DIR.glob("*.json"):
        try:
            node = json.loads(path.read_text(encoding="utf-8"))
            camera_id = str(node.get("camera_id") or path.stem)

            seen = datetime.fromisoformat(node["last_seen"])
            online = (now - seen).total_seconds() < 45

            job = _refresh_ota_job(camera_id, node)

            cameras.append({
                "camera_id": camera_id,
                "camera_name": node.get("camera_name"),
                "online": online,
                "firmware": node.get("firmware"),
                "ota_capable": bool(node.get("ota_capable")),
                "ota_partition": node.get("ota_partition"),
                "job": job,
            })
        except Exception:
            continue

    return {
        "manifest": manifest,
        "slot_size": OTA_SLOT_SIZE,
        "cameras": cameras,
    }


@app.post("/api/ota/camera/{camera_id}/apply")
def ota_apply(camera_id: str):
    manifest = load_ota_manifest()
    if manifest is None:
        raise HTTPException(404, "Upload a firmware image first")

    node = get_node(camera_id)
    if not node:
        raise HTTPException(404, "Camera node is unknown")

    try:
        seen = datetime.fromisoformat(node["last_seen"])
        if (now_local() - seen).total_seconds() >= 45:
            raise HTTPException(409, "Camera is offline")
    except KeyError:
        raise HTTPException(409, "Camera heartbeat is unavailable")

    installed = str(node.get("firmware") or "")
    target = str(manifest["version"])

    if installed == target:
        return {
            "ok": True,
            "already_current": True,
            "camera_id": camera_id,
            "version": installed,
        }

    if not bool(node.get("ota_capable")):
        raise HTTPException(
            409,
            (
                "This camera does not yet have the dual-slot OTA layout. "
                "Flash CamNode 0.8.0 once by USB first."
            ),
        )

    if ota_camera_busy(camera_id):
        raise HTTPException(409, "An OTA update is already active")

    with camera_operation_lock:
        if camera_operation_active is not None or camera_operation_queue:
            raise HTTPException(
                409,
                "Wait for the camera operation queue to become idle",
            )

    cfg = load_config()
    previous_mode = str(cfg.get("camera_mode") or "automatic")

    if previous_mode == "streaming":
        stop_streaming_mode(camera_id, wait_sec=5.0)

    start_url = str(node.get("ota_start_url") or "")
    if not start_url:
        if previous_mode == "streaming":
            start_streaming_mode(camera_id)
        raise HTTPException(
            409,
            "Camera firmware does not expose the OTA start endpoint",
        )

    query = urllib.parse.urlencode({
        "version": target,
        "sha256": str(manifest["sha256"]),
    })

    req = urllib.request.Request(
        start_url + "?" + query,
        method="POST",
        headers={
            "User-Agent": "CamHub/1.0",
            "X-Cam-Token": str(cfg["upload_token"]),
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=8) as response:
            body = response.read(8192).decode(
                "utf-8",
                errors="replace",
            )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if previous_mode == "streaming":
            start_streaming_mode(camera_id)
        raise HTTPException(
            502,
            f"Camera rejected OTA: HTTP {exc.code} {detail}",
        ) from exc
    except Exception as exc:
        if previous_mode == "streaming":
            start_streaming_mode(camera_id)
        raise HTTPException(
            502,
            f"Unable to start OTA on camera: {exc}",
        ) from exc

    job = set_ota_job(
        camera_id,
        state="requested",
        progress_pct=0,
        target_version=target,
        source_version=installed,
        sha256=str(manifest["sha256"]),
        size=int(manifest["size"]),
        previous_mode=previous_mode,
        started_at=now_local().isoformat(),
        response=body[-2000:],
        error="",
    )

    return {
        "ok": True,
        "already_current": False,
        "job": job,
    }


@app.get("/api/nodes")
def api_nodes():
    now = now_local()
    nodes: list[dict[str, Any]] = []
    for path in NODES_DIR.glob("*.json"):
        try:
            node = json.loads(path.read_text(encoding="utf-8"))
            seen = datetime.fromisoformat(node["last_seen"])
            node["online"] = (now - seen).total_seconds() < 45
            stream_state = streaming_mode_status(
                str(node.get("camera_id") or "")
            )
            node["ingest_fps"] = stream_state["source_fps"]
            node["stream_running"] = stream_state["running"]
            node["stream_frame_available"] = stream_state["frame_available"]
            nodes.append(node)
        except Exception:
            continue
    return nodes


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
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(415, "Only image uploads are accepted")
    camera = camera_id or cfg["camera_id"]
    dt = now_local()
    out_path = camera_day_dir(camera, dt) / make_filename(camera, event_type, dt, ".jpg")
    with out_path.open("wb") as output:
        shutil.copyfileobj(file.file, output)
    register_media(out_path, camera, event_type, dt, "multipart")
    return {"ok": True, "file": out_path.name, "relative": out_path.relative_to(DATA_DIR).as_posix()}


@app.post("/api/upload/raw")
async def upload_raw_image(
    request: Request,
    camera_id: str | None = None,
    event_type: str = "periodic",
    x_cam_token: str | None = Header(default=None),
):
    cfg = load_config()
    if x_cam_token != cfg["upload_token"]:
        raise HTTPException(401, "Invalid camera token")
    if not request.headers.get("content-type", "").lower().startswith("image/jpeg"):
        raise HTTPException(415, "Content-Type must be image/jpeg")
    payload = await request.body()
    if not payload:
        raise HTTPException(400, "Empty JPEG body")
    if len(payload) > 8 * 1024 * 1024:
        raise HTTPException(413, "Image too large")
    camera = camera_id or cfg["camera_id"]
    dt = now_local()
    out_path = camera_day_dir(camera, dt) / make_filename(camera, event_type, dt, ".jpg")
    out_path.write_bytes(payload)
    source = "alarm_node_upload" if event_type == "alarm" else "raw_jpeg"
    meta = register_media(out_path, camera, event_type, dt, source)
    return {"ok": True, "file": out_path.name, "relative": out_path.relative_to(DATA_DIR).as_posix(), "size": len(payload), "sha256": meta["sha256"]}


def _node_url(camera_id: str, field: str, fallback_path: str) -> str:
    node = get_node(camera_id)
    if not node:
        raise HTTPException(503, "Camera node has not sent a heartbeat yet")
    url = node.get(field)
    if not url:
        base = str(node.get("base_url") or "").rstrip("/")
        if not base:
            raise HTTPException(503, "Camera node URL unavailable")
        url = base + fallback_path
    return str(url)


def push_config_to_node(camera_id: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    try:
        base = _node_url(camera_id, "control_url", "/control")
        query = (
            f"?mode={cfg['camera_mode']}"
            f"&mirror={1 if cfg['horizontal_mirror'] else 0}"
            f"&flip={1 if cfg['vertical_flip'] else 0}"
            f"&frame={cfg['camera_frame_size']}"
            f"&quality={cfg['jpeg_quality']}"
            f"&brightness={cfg['brightness']}"
            f"&contrast={cfg['contrast']}"
            f"&saturation={cfg['saturation']}"
            f"&fps={cfg['stream_max_fps']}"
            f"&interval={cfg['snapshot_interval_sec']}"
            f"&alarm_video={cfg['alarm_video_sec']}"
            f"&motion_pct={cfg['motion_threshold_pct']}"
            f"&motion_delta={cfg['motion_pixel_delta']}"
            f"&motion_ms={cfg['motion_sample_ms']}"
            f"&motion_cooldown={cfg['motion_cooldown_sec']}"
        )
        req = urllib.request.Request(base + query, headers={"User-Agent": "CamHub/0.4.1"})
        with urllib.request.urlopen(req, timeout=8) as response:
            body = response.read(4096).decode("utf-8", errors="replace")
        return True, body
    except Exception as exc:
        return False, str(exc)


@app.post("/api/camera/mode/{mode}")
def set_camera_mode(mode: str):
    global automatic_next_due

    normalized = mode.strip().lower()
    if normalized == "manual":
        normalized = "streaming"
    if normalized not in ("automatic", "streaming", "alarm"):
        raise HTTPException(
            400,
            "Mode must be automatic, streaming or alarm",
        )

    cfg = load_config()
    old_mode = str(cfg.get("camera_mode") or "automatic")
    camera_id = str(cfg.get("camera_id") or "CAM01")

    if ota_camera_busy(camera_id):
        raise HTTPException(
            409,
            "Camera firmware update is in progress",
        )

    with camera_operation_lock:
        if camera_operation_active is not None:
            raise HTTPException(
                409,
                "Wait for the current camera operation to finish before changing mode",
            )

    stop_streaming_mode(camera_id)

    with camera_operation_lock:
        cancelled = len(camera_operation_queue)
        camera_operation_queue.clear()

    cfg["camera_mode"] = normalized
    save_config(cfg)

    if normalized == "automatic":
        automatic_next_due = (
            time.monotonic()
            + max(1, int(cfg.get("snapshot_interval_sec", 60)))
        )
    else:
        automatic_next_due = 0.0

    applied, message = push_config_to_node(camera_id, cfg)
    if not applied:
        cfg["camera_mode"] = old_mode
        save_config(cfg)
        if old_mode == "streaming":
            start_streaming_mode(camera_id)
        raise HTTPException(
            503,
            f"Camera did not apply the requested mode; server mode was restored: {message}",
        )

    if normalized == "streaming":
        time.sleep(0.3)
        start_streaming_mode(camera_id)

    result = camera_operation_status()
    result["cancelled_queued_from_previous_mode"] = cancelled
    result["camera_applied"] = applied
    result["camera_message"] = message
    return result


@app.get("/api/camera/queue")
def camera_queue():
    return camera_operation_status()


@app.post("/api/camera/{camera_id}/capture")
def manual_capture(camera_id: str):
    cfg = load_config()
    if ota_camera_busy(camera_id):
        raise HTTPException(409, "Camera firmware update is in progress")
    if cfg.get("camera_mode") != "streaming":
        raise HTTPException(
            409,
            "Photo is available only in streaming mode.",
        )

    item = enqueue_camera_operation(
        "stream_photo",
        camera_id,
        origin="streaming",
    )
    return {
        "ok": True,
        "queued": True,
        "operation": item,
    }


@app.post("/api/camera/{camera_id}/record")
def record_video(camera_id: str, duration: int | None = None):
    cfg = load_config()
    if ota_camera_busy(camera_id):
        raise HTTPException(409, "Camera firmware update is in progress")
    if cfg.get("camera_mode") != "streaming":
        raise HTTPException(
            409,
            "Manual video is available only in streaming mode.",
        )

    seconds = max(1, min(int(duration or cfg["event_video_sec"]), 60))
    item = enqueue_camera_operation(
        "stream_video",
        camera_id,
        duration=seconds,
        origin="streaming",
    )
    return {
        "ok": True,
        "queued": True,
        "operation": item,
    }


@app.get("/api/camera/{camera_id}/live")
def live_proxy(camera_id: str):
    cfg = load_config()
    if cfg.get("camera_mode") != "streaming":
        raise HTTPException(
            409,
            "Live view is available only in streaming mode.",
        )

    start_streaming_mode(camera_id)

    def generate():
        last_seq = -1

        while load_config().get("camera_mode") == "streaming":
            frame = get_stream_frame(camera_id, max_age=5.0)
            if frame is None:
                time.sleep(0.05)
                continue

            seq = int(frame.get("seq") or 0)
            if seq == last_seq:
                time.sleep(0.02)
                continue

            last_seq = seq
            payload = bytes(frame["frame"])

            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                + f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii")
                + payload
                + b"\r\n"
            )

    return StreamingResponse(
        generate(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/recent")
def api_recent(limit: int = 50):
    return recent_items(max(1, min(limit, 200)))


@app.get("/api/errors")
def api_errors(
    limit: int = 50,
    domain: str | None = None,
    severity: str | None = None,
    include_cleared: bool = False,
):
    max_items = max(1, min(limit, 200))
    domain_filter = domain if domain in {
        "camera", "protocol", "camhub", "cloud"
    } else None
    severity_filter = severity if severity in {
        "critical", "error", "warning"
    } else None

    return grouped_errors(
        include_cleared=include_cleared,
        domain_filter=domain_filter,
        severity_filter=severity_filter,
    )[:max_items]


@app.get("/api/errors/summary")
def api_error_summary():
    return error_summary()


@app.post("/api/errors/clear")
def clear_visible_errors():
    cleared_at = now_local().isoformat()
    save_error_state({"cleared_before": cleared_at})

    with runtime_error_lock:
        runtime_errors.clear()

    return {
        "ok": True,
        "cleared_before": cleared_at,
        "message": (
            "Error view cleared. Media, manifests and historical error files "
            "were not deleted."
        ),
    }


@app.get("/api/status")
def api_status():
    cfg = load_config()
    files = media_files()
    total_bytes = sum(path.stat().st_size for path in files)
    pending = sum(1 for path in files if read_metadata(path).get("cloud_status") in ("PENDING", "ERROR", "UPLOADING"))
    latest = latest_jpg()
    backoff = cloud_backoff_status()
    return {
        "server_name": cfg["server_name"],
        "media_count": len(files),
        "data_mb": round(total_bytes / 1024 / 1024, 2),
        "rclone_available": shutil.which("rclone") is not None,
        "drive_enabled": cfg.get("drive_enabled", False),
        "pending_cloud": pending,
        "cloud_backoff": backoff,
        "drive_custom_oauth": has_custom_drive_oauth(str(cfg.get("drive_remote") or "gdrive")),
        "camera_mode": cfg.get("camera_mode", "automatic"),
        "camera_queue": camera_operation_status(),
        "streaming": streaming_mode_status(str(cfg.get("camera_id") or "CAM01")),
        "latest_name": latest.name if latest else None,
        "latest_url": f"/data/{latest.relative_to(DATA_DIR).as_posix()}" if latest else None,
        "errors": error_summary(),
    }


@app.post("/api/cloud/sync-pending")
def sync_pending(force: bool = False):
    cfg = load_config()
    if not cfg.get("drive_enabled"):
        raise HTTPException(400, "Google Drive sync is disabled")

    status = cloud_backoff_status()
    if status["active"] and not force:
        return {
            "queued": False,
            "paused": True,
            "remaining_sec": status["remaining_sec"],
            "reason": status["reason"],
        }

    if force:
        clear_cloud_backoff()

    queue_cloud_sync(force=True)
    return {"queued": True, "forced": force}


@app.post("/api/cloud/test")
def test_cloud():
    result = drive_health()
    if not result["ok"]:
        record_runtime_error("google_drive_test", result.get("detail", "Google Drive test failed"))
    return result


@app.post("/api/cloud/oauth/start")
async def start_google_oauth(request: Request):
    payload = await request.json()
    client_id = str(payload.get("client_id") or "").strip()
    supplied_secret = str(payload.get("client_secret") or "").strip()

    if not client_id:
        raise HTTPException(400, "Google OAuth Client ID is required")

    saved_credentials = load_google_oauth_credentials()
    client_secret = supplied_secret

    if not client_secret and saved_credentials.get("client_id") == client_id:
        client_secret = saved_credentials.get("client_secret", "")

    if not client_secret:
        raise HTTPException(
            400,
            "Google OAuth Client Secret is required the first time. "
            "Open this OAuth client in Google Cloud Console and copy its Client Secret."
        )

    if supplied_secret:
        save_google_oauth_credentials(client_id, client_secret)

    result = google_form_post(
        "https://oauth2.googleapis.com/device/code",
        {
            "client_id": client_id,
            "scope": "https://www.googleapis.com/auth/drive.file",
        },
        timeout=20,
    )

    if not result.get("device_code"):
        detail = str(result.get("error_description") or result.get("error") or result)
        record_runtime_error("google_oauth", detail)
        raise HTTPException(502, detail)

    cfg = load_config()
    cfg["google_oauth_client_id"] = client_id
    save_config(cfg)

    expires_in = int(result.get("expires_in") or 1800)
    interval = int(result.get("interval") or 5)
    verification_url = str(
        result.get("verification_url")
        or result.get("verification_uri")
        or "https://www.google.com/device"
    )
    expires_at = time.time() + expires_in

    with google_oauth_lock:
        google_oauth_state.update({
            "status": "pending",
            "message": "Waiting for Google authorization.",
            "user_code": str(result.get("user_code") or ""),
            "verification_url": verification_url,
            "expires_at": expires_at,
            "device_code": str(result["device_code"]),
            "client_id": client_id,
        })

    threading.Thread(
        target=google_device_oauth_worker,
        args=(client_id, client_secret, str(result["device_code"]), interval, expires_at),
        daemon=True,
        name="camhub-google-oauth",
    ).start()

    return {
        "ok": True,
        "status": "pending",
        "user_code": str(result.get("user_code") or ""),
        "verification_url": verification_url,
        "expires_in": expires_in,
        "scope": "drive.file",
    }


@app.get("/api/cloud/oauth/status")
def google_oauth_status():
    state = google_oauth_public_status()
    credentials = load_google_oauth_credentials()
    state["connected"] = has_custom_drive_oauth(str(load_config().get("drive_remote") or "gdrive"))
    state["client_secret_configured"] = bool(credentials.get("client_secret"))
    state["configured_client_id"] = credentials.get("client_id", "")
    return state


@app.post("/api/cloud/oauth/disconnect")
def disconnect_google_oauth():
    cfg = load_config()
    remote = str(cfg.get("drive_remote") or "gdrive")

    try:
        result = subprocess.run(
            ["rclone", "config", "disconnect", f"{remote}:"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        detail = ((result.stderr or "") or (result.stdout or "")).strip()
        if result.returncode != 0:
            raise HTTPException(502, detail or "rclone disconnect failed")
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, "Google Drive disconnect timed out") from exc

    with google_oauth_lock:
        google_oauth_state.update({
            "status": "idle",
            "message": "Google Drive disconnected.",
            "user_code": "",
            "verification_url": "",
            "expires_at": 0.0,
        })

    return {"ok": True, "message": "Google Drive disconnected from CamHub."}


@app.post("/api/maintenance/cleanup")
def cleanup_retention():
    cfg = load_config()
    cutoff = now_local() - timedelta(days=max(1, int(cfg.get("retention_days", 7))))
    deleted = 0
    for path in media_files():
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=now_local().tzinfo)
        if modified < cutoff:
            metadata_path(path).unlink(missing_ok=True)
            legacy_metadata_path(path).unlink(missing_ok=True)
            path.unlink(missing_ok=True)
            deleted += 1
    return {"deleted": deleted, "cutoff": cutoff.isoformat()}


@app.get("/data/{file_path:path}")
def serve_data(file_path: str):
    candidate = (DATA_DIR / file_path).resolve()
    data_root = DATA_DIR.resolve()
    if data_root not in candidate.parents and candidate != data_root:
        raise HTTPException(403, "Forbidden")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(404, "File not found")
    if candidate.suffix.lower() not in (".jpg", ".jpeg", ".json", ".mp4", ".txt"):
        raise HTTPException(403, "File type not served")
    return FileResponse(candidate)