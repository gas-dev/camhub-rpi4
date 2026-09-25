import argparse
from pathlib import Path
import requests

parser = argparse.ArgumentParser()
parser.add_argument("image", type=Path)
parser.add_argument("--server", default="http://127.0.0.1:8080")
parser.add_argument("--token", default="change-me-now")
parser.add_argument("--camera", default="CAM01")
parser.add_argument("--event", default="manual_test")
args = parser.parse_args()

with args.image.open("rb") as f:
    r = requests.post(
        f"{args.server}/api/upload",
        params={"camera_id": args.camera, "event_type": args.event},
        headers={"X-Cam-Token": args.token},
        files={"file": (args.image.name, f, "image/jpeg")},
        timeout=30,
    )

print(r.status_code)
print(r.text)
