# CamHub RPi4

CamHub is the Raspberry Pi 4 server for the CamNode camera project.

## V1 scope

The first version intentionally keeps the architecture simple:

ESP32-CAM -> HTTP JPEG upload -> Raspberry Pi 4 -> local archive -> Google Drive

Current features:

- JPEG upload endpoint with camera token
- automatic folders by camera and date
- JSON metadata beside each acquired image
- local web dashboard
- latest image and recent acquisition list
- basic configuration page
- optional Google Drive synchronization through rclone
- retry of pending or failed cloud uploads
- local operation when Internet or Google Drive is unavailable

## Raspberry Pi

Recommended OS: Raspberry Pi OS Lite 64-bit.

Clone the repository:

    git clone https://github.com/gas-dev/camhub-rpi4.git
    cd camhub-rpi4
    chmod +x install_rpi.sh install_service.sh
    ./install_rpi.sh

Run manually for the first test:

    .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080

Find the Raspberry Pi address:

    hostname -I

Open from another device on the LAN:

    http://RASPBERRY_IP:8080

When the manual test is successful:

    ./install_service.sh

## Google Drive

Configure rclone:

    rclone config

Create a Google Drive remote named gdrive, then test it:

    rclone lsd gdrive:

Enable Drive from the CamHub dashboard after the rclone remote works.

Default destination:

    gdrive:CamHub/CAM01/YYYY/MM/DD/

## Test without ESP32-CAM

On a PC with Python:

    pip install requests
    python test_upload.py test.jpg --server http://RASPBERRY_IP:8080 --token change-me-now

The JPEG should immediately appear in the CamHub dashboard.

## Camera API contract

The first CamNode firmware will upload JPEG images using:

    POST /api/upload?camera_id=CAM01&event_type=periodic
    X-Cam-Token: <configured token>
    multipart field: file

## Configuration and secrets

config.json is generated locally and is intentionally excluded from Git. Use config.example.json as the template.

Do not commit Wi-Fi passwords, Google credentials, rclone configuration, operational databases, acquired photos or videos.

## Roadmap

V2:
- ESP32-CAM firmware
- automatic configurable snapshots
- manual snapshot from CamHub
- live MJPEG streaming
- ESP32 diagnostics

V3:
- motion events
- event video on Raspberry with FFmpeg
- configurable event duration
- Google Drive video upload

V4:
- pre-event and post-event buffering
- event timeline
- multi-camera support
