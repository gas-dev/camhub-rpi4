# CamHub RPi4

CamHub is the Raspberry Pi server for the CamNode ESP32-CAM project.

## Current version: 0.5.1

Architecture:

    ESP32-CAM -> one MJPEG stream -> Raspberry Pi 4 -> live fan-out / recording / archive -> Google Drive

CamHub now keeps a single stream from each ESP32-CAM and redistributes the latest frames locally. This avoids opening competing long-lived streams against the camera.

Current functions:

- high-resolution live view through the Raspberry Pi
- live view remains visible while recording
- manual still capture from the current live frame
- video recording from 1 to 15 FPS
- video keeps the currently selected camera resolution
- configurable VGA / SVGA / XGA / HD / SXGA / UXGA
- configurable JPEG quality
- horizontal mirror and vertical flip control
- brightness, contrast and saturation control
- configurable periodic snapshots
- camera heartbeat and online/offline state
- observed source FPS shown in the dashboard
- H.264 MP4 generation with FFmpeg
- SHA-256 for every archived photo and video
- JSON sidecar metadata
- batched Google Drive synchronization
- local retention cleanup

## Important FPS behavior

The selected FPS is the output video rate, up to 15 FPS.

The camera remains at the selected resolution during recording. CamHub never lowers the resolution automatically.

At very high resolutions such as UXGA, the ESP32-CAM sensor, JPEG encoder, Wi-Fi link or network conditions can deliver fewer unique source frames than the selected output FPS. CamHub displays the observed source FPS and reports the number of distinct source frames used in each recording.

If the source delivers fewer than the requested FPS, CamHub repeats the newest valid frame as necessary so that the MP4 keeps the requested timing and duration.

## Raspberry Pi update

    cd /home/germano/camhub-rpi4
    git pull
    sudo systemctl restart camhub
    systemctl status camhub --no-pager

Dashboard:

    http://RASPBERRY_IP:8080

## Camera firmware

CamNode 0.5.0 is required for the best behavior.

It separates:

    port 80: capture, status and camera controls
    port 81: MJPEG stream

This prevents a long-running live stream from blocking manual capture or camera configuration.

## Google Drive

Google Drive is handled through rclone and pending files are transferred in batches.

Default destination:

    gdrive:CamHub/CAM01/YYYY/MM/DD/

## Not yet enabled

- automatic motion-triggered recording
- pre-event ring buffer
- microSD offline queue
- OTA firmware deployment
- production authentication and encryption
- multi-camera fleet management


## Error diagnostics

CamHub 0.5.1 exposes detailed errors in the dashboard.

- Google Drive/rclone failures show the actual stored error text
- ERROR entries in the media table have a details button
- recent runtime, stream and cloud errors are shown in an Errori recenti panel
- camera-originated errors are archived as TXT files beside the normal media
- camera error TXT files receive SHA-256 metadata and are queued for Google Drive like other evidence files
- historical cloud ERROR rows can show their previously stored cloud_error after upgrading CamHub

Camera error files use names similar to:

    CAM01_CAMERA_ERROR_SNAPSHOT_CAPTURE_YYYYMMDD_HHMMSS_mmm.txt

A cloud ERROR does not by itself mean the camera failed. If the image/video exists locally with size and SHA-256, acquisition succeeded and the failure happened during cloud synchronization.
