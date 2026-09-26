# CamHub RPi4

CamHub is the Raspberry Pi server for the CamNode ESP32-CAM project.

## Current version: 0.4.0

Architecture:

    ESP32-CAM -> Wi-Fi -> Raspberry Pi 4 -> local archive -> Google Drive

Current functions:

- raw JPEG and multipart JPEG upload
- automatic folders by camera and date
- JSON sidecar metadata
- SHA-256 hash for every archived photo and video
- live node heartbeat and online/offline status
- automatic discovery of the camera LAN address
- live MJPEG view from the dashboard
- manual still capture from the dashboard
- 10-second video recording by default through FFmpeg
- configurable recording duration up to 60 seconds
- Raspberry-side MP4/H.264 creation
- camera resolution control
- JPEG quality control
- mirror and vertical flip control
- brightness, contrast and saturation control
- configurable live FPS
- configurable periodic snapshot interval
- local retention cleanup
- recent media list for photos and videos
- batched Google Drive synchronization with multiple transfers
- retry of failed cloud items
- local operation when Internet or Google Drive is unavailable

## Raspberry Pi update

    cd /home/germano/camhub-rpi4
    git pull
    sudo systemctl restart camhub
    systemctl status camhub --no-pager

Dashboard:

    http://RASPBERRY_IP:8080

## Google Drive

The Google Drive remote remains managed through rclone.

Default destination:

    gdrive:CamHub/CAM01/YYYY/MM/DD/

CamHub v0.4 batches pending media and metadata into fewer rclone operations instead of starting a separate cloud transfer for every image. This reduces process overhead and should make bursts of captures noticeably faster.

## Video

CamNode exposes an MJPEG stream on port 81. CamHub records the stream using FFmpeg and converts it to browser-friendly H.264 MP4.

Default recording duration:

    10 seconds

The duration can be changed in the dashboard up to 60 seconds for testing.

## Camera configuration

CamNode requests configuration from CamHub every approximately 30 seconds. Changes from the dashboard therefore do not require reflashing the ESP32.

Default test profile:

    SXGA
    JPEG quality 8
    mirror off
    flip off
    5 FPS live
    60-second periodic snapshots

If SXGA live streaming is too heavy for the ESP32-CAM, XGA is the recommended first fallback while keeping materially better detail than the original SVGA test.

## Not yet enabled

The following are intentionally deferred until the live/video build is stable:

- automatic motion-triggered recording
- pre-event ring buffer
- microSD offline queue
- OTA firmware deployment
- production authentication and encryption
- multi-camera fleet management
