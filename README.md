# CamHub RPi4

CamHub is the Raspberry Pi server for the CamNode ESP32-CAM project.

## Current version: 0.5.3

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


## Google Drive production configuration

CamHub 0.5.3 is designed to avoid relying on rclone's shared/default Google OAuth project.

Recommended configuration:

1. Create a Google Cloud project owned by the deployment owner.
2. Enable Google Drive API for that project.
3. Configure the OAuth consent screen as appropriate for the deployment.
4. Create an OAuth Client ID of type Desktop app.
5. On the Raspberry Pi run:

    chmod +x setup_gdrive_oauth.sh
    ./setup_gdrive_oauth.sh

The script asks interactively for the Client ID and Client Secret. Credentials are written only to the local rclone configuration and are never committed to Git.

For a headless Raspberry Pi, rclone may ask you to authorize from another computer with a browser. Follow the URL/remote-authorization instructions printed by rclone.

After authorization, the script tests the configured remote.

The CamHub dashboard shows:

    OAuth dedicato configurato

when a client_id is present for the configured rclone remote.

## Google Drive rate limiting

CamHub now protects the Drive API in several ways:

- pending files are coalesced before cloud transfer
- rclone traffic is capped with a configurable TPS limit
- transfers/checkers are kept conservative
- destination traversal is avoided for targeted file batches
- Google RATE_LIMIT_EXCEEDED responses trigger an automatic backoff
- the cloud worker wakes periodically and retries pending files after backoff expires
- local acquisition continues while Drive is unavailable

Default cloud protection:

    cloud_batch_delay_sec = 5
    cloud_rate_limit_backoff_sec = 600
    cloud_tps_limit = 8

These values can be changed from the dashboard.

## Cloud health test

The dashboard includes Test Google Drive.

It verifies the configured remote/root and reports whether a dedicated OAuth client is configured. The detailed rclone response is shown directly in the web interface and is also available in recent error diagnostics when the test fails.
