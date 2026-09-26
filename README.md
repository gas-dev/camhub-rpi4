# CamHub RPi4

CamHub is the Raspberry Pi server for the CamNode ESP32-CAM project.

## Current version: 0.6.1

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


## Connect Google Drive from the dashboard

CamHub 0.6.0 can complete Google Drive authorization directly from the web dashboard without manually copying OAuth tokens.

Because a CamHub prototype normally runs on a private LAN address such as 192.168.x.x over HTTP, a normal Google web-server OAuth callback cannot be used directly. The dashboard therefore uses Google's limited-input/device authorization flow.

One-time Google Cloud preparation:

1. Enable Google Drive API in your Google Cloud project.
2. Configure the OAuth consent screen.
3. Create an OAuth client suitable for TVs / Limited Input Devices.
4. Copy only the Client ID into CamHub. This flow does not require entering a Client Secret in the CamHub dashboard.

Dashboard flow:

1. Enter the Google OAuth Client ID.
2. Click "Collega Google Drive".
3. CamHub asks Google for a temporary user code.
4. The Google authorization page opens in a new browser tab.
5. Enter the displayed code and approve access.
6. CamHub receives the access and refresh tokens server-side.
7. The token is written directly into the local rclone configuration.
8. CamHub creates/tests the configured Drive root and starts synchronizing pending files.

The OAuth token is never returned to dashboard JavaScript and is never committed to Git.

The dashboard connection uses the Google Drive drive.file scope. This deliberately limits CamHub/rclone to files and folders created by this OAuth application, instead of granting access to every file already present in the user's Drive.

The older setup_gdrive_oauth.sh flow remains available as an administrative fallback.


## Google device OAuth client secret

CamHub 0.6.1 fixes the Google limited-input OAuth token exchange.

Google's device authorization flow requires the OAuth Client Secret when polling the token endpoint for a TVs / Limited Input Devices client. The dashboard therefore accepts the Client Secret the first time the Google account is linked.

Security behavior:

- the Client Secret is saved only on the Raspberry Pi in google_oauth.json
- google_oauth.json is ignored by Git
- file permissions are set to 0600
- the secret is never returned by the API
- the secret is never repopulated into the browser after it has been saved
- later reconnects can leave the Client Secret field blank
- rclone receives the same Client ID and Client Secret so refresh-token operations use the dedicated Google OAuth client

Google Cloud location:

    Google Cloud Console -> Google Auth Platform / Clients
    -> open the TVs and Limited Input Devices client
    -> copy Client ID and Client Secret

After updating CamHub, enter both values once and press "Collega Google Drive".
