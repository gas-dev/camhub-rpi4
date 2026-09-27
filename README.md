# CamHub RPi4

CamHub is the Raspberry Pi server for the CamNode ESP32-CAM project.

## Current version: 2.0.0

CamHub 2.x uses an acknowledged state-machine protocol with CamNode 1.x. The complete contract is in `CAMERA_ARCHITECTURE_V2.md`.

Core flow:

    CamHub command
        -> CamNode returns ACCEPTED
        -> CamNode cameraTask performs transition
        -> heartbeat confirms active_mode + state
        -> CamHub considers the command complete

The four modes are Standby, Automatic, Streaming and Alarm.

Streaming uses one authenticated MJPEG connection from CamNode to Raspberry. Browser live, manual photos and manual video reuse the Raspberry frame cache.

Automatic capture is command based: CamHub sends a short capture command and CamNode uploads the resulting JPEG. There is no synchronous long-running /capture request.

OTA for CamNode 1.x always uses:

    current mode
    -> confirmed Standby
    -> OTA
    -> reboot/new firmware heartbeat
    -> restore previous mode
    -> confirmed completion

The one-time 0.8.x -> 1.x migration is handled as a compatibility exception because legacy firmware does not implement Standby.

## Important FPS behavior

The selected FPS is the maximum requested capture rate. MP4 duration is based on real wall-clock capture time and measured received FPS, so playback duration does not shrink when the ESP32 physically delivers fewer frames.

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


## Debug page

CamHub 0.7.0 keeps technical errors out of the main dashboard.

Open:

    http://RASPBERRY_IP:8080/debug

The Debug page contains:

- recent camera, Google OAuth, rclone and cloud errors
- current Drive/backoff status
- media currently in ERROR state
- full technical error details
- Google Drive connectivity test

The main dashboard contains only a compact Debug / Errori link. An ERROR button in the media table opens the Debug page instead of expanding technical details into the main screen.

## Daily manifest directory

JSON metadata sidecars are now stored in a dedicated manifest directory inside each camera/day directory.

Example local/cloud layout:

    CAM01/
      2026/
        09/
          26/
            CAM01_PERIODIC_20260926_221503_189.jpg
            CAM01_VIDEO_10S_20260926_220709_869.mp4
            manifest/
              CAM01_PERIODIC_20260926_221503_189.json
              CAM01_VIDEO_10S_20260926_220709_869.json

The same relative layout is preserved on Google Drive.

Older adjacent JSON sidecars remain readable for backward compatibility. When an older media item is updated/retried, CamHub writes its current metadata into the new manifest directory.


## Periodic snapshots from the shared live stream

CamHub 0.7.0 archives periodic still images on the Raspberry Pi from the already-running shared MJPEG stream. This removes the second ESP32 camera acquisition path that previously competed with live streaming, especially at UXGA/high JPEG quality.

The configured snapshot interval is unchanged. Periodic images are registered with source shared_live_stream, hashed, given manifest metadata and queued for Google Drive exactly like other media.


## Manual capture from shared live stream

CamHub 0.7.0 removes the legacy direct ESP32 /capture fallback from the manual capture action.

Manual snapshots now:

- use the already-running shared MJPEG stream only
- wait up to 6 seconds for a fresh live frame if the cache is temporarily empty
- archive that frame with source shared_live_stream
- never open a second camera acquisition path on the ESP32

If no recent live frame is available, CamHub returns a 503 diagnostic locally instead of asking the ESP32 for a competing direct capture.


## CamHub 0.7.0 exclusive camera operation model

CamHub now isolates camera functions instead of keeping multiple acquisition paths active at the same time.

Camera modes:

    AUTOMATIC
    - no continuous MJPEG stream
    - one exclusive direct JPEG capture at snapshot_interval_sec
    - default interval: 60 seconds
    - manual Photo/Video buttons are disabled

    MANUAL
    - automatic acquisition is stopped
    - Photo and Video requests are appended to a FIFO queue
    - exactly one camera operation is executed at a time
    - multiple button presses remain queued in their original order

Examples:

    Photo -> Photo -> Video -> Photo

is executed exactly in that sequence.

Video behavior:

- the MJPEG stream is opened only when a queued video operation starts
- the stream is closed when the requested recording duration finishes
- FFmpeg encodes the received JPEG sequence into MP4
- no continuous background live stream is kept open

Photo behavior:

- automatic and manual photos use the ESP32 /capture endpoint exclusively
- there is no simultaneous video/live acquisition when a photo is requested

Dashboard:

- explicit Automatico / Manuale mode buttons
- current active operation
- FIFO queue length and queued operations
- countdown to the next automatic photo
- manual buttons are available only in Manual mode

API:

    POST /api/camera/mode/automatic
    POST /api/camera/mode/manual
    GET  /api/camera/queue

The previous continuous /api/camera/{camera_id}/live proxy is intentionally disabled in exclusive camera mode.


## CamHub 0.8.0 camera architecture

The camera path has been rebuilt around one principle: exactly one acquisition operation owns the ESP32 camera at a time.

There is no persistent background MJPEG ingestion worker anymore.

Automatic mode:

    wait configured interval
    -> queue one PHOTO
    -> execute exclusive /capture
    -> archive + SHA-256 + manifest + cloud queue
    -> return idle

Manual mode:

    automatic scheduler paused
    -> button presses create FIFO operations
    -> one worker executes exactly one item at a time

Example:

    PHOTO -> PHOTO -> VIDEO -> PHOTO

No two items overlap.

Video is no longer expanded to hundreds of temporary JPEG files by Python. CamHub starts FFmpeg directly against the ESP32 MJPEG endpoint only for the requested recording duration, encodes H.264, closes the HTTP stream, waits for camera quiescence, then moves to the next queue item.

Still capture retries transient 409/423/429/503 or connection failures up to three times before declaring the queued operation failed.

The dashboard shows mode, active item, FIFO queue, and automatic countdown. Queue state refreshes every second.

This architecture intentionally removes the old continuous-live cache path because it was competing with direct still capture on constrained ESP32-CAM hardware.


## Three camera modes

CamHub 0.9.0 uses three isolated camera operating modes.

Automatico

    No persistent video stream.
    CamHub queues one exclusive still image at snapshot_interval_sec.
    Default interval is 60 seconds.

Streaming

    Exactly one MJPEG connection is kept open from the ESP32 to the Raspberry.
    The browser live view, manual still images and manual video all reuse this same shared stream.
    A manual photo saves the latest shared frame; it does not call /capture.
    A manual video feeds the shared JPEG frames directly to FFmpeg through stdin; it does not open a second ESP32 stream and does not create a temporary JPEG sequence.
    Multiple manual requests are still processed in FIFO order.

Allarme

    No continuous video is sent to the Raspberry while armed.
    The ESP32 performs local QVGA grayscale motion detection.
    Two consecutive motion-positive samples are required before triggering.
    Global brightness changes are compensated before the changed-pixel percentage is evaluated.
    On trigger, the ESP32:
      1. switches from local grayscale detection to the configured JPEG resolution/quality;
      2. captures and uploads one alarm JPEG;
      3. notifies CamHub;
      4. CamHub queues an alarm video, default 10 seconds;
      5. the ESP32 remains in JPEG mode while CamHub records the video;
      6. after video/cooldown, the ESP32 returns to local motion detection.

Alarm configuration fields:

    alarm_video_sec
    motion_threshold_pct
    motion_pixel_delta
    motion_sample_ms
    motion_cooldown_sec

The alarm image is stored with event_type=alarm. The alarm video is stored with an alarm_video_<N>s event type. Both use the normal SHA-256, manifest and cloud-sync path.


## CamHub 1.0.0 dashboard and OTA

CamHub 1.0.0 reorganizes the operational UI into separate sections without removing the existing camera, media, cloud, alarm or diagnostic functions.

Dashboard sections:

    Panoramica
    Media
    Camera
    Cloud
    Firmware / OTA
    Sistema

The operational overview keeps the three camera modes visible:

    Automatico
    Streaming
    Allarme

Streaming still uses one shared ESP32 -> Raspberry MJPEG connection for browser live, manual photographs and manual video. Automatic and Alarm do not keep a continuous stream open.

### Firmware / OTA

CamHub stores one current CamNode firmware image plus archived uploads.

Firmware upload validates:

    .bin extension
    ESP32 application image magic byte 0xE9
    maximum OTA slot size
    SHA-256

Runtime files are stored under:

    firmware/
      camnode-current.bin
      camnode-manifest.json
      ota-jobs.json
      archive/

The runtime firmware directory is gitignored.

The dashboard Firmware / OTA section shows:

    installed firmware
    available firmware
    camera online status
    active OTA partition
    OTA capability
    update state
    progress
    errors
    update action

CamHub starts OTA only when the camera operation queue is idle. Streaming is stopped before update. Camera configuration, mode changes and new camera operations are blocked while an OTA job is active.

The update is considered complete only after CamHub receives a heartbeat reporting the target firmware version.

OTA jobs are persisted so the dashboard can recover their state after a CamHub restart.

### CI

CamHub GitHub Actions validates Python syntax and config.example.json on each push.


## CamHub 1.0.1 error management

The diagnostic system now separates operational problems into four domains:

    CAMERA
    PROTOCOL
    CAMHUB
    CLOUD

Repeated identical problems are grouped by subsystem, category and message title. A repeated transient protocol/cloud warning is promoted to ERROR after three occurrences so recurring faults become visible without flooding the interface.

Severity levels:

    CRITICAL
    ERROR
    WARNING

The main Panoramica shows only CRITICAL and ERROR groups. Warnings and full technical details remain in Errori / Debug.

The Debug page includes subsystem and severity filters, repeat counts, first/last occurrence and expandable technical detail.

The Media page hides diagnostic .txt entries so normal evidence/media browsing is not cluttered by technical logs.

“Pulisci vista” stores a local cutoff timestamp in error-state.json and clears the in-memory runtime feed. It does not delete media, manifests or historical diagnostic files. Older entries can still be inspected by enabling “mostra anche puliti”.


## CamHub 1.0.2 permanent acquisition markers

Saved still images now receive a small permanent color dot in the top-right corner before archival and SHA-256 registration.

Marker legend:

    BLUE   #2F80ED  Automatico / periodic
    GREEN  #27AE60  Streaming / manual still
    RED    #EB5757  Allarme

The marker is burned into the saved JPEG itself; it is not a browser/dashboard overlay. Therefore the acquisition mode remains visible if the image is copied, exported, opened years later, or viewed without CamHub.

The marker is intentionally small and its radius scales with image resolution.

For provenance, the manifest stores both:

    source_sha256_before_marker
    final media sha256

and also records acquisition_mode plus marker position/color. The normal media SHA-256 is calculated after the marker has been applied, so the archived visible file and its integrity hash always correspond.

Marker rendering is performed by CamHub rather than the ESP32 camera firmware. This avoids adding JPEG decode/re-encode memory pressure to the ESP32-CAM while producing the same permanent visual result in the archived file.


## CamHub 1.0.3 camera health and OTA proof

CamHub now derives a compact camera health state from CamNode heartbeat data:

    OK
    ATTENZIONE
    ERRORE

The health calculation considers online state, Wi-Fi RSSI, minimum free heap, abnormal reset reasons and camera-driver recovery failures.

The dashboard Camera card now shows:

    firmware
    persistent boot number
    uptime
    reset reason
    active OTA partition
    firmware-changed-at-boot indication
    Wi-Fi dBm and percentage
    free heap and minimum free heap
    free PSRAM

The Firmware / OTA table also shows boot count and reset reason so a successful OTA can be verified without opening serial logs.

For the first OTA validation, CamNode 0.8.1 is expected to show a firmware change, one additional persistent boot and an alternate ota_0/ota_1 partition after reboot.


### First 0.8.1 OTA verification

Because 0.8.0 did not yet contain the persistent boot counter, the first 0.8.1 boot intentionally starts the health baseline at boot #1.

CamHub stores source_version and source_partition when the OTA starts. After the 0.8.1 heartbeat confirms the new firmware, the Firmware / OTA page can show the actual slot transition, for example:

    source 0.8.0 / ota_0
    -> target 0.8.1 / ota_1

Subsequent firmware versions can additionally verify previous_firmware and an incremented persistent boot_count.


## CamHub 1.1.0 wall-clock video recording

Video duration is now independent from the requested camera FPS.

The old streaming recorder fed JPEG frames to FFmpeg with the configured FPS as the input framerate. If the ESP32 was configured for 15 FPS but could physically deliver only about 3 FPS at the selected resolution/quality, ten seconds of captured wall-clock time could become roughly two seconds of playback.

The new model uses wall-clock capture duration:

    start monotonic clock
    -> collect every unique JPEG frame received during N real seconds
    -> store frames in one temporary MJPEG spool file
    -> measure actual average FPS = captured_frames / real_elapsed_seconds
    -> encode the MP4 using the measured FPS
    -> ffprobe the final MP4 duration
    -> store timing diagnostics in the manifest
    -> delete the temporary MJPEG spool

The configured stream_max_fps is therefore only a camera/source ceiling. It no longer defines MP4 playback speed.

Manifest fields now include:

    requested_duration_sec
    capture_duration_sec
    encoded_duration_sec
    duration_error_sec
    requested_fps_ceiling
    measured_capture_fps
    captured_frames
    captured_mjpeg_bytes
    timing_model
    stopped_early

Manual recordings support 1 second through 3600 seconds (60 minutes).

Dashboard quick presets:

    10 seconds
    30 seconds
    1 minute
    3 minutes
    10 minutes

During a long recording the queue displays elapsed time, remaining time, percentage, measured FPS and recording/encoding phase.

A Stop video control terminates manual capture early and saves the portion already acquired.

Long recordings use one temporary .mjpg spool instead of thousands of individual temporary JPEG files. CamHub estimates the required working disk space during the first seconds and aborts early if there is insufficient space. Stale temporary spool files are removed automatically on CamHub restart.

Alarm recordings use the same measured-FPS timing model and support up to 10 minutes when paired with CamNode 0.8.1 or later.


## CamHub 1.1.1 self-healing streaming protocol

The Raspberry streaming reader now owns reconnect behavior.

A timeout, remote close or temporary ESP32 stream interruption no longer terminates the streaming worker and immediately produces a protocol error. CamHub clears the stale frame cache, reconnects automatically with bounded backoff and resumes the shared stream when frames return.

Protocol diagnostics are emitted only when the stream has remained unavailable for at least 12 seconds. Repeated diagnostic messages are rate-limited to one per minute while the outage persists.

This means a short ESP32 framebuffer recovery should normally remain invisible to the operator. A persistent outage remains visible through the normal grouped PROTOCOLLO warning/error mechanism.


## CamHub 1.1.2 remote reboot and startup-aware connection recovery

The Firmware / OTA camera list now supports an authenticated Riavvia action for CamNode 0.8.3 or later.

Remote reboot is blocked when:

    OTA is active
    a camera operation is running
    camera operations are queued
    the camera is offline

If Streaming is active, CamHub first closes the shared stream, places the camera in a temporary maintenance window, sends the reboot command, and waits for the new heartbeat.

The camera list can display:

    RIAVVIO
    IN AVVIO
    ONLINE
    OFFLINE

### Startup-aware streaming

CamHub no longer treats an intentional restart, power-cycle recovery, stale heartbeat or CamNode startup-grace period as a protocol failure.

The shared stream reader waits until:

    heartbeat is fresh
    service_ready = true
    maintenance/reboot window has ended

Only then does it connect to the MJPEG endpoint.

For older firmware without service_ready, CamHub applies an uptime-based compatibility grace.

Persistent ready-camera stream outages are still auto-reconnected and reported, but only after about 30 seconds of continuous outage. This avoids diagnostic noise when a camera is unplugged, moved and powered again.

Automatic still capture also waits for camera readiness before calling /capture.


## CamHub 1.1.3 camera-pipeline recovery awareness

CamHub now treats camera_pipeline_healthy=false as a camera recovery state rather than a healthy streaming node.

Manual streaming photos wait up to 20 seconds for the shared stream to self-heal instead of failing after five seconds.

Streaming photo/video availability errors are classified under the streaming subsystem rather than as an internal CamHub programming error.

The Overview camera card exposes framebuffer recovery counters, sensor power cycles and background repair attempts.


## CamHub 1.1.4 robust mode changes

CamHub now allows up to 20 seconds for the node to complete a camera mode transition and retries transient HTTP 503 responses up to three times.

This complements CamNode 0.8.5, which releases the long-lived Streaming camera mutex before applying a new mode or camera configuration.
