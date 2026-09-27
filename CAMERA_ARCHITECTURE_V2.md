# CamHub 2.x camera-control contract

CamHub 2.x treats CamNode as an asynchronous state machine.

## Command semantics

A successful HTTP control response means only that the command was accepted.

A mode/configuration change is complete only after a fresh heartbeat reports the expected `active_mode` and `state`.

CamHub does not roll a mode back merely because a camera transition takes time.

## Streaming ownership

CamHub maintains one shared ESP32 MJPEG ingestion connection in Streaming mode.

Browser live view, manual stills and manual video reuse the Raspberry's shared frame cache.

CamHub opens MJPEG only when heartbeat reports:
- active_mode=streaming
- state=ready
- camera_ready=true
- a recent CamNode frame exists

If CamNode reports transition/recovering/error/standby, Raspberry ingestion closes quietly. Those states are not protocol errors.

A protocol error is surfaced only when a camera continues to report READY while MJPEG remains unavailable for 45 seconds.

## Automatic mode

Automatic photos are command based.

CamHub POSTs a short capture request to CamNode. CamNode cameraTask performs the capture and uploads the JPEG back to CamHub.

CamHub no longer performs a long synchronous GET /capture.

## Alarm mode

CamNode owns motion detection and notifies CamHub after switching into the temporary JPEG alarm-event phase.

CamHub then records the alarm video directly from the authenticated CamNode MJPEG endpoint using wall-clock duration and measured FPS.

## OTA

For CamNode 1.x OTA:
1. stop Raspberry streaming ingestion
2. save previous operating mode
3. set CamHub desired mode to standby
4. send control command
5. wait up to 30 s for confirmed standby
6. request OTA
7. wait for target firmware heartbeat
8. restore previous mode
9. mark OTA complete only after previous mode is acknowledged

CamNode 0.8.x uses a one-time legacy migration path because it cannot enter standby.

## Error priority

Camera state errors and transport errors are kept separate.

Transient mode transitions, expected reboot windows and camera recovery are not logged as protocol failures.

Repeated automatic-capture command failures are surfaced only if the camera still reports READY.

## Operational data shown in dashboard

- desired mode
- active mode
- node state
- camera driver ON/OFF
- camera_ready
- CamNode latest frame age and sequence
- Raspberry ingest FPS
- capture success/attempt/failure counters
- recovery and recovery-failure counters
- sensor power-cycle count
- config desired/active revisions
- last camera error code/message
- Wi-Fi, heap and PSRAM
