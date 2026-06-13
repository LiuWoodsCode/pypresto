import asyncio
import logging
import threading
import time
from collections import deque
from time import monotonic

CONNECTION_WINDOW_SECONDS = 700
MAX_CONNECTION_CHANGES = 4
POLL_INTERVAL_SECONDS = 5

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_monitor_started = False
_latest_devices = []
_device_states = {}
_disabled_devices = {}
_known_devices = {}


def start_background_healthcheck(fetch_devices, poll_interval=POLL_INTERVAL_SECONDS):
    global _monitor_started

    with _lock:
        if _monitor_started:
            return
        _monitor_started = True

    thread = threading.Thread(
        target=_monitor_devices,
        args=(fetch_devices, poll_interval),
        name="idevice-healthcheck",
        daemon=True,
    )
    thread.start()
    logger.info("Started iDevice healthcheck monitor")


def get_devices():
    with _lock:
        return [_copy_device(device) for device in _latest_devices]


def get_device(udid):
    with _lock:
        for device in _latest_devices:
            if device.get("udid") == udid:
                return _copy_device(device)

        disabled_device = _disabled_devices.get(udid)
        if disabled_device is not None:
            return _copy_device(disabled_device)

    return None


def check_devices(devices):
    with _lock:
        return _check_devices(devices)


def _monitor_devices(fetch_devices, poll_interval):
    while True:
        try:
            devices = asyncio.run(fetch_devices())
            with _lock:
                global _latest_devices
                _latest_devices = _check_devices(devices)
            logger.debug("Healthcheck scanned %s connected iDevice(s)", len(devices))
        except Exception:
            logger.exception("Healthcheck scan failed")

        time.sleep(poll_interval)


def _check_devices(devices):
    now = monotonic()
    connected_udids = {device["udid"] for device in devices if device.get("udid")}

    for device in devices:
        udid = device.get("udid")
        if not udid:
            continue

        _observe_device(udid, True, now)
        _remember_device(device)

    for udid in list(_device_states):
        if udid not in connected_udids:
            _observe_device(udid, False, now)

    return _with_disabled_devices(devices)


def _observe_device(udid, connected, now):
    state = _device_states.setdefault(udid, {
        "connected": None,
        "changes": deque(),
    })

    if state["connected"] is None:
        state["connected"] = connected
        logger.info("iDevice %s first seen as %s", udid, _connection_label(connected))
        return

    if state["connected"] == connected:
        return

    state["connected"] = connected
    state["changes"].append(now)
    _trim_changes(state["changes"], now)
    logger.warning(
        "iDevice %s changed connection state to %s (%s change(s) in the health window)",
        udid,
        _connection_label(connected),
        len(state["changes"]),
    )

    if len(state["changes"]) >= MAX_CONNECTION_CHANGES:
        _disable_device(udid)


def _remember_device(device):
    udid = device["udid"]
    _known_devices[udid] = {**device}

    existing = _disabled_devices.get(udid)
    if existing is not None:
        existing.update(device)
        existing["disabled"] = True
        existing["connected"] = True


def _disable_device(udid):
    was_disabled = udid in _disabled_devices
    disabled_device = _disabled_devices.setdefault(udid, _known_devices.get(udid, {
        "name": "Disconnected iDevice",
        "model": "Unknown",
        "product": "Unknown",
        "version": "Unknown",
        "udid": udid,
    }))
    disabled_device["disabled"] = True
    disabled_device["connected"] = _device_states[udid]["connected"]
    disabled_device["warning"] = "This device has been disabled due to a connection issue"

    if not was_disabled:
        logger.error("Disabled iDevice %s due to repeated connection issues", udid)


def _with_disabled_devices(devices):
    devices_without_udid = [
        {**device, "disabled": False, "connected": bool(device.get("udid"))}
        for device in devices
        if not device.get("udid")
    ]
    devices_by_udid = {
        device["udid"]: {**device, "disabled": False, "connected": True}
        for device in devices
        if device.get("udid")
    }

    for udid, disabled_device in _disabled_devices.items():
        devices_by_udid[udid] = {
            **devices_by_udid.get(udid, disabled_device),
            **disabled_device,
            "disabled": True,
        }

    return devices_without_udid + list(devices_by_udid.values())


def _trim_changes(changes, now):
    while changes and now - changes[0] > CONNECTION_WINDOW_SECONDS:
        changes.popleft()


def _copy_device(device):
    return {**device}


def _connection_label(connected):
    return "connected" if connected else "disconnected"
