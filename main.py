import asyncio
import json
import logging

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, stream_with_context, url_for
from usb.core import NoBackendError, USBError, find as find_usb_devices

from pymobiledevice3.irecv import APPLE_VENDOR_ID, Mode
from pymobiledevice3.irecv_devices import IRECV_DEVICES
from pymobiledevice3.usbmux import list_devices
from pymobiledevice3.lockdown import create_using_usbmux
from pymobiledevice3.services.diagnostics import DiagnosticsService
from pymobiledevice3.services.syslog import SyslogService

from healthcheck import get_device, get_devices, start_background_healthcheck

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

DEVICE_ACTIONS = {
    "restart": "Restart",
    "shutdown": "Shut down",
    "recovery": "Enter recovery mode",
}

DEVICE_POWER_ACTIONS = {
    "restart": "Restart",
    "shutdown": "Shut down",
    "sleep": "Sleep",
}


@app.context_processor
def inject_device_actions():
    return {
        "device_actions": DEVICE_ACTIONS,
        "device_power_actions": DEVICE_POWER_ACTIONS,
    }


@app.route("/")
def index():
    return render_template(
        "index.html",
        devices=get_devices(),
        action_status=request.args.get("status"),
        action_error=request.args.get("error"),
    )


@app.route("/api/devices")
def api_devices():
    return jsonify({"devices": get_devices()})


@app.get("/api/devices/<udid>/syslog")
def stream_device_syslog(udid):
    device = get_device(udid)
    if device is None:
        abort(404)

    if not _is_actionable_device(device):
        return jsonify({"error": "This device is not available."}), 400

    return Response(
        stream_with_context(_stream_syslog_events(udid)),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/devices/<udid>")
def device_detail(udid):
    device = get_device(udid)
    if device is None:
        abort(404)

    if device.get("template") in ("device_rcm.html", "device_dfu.html"):
        return render_template(
            device["template"],
            device=device,
            action_status=request.args.get("status"),
            action_error=request.args.get("error"),
        )

    battery = None
    battery_error = None
    if _is_actionable_device(device):
        battery_result = asyncio.run(_get_battery_for_device(device))
        if battery_result["ok"]:
            battery = battery_result["battery"]
        else:
            battery_error = battery_result["error"]

    return render_template(
        "device.html",
        device=device,
        battery=battery,
        battery_error=battery_error,
        action_status=request.args.get("status"),
        action_error=request.args.get("error"),
    )


@app.post("/api/devices/actions/<action>")
def run_device_action(action):
    if action not in DEVICE_ACTIONS:
        abort(404)

    devices = [
        device for device in get_devices()
        if _is_actionable_device(device)
    ]
    if not devices:
        return redirect(url_for("index", error="No connected devices available."))

    results = asyncio.run(_run_action_for_devices(action, devices))
    failures = [result for result in results if not result["ok"]]

    if failures:
        summary = f"{len(failures)} of {len(results)} devices failed."
        return redirect(url_for("index", error=summary))

    summary = f"{DEVICE_ACTIONS[action]} sent to {len(results)} device(s)."
    return redirect(url_for("index", status=summary))


@app.post("/api/devices/<udid>/actions/<action>")
def run_single_device_action(udid, action):
    if action not in DEVICE_POWER_ACTIONS:
        abort(404)

    device = get_device(udid)
    if device is None:
        abort(404)

    if not _is_actionable_device(device):
        return redirect(url_for(
            "device_detail",
            udid=udid,
            error="This device is not available.",
        ))

    result = asyncio.run(_run_action_for_device(action, device))
    if not result["ok"]:
        return redirect(url_for(
            "device_detail",
            udid=udid,
            error=f"{DEVICE_POWER_ACTIONS[action]} failed.",
        ))

    return redirect(url_for(
        "device_detail",
        udid=udid,
        status=f"{DEVICE_POWER_ACTIONS[action]} sent.",
    ))


async def _run_action_for_devices(action, devices):
    return await asyncio.gather(*(
        _run_action_for_device(action, device) for device in devices
    ))


async def _run_action_for_device(action, device):
    lockdown = None
    udid = device["udid"]

    try:
        lockdown = await create_using_usbmux(udid)

        if action == "restart":
            await DiagnosticsService(lockdown=lockdown).restart()
        elif action == "shutdown":
            await DiagnosticsService(lockdown=lockdown).shutdown()
        elif action == "sleep":
            await DiagnosticsService(lockdown=lockdown).sleep()
        elif action == "recovery":
            await lockdown.enter_recovery()

        return {"udid": udid, "ok": True}
    except Exception as e:
        app.logger.exception("Failed to run %s on %s", action, udid)
        return {"udid": udid, "ok": False, "error": str(e)}
    finally:
        if lockdown is not None:
            await lockdown.close()


async def _get_battery_for_device(device):
    lockdown = None
    udid = device["udid"]

    try:
        lockdown = await create_using_usbmux(udid)
        battery = await DiagnosticsService(lockdown=lockdown).get_battery()
        return {"udid": udid, "ok": True, "battery": battery}
    except Exception as e:
        app.logger.exception("Failed to fetch battery info for %s", udid)
        return {"udid": udid, "ok": False, "error": str(e)}
    finally:
        if lockdown is not None:
            await lockdown.close()


async def _watch_syslog_lines(udid):
    lockdown = None
    syslog = None

    try:
        lockdown = await create_using_usbmux(udid)
        syslog = SyslogService(lockdown)

        async for line in syslog.watch():
            yield line
    finally:
        if syslog is not None:
            await syslog.close()
        if lockdown is not None:
            await lockdown.close()


def _stream_syslog_events(udid):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    watcher = _watch_syslog_lines(udid)

    try:
        yield _sse_event("status", "Connecting to syslog...")

        while True:
            try:
                line = loop.run_until_complete(watcher.__anext__())
            except StopAsyncIteration:
                yield _sse_event("status", "Syslog stream ended.")
                break
            except Exception as e:
                app.logger.exception("Failed to stream syslog for %s", udid)
                yield _sse_event("stream-error", str(e))
                break

            yield _sse_event("log", line)
    finally:
        try:
            loop.run_until_complete(watcher.aclose())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def _sse_event(event, data):
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def _is_actionable_device(device):
    return (
        device.get("udid")
        and not device.get("disabled")
        and device.get("state", "normal") == "normal"
    )


async def get_devices_info():
    devices_info = []

    try:
        for device in await list_devices():
            lockdown = None
            try:
                lockdown = await create_using_usbmux(device.serial)

                values = lockdown.all_values

                devices_info.append({
                    "name": values.get("DeviceName", "Unknown"),
                    "model": values.get("ModelNumber", "Unknown"),
                    "product": values.get("ProductType", "Unknown"),
                    "version": values.get("ProductVersion", "Unknown"),
                    "udid": device.serial,
                    "state": "normal",
                    "state_label": "Normal",
                })

            except Exception as e:
                devices_info.append({
                    "name": "Error",
                    "model": str(e),
                    "product": "",
                    "version": "",
                    "udid": device.serial,
                    "state": "normal",
                    "state_label": "Normal",
                })
            finally:
                if lockdown is not None:
                    await lockdown.close()

    except Exception as e:
        devices_info.append({
            "name": "Error",
            "model": str(e),
            "product": "",
            "version": "",
            "udid": "",
            "state": "normal",
            "state_label": "Normal",
        })

    devices_info.extend(get_recovery_dfu_devices_info())
    return devices_info


def get_recovery_dfu_devices_info():
    devices_info = []

    try:
        usb_devices = find_usb_devices(find_all=True, idVendor=APPLE_VENDOR_ID)
    except (NoBackendError, USBError):
        app.logger.debug("Recovery/DFU discovery skipped because no USB backend is available")
        return devices_info

    for usb_device in usb_devices:
        mode = Mode.get_mode_from_value(usb_device.idProduct)
        if mode is None or mode == Mode.WTF_MODE:
            continue

        try:
            device_info = _parse_irecv_serial(usb_device.serial_number or "")
            state = "recovery" if mode.is_recovery else "dfu"
            state_label = "Recovery" if state == "recovery" else "DFU"
            ecid = device_info.get("ECID", "")
            chip_id = _hex_to_int(device_info.get("CPID"))
            board_id = _hex_to_int(device_info.get("BDID"))
            known_device = _find_irecv_device(board_id, chip_id)
            fallback_id = f"{usb_device.bus}-{usb_device.address}-{usb_device.idProduct:04x}"
            identifier = ecid.lower() if ecid else fallback_id

            devices_info.append({
                "name": known_device.display_name if known_device else f"iDevice in {state_label}",
                "model": known_device.hardware_model if known_device else device_info.get("MODEL", "Unknown"),
                "product": known_device.product_type if known_device else "Unknown",
                "version": device_info.get("SRTG", state_label),
                "udid": f"irecv-{identifier}",
                "ecid": ecid or "Unknown",
                "chip_id": _format_hex(chip_id),
                "board_id": _format_hex(board_id),
                "serial_number": device_info.get("SRNM", "Unknown"),
                "state": state,
                "state_label": state_label,
                "mode": mode.name,
                "template": "device_rcm.html" if state == "recovery" else "device_dfu.html",
                "actionable": False,
            })
        except (USBError, ValueError) as e:
            devices_info.append({
                "name": "iDevice in Recovery/DFU",
                "model": str(e),
                "product": "Unknown",
                "version": "Unknown",
                "udid": f"irecv-error-{usb_device.bus}-{usb_device.address}",
                "ecid": "Unknown",
                "chip_id": "Unknown",
                "board_id": "Unknown",
                "serial_number": "Unknown",
                "state": "recovery_dfu",
                "state_label": "Recovery/DFU",
                "mode": "Unknown",
                "template": "device_rcm.html",
                "actionable": False,
            })

    return devices_info


def _parse_irecv_serial(serial):
    device_info = {}
    for component in serial.split():
        if ":" not in component:
            continue

        key, value = component.split(":", 1)
        if key in ("SRNM", "SRTG") and value.startswith("[") and value.endswith("]"):
            value = value[1:-1]
        device_info[key] = value
    return device_info


def _find_irecv_device(board_id, chip_id):
    if board_id is None or chip_id is None:
        return None

    for device in IRECV_DEVICES:
        if device.board_id == board_id and device.chip_id == chip_id:
            return device
    return None


def _hex_to_int(value):
    if value is None:
        return None
    return int(value, 16)


def _format_hex(value):
    if value is None:
        return "Unknown"
    return f"0x{value:x}"


if __name__ == "__main__":
    start_background_healthcheck(get_devices_info)
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
