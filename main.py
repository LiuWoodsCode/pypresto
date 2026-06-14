import asyncio
import logging

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from pymobiledevice3.usbmux import list_devices
from pymobiledevice3.lockdown import create_using_usbmux
from pymobiledevice3.services.diagnostics import DiagnosticsService

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


@app.route("/devices/<udid>")
def device_detail(udid):
    device = get_device(udid)
    if device is None:
        abort(404)

    battery = None
    battery_error = None
    if device.get("udid") and not device.get("disabled"):
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
        if device.get("udid") and not device.get("disabled")
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

    if not device.get("udid") or device.get("disabled"):
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
                })

            except Exception as e:
                devices_info.append({
                    "name": "Error",
                    "model": str(e),
                    "product": "",
                    "version": "",
                    "udid": device.serial,
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
        })

    return devices_info

if __name__ == "__main__":
    start_background_healthcheck(get_devices_info)
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
