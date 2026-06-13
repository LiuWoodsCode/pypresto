import logging

from flask import Flask, abort, jsonify, render_template
from pymobiledevice3.usbmux import list_devices
from pymobiledevice3.lockdown import create_using_usbmux

from healthcheck import get_device, get_devices, start_background_healthcheck

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.route("/")
def index():
    return render_template("index.html", devices=get_devices())


@app.route("/api/devices")
def api_devices():
    return jsonify({"devices": get_devices()})


@app.route("/devices/<udid>")
def device_detail(udid):
    device = get_device(udid)
    if device is None:
        abort(404)

    return render_template("device.html", device=device)


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
