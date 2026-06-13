import asyncio

from flask import Flask, render_template_string
from pymobiledevice3.usbmux import list_devices
from pymobiledevice3.lockdown import create_using_usbmux

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>iDevice Viewer</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 40px auto;
        }

        .device {
            border: 1px solid #ccc;
            padding: 12px;
            margin-bottom: 10px;
            border-radius: 8px;
        }
    </style>
</head>
<body>
    <h1>Connected iDevices</h1>

    {% if devices %}
        {% for device in devices %}
        <div class="device">
            <b>Name:</b> {{ device.name }}<br>
            <b>Model:</b> {{ device.model }}<br>
            <b>Product:</b> {{ device.product }}<br>
            <b>iOS Version:</b> {{ device.version }}<br>
            <b>UDID:</b> {{ device.udid }}
        </div>
        {% endfor %}
    {% else %}
        <p>No devices connected.</p>
    {% endif %}
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML, devices=asyncio.run(get_devices_info()))


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
    app.run(host="0.0.0.0", port=5000, debug=True)
