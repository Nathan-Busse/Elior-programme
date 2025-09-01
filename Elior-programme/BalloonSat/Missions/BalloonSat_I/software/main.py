#!/usr/bin/env python3
"""
BalloonSat-1 Flight Software
Integrates battery, comms, environmental, and location subsystems
with camera and audio capture for October 2025 stratospheric test.
"""

import os
import time
import threading
from datetime import datetime

# Import your existing subsystems
from battery.battery import BatteryMonitor
from communication.communication import Communication
from environmental.environment import EnvMonitor
from location.location import GPSModule

# Config
DATA_ROOT = os.path.join(os.path.dirname(__file__), "..", "storage")
IMG_DIR = os.path.join(DATA_ROOT, "images")
AUD_DIR = os.path.join(DATA_ROOT, "audio")
IMAGE_INTERVAL = 15       # seconds
AUDIO_SEGMENT = 60        # seconds
SENSOR_INTERVAL = 2       # seconds
TELEMETRY_INTERVAL = 10   # seconds

# Ensure storage dirs exist
os.makedirs(IMG_DIR, exist_ok=True)
os.makedirs(AUD_DIR, exist_ok=True)

# Camera capture (simple CLI wrapper)
def capture_image():
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(IMG_DIR, f"img_{ts}.jpg")
    cmd = f"libcamera-still -n -o {path} --width 1920 --height 1080 --quality 90 --timeout 1000"
    os.system(cmd)

# Audio capture (simple CLI wrapper)
def capture_audio():
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(AUD_DIR, f"aud_{ts}.wav")
    cmd = f"arecord -q -D default -d {AUDIO_SEGMENT} -f S16_LE -c 1 -r 16000 {path}"
    os.system(cmd)

def image_loop(stop_event):
    while not stop_event.is_set():
        capture_image()
        stop_event.wait(IMAGE_INTERVAL)

def audio_loop(stop_event):
    while not stop_event.is_set():
        capture_audio()

def sensor_loop(stop_event, battery, env, gps):
    while not stop_event.is_set():
        battery.read_and_log()
        env.read_and_log()
        gps.read_and_log()
        stop_event.wait(SENSOR_INTERVAL)

def telemetry_loop(stop_event, comms, battery, env, gps):
    while not stop_event.is_set():
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "battery": battery.status(),
            "environment": env.status(),
            "gps": gps.status()
        }
        comms.send(payload)
        stop_event.wait(TELEMETRY_INTERVAL)

def main():
    print("[INFO] Initialising subsystems...")
    battery = BatteryMonitor()
    comms = Communication()
    env = EnvMonitor()
    gps = GPSModule()

    stop_event = threading.Event()

    threads = [
        threading.Thread(target=image_loop, args=(stop_event,), daemon=True),
        threading.Thread(target=audio_loop, args=(stop_event,), daemon=True),
        threading.Thread(target=sensor_loop, args=(stop_event, battery, env, gps), daemon=True),
        threading.Thread(target=telemetry_loop, args=(stop_event, comms, battery, env, gps), daemon=True)
    ]

    print("[INFO] Starting mission loops...")
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[INFO] Stopping mission...")
        stop_event.set()
        for t in threads:
            t.join()

if __name__ == "__main__":
    main()
