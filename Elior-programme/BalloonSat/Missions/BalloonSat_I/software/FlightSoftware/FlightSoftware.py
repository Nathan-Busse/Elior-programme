#!/usr/bin/env python3
"""
BalloonSat-I Flight Controller
- Orchestrates battery, comms, environmental, and location subsystems
- Captures images and audio with integrity sidecars (SHA-256)
- Optional cooling control via DS18B20 + GPIO MOSFET

I decided to repace the mocfet heater with an on-board fan instead.
The reason for this is because the electronics on board will generate there own hwat.
"""

import os
import sys
import json
import time
import math
import glob
import csv
import hashlib
import signal
import shutil
import socket
import threading
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# ----------------------------
# Configuration
# ----------------------------
CONFIG = {
    "mission_id": "BalloonSat-I",
    "image_interval_sec": 15,
    "audio_segment_sec": 60,
    "telemetry_interval_sec": 10,
    "log_level": "INFO",  # "DEBUG" for more noise
    # Camera
    "camera_w": 1920,
    "camera_h": 1080,
    "camera_quality": 90,
    "camera_rotate": 0,   # 0/90/180/270
    "camera_awb": "auto",
    # Audio
    "alsa_device": "default",
    "audio_rate": 16000,
    "audio_channels": 1,
    # cooling (optional)
    "cooling_enable": False,
    "cooling_pin": 18,      # BCM pin that drives the MOSFET gate
    "temp_target_c": 15.0,
    "temp_band_c": 6.0,    # hysteresis band (+/-)
    "min_safe_c": -10.0,   # force fan ON below
    "max_safe_c": 45.0,    # force fan OFF above
    # CSV rotation (sensors.json optional if your subsystems already persist)
    "csv_enable": True,
    "csv_bucket_minutes": 30,
}

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent  # .../software
STORAGE_DIR = BASE_DIR / "storage"
IMG_DIR = STORAGE_DIR / "images"
AUD_DIR = STORAGE_DIR / "audio"
LOG_DIR = BASE_DIR / "logs"  # optional local logs for this controller
for p in (IMG_DIR, AUD_DIR, LOG_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Utilities
# ----------------------------
def log(level, msg):
    if CONFIG["log_level"] == "DEBUG" or level != "DEBUG":
        ts = datetime.now(timezone.utc).isoformat()
        print(f"{ts} [{level}] {msg}", flush=True)

def ts_now_iso():
    return datetime.now(timezone.utc).isoformat()

def ts_for_name():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def write_sidecar_sha256(path: Path):
    try:
        digest = sha256_file(path)
        with open(str(path) + ".sha256", "w") as s:
            s.write(f"{digest}  {path.name}\n")
    except Exception as e:
        log("DEBUG", f"SHA256 sidecar failed for {path}: {e}")

def which(cmds):
    for c in cmds:
        p = shutil.which(c)
        if p:
            return p
    return None

def bytes_free(path: Path):
    s = shutil.disk_usage(path)
    return s.free

# ----------------------------
# Subsystem adapters
# ----------------------------
class BatteryAdapter:
    def __init__(self):
        self.impl = None
        try:
            from battery.battery import BatteryMonitor  # preferred
            self.impl = BatteryMonitor()
            log("INFO", "Battery: BatteryMonitor loaded")
        except Exception:
            try:
                import battery.battery as bat  # fallback import
                self.impl = getattr(bat, "BatteryMonitor", None)
                if callable(self.impl):
                    self.impl = self.impl()
                    log("INFO", "Battery: BatteryMonitor loaded (fallback)")
            except Exception as e:
                log("INFO", f"Battery unavailable ({e})")

    def start(self):
        if self.impl and hasattr(self.impl, "start"):
            try:
                self.impl.start()
            except Exception as e:
                log("DEBUG", f"Battery start error: {e}")

    def tick(self):
        # If your battery module expects explicit polling
        for name in ("read_and_log", "poll", "step"):
            if self.impl and hasattr(self.impl, name):
                try:
                    getattr(self.impl, name)()
                except Exception as e:
                    log("DEBUG", f"Battery tick error: {e}")
                break

    def status(self):
        # Try common status access patterns
        if not self.impl:
            return None
        for name in ("status", "get_status", "snapshot"):
            if hasattr(self.impl, name):
                try:
                    return getattr(self.impl, name)()
                except Exception:
                    pass
        return None

class CommsAdapter:
    def __init__(self):
        self.impl = None
        try:
            from communication.communication import Communication as Comms  # preferred
            self.impl = Comms()
            log("INFO", "Comms: Communication loaded")
        except Exception:
            try:
                from communication.communication import Comms  # alt name
                self.impl = Comms()
                log("INFO", "Comms: Comms loaded")
            except Exception as e:
                log("INFO", f"Comms unavailable ({e})")

    def start(self):
        if self.impl and hasattr(self.impl, "start"):
            try:
                self.impl.start()
            except Exception as e:
                log("DEBUG", f"Comms start error: {e}")

    def send(self, payload: dict):
        if not self.impl:
            return
        for name in ("send", "emit", "transmit", "publish"):
            if hasattr(self.impl, name):
                try:
                    getattr(self.impl, name)(payload)
                    return
                except Exception as e:
                    log("DEBUG", f"Comms send error: {e}")
        # no known method, ignore

class EnvAdapter:
    def __init__(self):
        self.impl = None
        try:
            from environmental.environment import EnvMonitor  # preferred
            self.impl = EnvMonitor()
            log("INFO", "Env: EnvMonitor loaded")
        except Exception:
            try:
                import environmental.environment as env
                cls = getattr(env, "EnvMonitor", None) or getattr(env, "Environment", None)
                if callable(cls):
                    self.impl = cls()
                    log("INFO", "Env: Environment loaded")
            except Exception as e:
                log("INFO", f"Env unavailable ({e})")

    def start(self):
        if self.impl and hasattr(self.impl, "start"):
            try:
                self.impl.start()
            except Exception as e:
                log("DEBUG", f"Env start error: {e}")

    def tick(self):
        for name in ("read_and_log", "poll", "step"):
            if self.impl and hasattr(self.impl, name):
                try:
                    getattr(self.impl, name)()
                except Exception as e:
                    log("DEBUG", f"Env tick error: {e}")
                break

    def status(self):
        if not self.impl:
            return None
        for name in ("status", "get_status", "snapshot", "latest"):
            if hasattr(self.impl, name):
                try:
                    return getattr(self.impl, name)()
                except Exception:
                    pass
        return None

class GPSAdapter:
    def __init__(self):
        self.impl = None
        try:
            from location.location import GPSModule  # preferred
            self.impl = GPSModule()
            log("INFO", "GPS: GPSModule loaded")
        except Exception:
            try:
                import location.location as loc
                cls = getattr(loc, "GPSModule", None) or getattr(loc, "Location", None)
                if callable(cls):
                    self.impl = cls()
                    log("INFO", "GPS: Location loaded")
            except Exception as e:
                log("INFO", f"GPS unavailable ({e})")

    def start(self):
        if self.impl and hasattr(self.impl, "start"):
            try:
                self.impl.start()
            except Exception as e:
                log("DEBUG", f"GPS start error: {e}")

    def tick(self):
        for name in ("read_and_log", "poll", "step"):
            if self.impl and hasattr(self.impl, name):
                try:
                    getattr(self.impl, name)()
                except Exception as e:
                    log("DEBUG", f"GPS tick error: {e}")
                break

    def status(self):
        if not self.impl:
            return None
        for name in ("status", "get_status", "snapshot", "position"):
            if hasattr(self.impl, name):
                try:
                    return getattr(self.impl, name)()
                except Exception:
                    pass
        return None

# ----------------------------
# Optional DS18B20 + cooling control
# ----------------------------
def find_ds18b20_path():
    try:
        candidates = glob.glob("/sys/bus/w1/devices/28-*/w1_slave")
        return candidates[0] if candidates else None
    except Exception:
        return None

def read_ds18b20_c(path):
    try:
        with open(path, "r") as f:
            lines = f.read().strip().splitlines()
        if len(lines) >= 2 and "YES" in lines[0]:
            tpos = lines[1].rfind("t=")
            if tpos != -1:
                return int(lines[1][tpos+2:]) / 1000.0
    except Exception:
        pass
    return None

class coolingController(threading.Thread):
    def __init__(self, stop_event):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.enabled = CONFIG["cooling_enable"]
        self.gpio_ok = False
        self.ds_path = find_ds18b20_path()
        self.cooling_on = False
        if self.enabled:
            try:
                import RPi.GPIO as GPIO
                self.GPIO = GPIO
                GPIO.setmode(GPIO.BCM)
                GPIO.setwarnings(False)
                GPIO.setup(CONFIG["cooling_pin"], GPIO.OUT)
                GPIO.output(CONFIG["cooling_pin"], GPIO.LOW)
                self.gpio_ok = True
            except Exception as e:
                log("INFO", f"cooling disabled (GPIO issue): {e}")
                self.enabled = False

    def run(self):
        if not (self.enabled and self.gpio_ok):
            return
        target = CONFIG["temp_target_c"]
        band = CONFIG["temp_band_c"]
        low = target - band / 2.0
        high = target + band / 2.0
        while not self.stop_event.is_set():
            t = read_ds18b20_c(self.ds_path) if self.ds_path else None
            if t is None:
                time.sleep(1)
                continue
            if t <= CONFIG["min_safe_c"]:
                demand = True
            elif t >= CONFIG["max_safe_c"]:
                demand = False
            else:
                if self.cooling_on:
                    demand = t < high
                else:
                    demand = t < low
            if demand != self.cooling_on:
                self.GPIO.output(CONFIG["cooling_pin"], self.GPIO.HIGH if demand else self.GPIO.LOW)
                self.cooling_on = demand
                log("INFO", f"cooling {'ON' if demand else 'OFF'} at {t:.1f} C")
            time.sleep(1)

    def cleanup(self):
        if self.gpio_ok:
            try:
                self.GPIO.output(CONFIG["cooling_pin"], self.GPIO.LOW)
                self.GPIO.cleanup(CONFIG["cooling_pin"])
            except Exception:
                pass

# ----------------------------
# Camera and audio workers
# ----------------------------
class CameraWorker(threading.Thread):
    def __init__(self, stop_event):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.cmd = which(["libcamera-still", "libcamera-jpeg", "raspistill"])
        if self.cmd:
            log("INFO", f"Camera command: {self.cmd}")
        else:
            log("INFO", "Camera disabled (no CLI found)")

    def run(self):
        if not self.cmd:
            return
        while not self.stop_event.is_set():
            if bytes_free(STORAGE_DIR) < 50 * 1024 * 1024:
                log("INFO", "Low disk space; skipping image")
            else:
                ts = ts_for_name()
                out = IMG_DIR / f"img_{ts}.jpg"
                try:
                    if self.cmd.startswith("libcamera"):
                        args = [
                            self.cmd, "-n",
                            "--width", str(CONFIG["camera_w"]),
                            "--height", str(CONFIG["camera_h"]),
                            "--quality", str(CONFIG["camera_quality"]),
                            "--awb", CONFIG["camera_awb"],
                            "--timeout", "1000",
                            "-o", str(out)
                        ]
                        if CONFIG["camera_rotate"] in (90, 180, 270):
                            args += ["--rotation", str(CONFIG["camera_rotate"])]
                    else:
                        args = [
                            self.cmd, "-n",
                            "-w", str(CONFIG["camera_w"]),
                            "-h", str(CONFIG["camera_h"]),
                            "-q", str(CONFIG["camera_quality"]),
                            "-t", "1000",
                            "-o", str(out)
                        ]
                        if CONFIG["camera_rotate"] in (90, 180, 270):
                            args += ["-rot", str(CONFIG["camera_rotate"])]
                    subprocess.run(args, check=False)
                    if out.exists():
                        write_sidecar_sha256(out)
                except Exception as e:
                    log("DEBUG", f"Camera error: {e}")
            self.stop_event.wait(CONFIG["image_interval_sec"])

class AudioWorker(threading.Thread):
    def __init__(self, stop_event):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.cmd = which(["arecord"])
        if self.cmd:
            log("INFO", "Audio: arecord available")
        else:
            log("INFO", "Audio disabled (arecord not found)")

    def run(self):
        if not self.cmd:
            return
        while not self.stop_event.is_set():
            if bytes_free(STORAGE_DIR) < 50 * 1024 * 1024:
                log("INFO", "Low disk space; skipping audio")
                self.stop_event.wait(5)
                continue
            ts = ts_for_name()
            out = AUD_DIR / f"aud_{ts}.wav"
            args = [
                self.cmd, "-q",
                "-D", CONFIG["alsa_device"],
                "-d", str(CONFIG["audio_segment_sec"]),
                "-f", "S16_LE",
                "-c", str(CONFIG["audio_channels"]),
                "-r", str(CONFIG["audio_rate"]),
                str(out)
            ]
            try:
                subprocess.run(args, check=False)
                if out.exists():
                    write_sidecar_sha256(out)
            except Exception as e:
                log("DEBUG", f"Audio error: {e}")

# ----------------------------
# Controller
# ----------------------------
class FlightController:
    def __init__(self):
        self.stop_event = threading.Event()
        self.battery = BatteryAdapter()
        self.comms = CommsAdapter()
        self.env = EnvAdapter()
        self.gps = GPSAdapter()
        self.camera = CameraWorker(self.stop_event)
        self.audio = AudioWorker(self.stop_event)
        self.cooling = coolingController(self.stop_event)
        self.csv_file = None
        self.csv_writer = None
        self.bucket_start = None
        self._write_manifest()

    def _write_manifest(self):
        manifest = {
            "mission_id": CONFIG["mission_id"],
            "started_utc": ts_now_iso(),
            "hostname": socket.gethostname(),
            "paths": {
                "images": str(IMG_DIR),
                "audio": str(AUD_DIR)
            },
            "config": CONFIG
        }
        path = STORAGE_DIR / f"manifest_{ts_for_name()}.json"
        try:
            with open(path, "w") as f:
                json.dump(manifest, f, indent=2)
            write_sidecar_sha256(path)
        except Exception as e:
            log("DEBUG", f"Manifest write error: {e}")

    def _rotate_csv_if_needed(self):
        if not CONFIG["csv_enable"]:
            return
        now = datetime.now(timezone.utc)
        need_new = self.csv_file is None
        if not need_new and (now - self.bucket_start).total_seconds() > CONFIG["csv_bucket_minutes"] * 60:
            need_new = True
        if need_new:
            if self.csv_file:
                try:
                    self.csv_file.flush()
                    os.fsync(self.csv_file.fileno())
                    name = Path(self.csv_file.name)
                    self.csv_file.close()
                    write_sidecar_sha256(name)
                except Exception:
                    pass
            name = STORAGE_DIR / f"sensors_{ts_for_name()}.csv"
            self.csv_file = open(name, "w", newline="")
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow([
                "timestamp_utc",
                "free_bytes",
                "battery_status",
                "env_status",
                "gps_status",
                "cooling_on"
            ])
            self.bucket_start = now

    def _log_csv_row(self, batt, env, gps, cooling_on):
        if not CONFIG["csv_enable"] or not self.csv_writer:
            return
        try:
            self.csv_writer.writerow([
                ts_now_iso(),
                bytes_free(STORAGE_DIR),
                json.dumps(batt) if batt is not None else "",
                json.dumps(env) if env is not None else "",
                json.dumps(gps) if gps is not None else "",
                int(bool(cooling_on))
            ])
            self.csv_file.flush()
            os.fsync(self.csv_file.fileno())
        except Exception as e:
            log("DEBUG", f"CSV write error: {e}")

    def start(self):
        log("INFO", "Starting subsystems")
        # Start background subsystems if they have threads
        self.battery.start()
        self.env.start()
        self.gps.start()
        self.comms.start()

        # Start workers
        self.camera.start()
        self.audio.start()
        if CONFIG["cooling_enable"]:
            self.cooling.start()

        # Foreground control loops
        self.control_thread = threading.Thread(target=self._control_loop, daemon=True)
        self.telemetry_thread = threading.Thread(target=self._telemetry_loop, daemon=True)
        self.control_thread.start()
        self.telemetry_thread.start()

    def _control_loop(self):
        last_sensor_tick = 0
        while not self.stop_event.is_set():
            self._rotate_csv_if_needed()
            # allow adapters that need polling to tick ~2 Hz
            now = time.time()
            if now - last_sensor_tick >= 0.5:
                self.battery.tick()
                self.env.tick()
                self.gps.tick()
                last_sensor_tick = now

            # periodic CSV row (every 2 s)
            time.sleep(2)
            batt = self.battery.status()
            env = self.env.status()
            gps = self.gps.status()
            cooling_on = getattr(self.cooling, "cooling_on", False)
            self._log_csv_row(batt, env, gps, cooling_on)

    def _telemetry_loop(self):
        while not self.stop_event.is_set():
            batt = self.battery.status()
            env = self.env.status()
            gps = self.gps.status()
            payload = {
                "timestamp": ts_now_iso(),
                "mission_id": CONFIG["mission_id"],
                "battery": batt,
                "environment": env,
                "gps": gps,
                "free_bytes": bytes_free(STORAGE_DIR),
                "cooling_on": getattr(self.cooling, "cooling_on", False)
            }
            try:
                self.comms.send(payload)
            except Exception as e:
                log("DEBUG", f"Telemetry send error: {e}")
            self.stop_event.wait(CONFIG["telemetry_interval_sec"])

    def wait(self):
        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            log("INFO", "KeyboardInterrupt received")
            self.stop()

    def stop(self):
        log("INFO", "Stopping controller")
        self.stop_event.set()
        try:
            self.control_thread.join(timeout=3)
            self.telemetry_thread.join(timeout=3)
        except Exception:
            pass
        if CONFIG["cooling_enable"]:
            self.cooling.cleanup()
        # Close CSV cleanly
        if self.csv_file:
            try:
                self.csv_file.flush()
                os.fsync(self.csv_file.fileno())
                name = Path(self.csv_file.name)
                self.csv_file.close()
                write_sidecar_sha256(name)
            except Exception:
                pass

# ----------------------------
# Entrypoint
# ----------------------------
def main():
    ctrl = FlightController()

    def handle_signal(signum, frame):
        log("INFO", f"Signal {signum} received")
        ctrl.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    ctrl.start()
    ctrl.wait()

if __name__ == "__main__":
    main()
