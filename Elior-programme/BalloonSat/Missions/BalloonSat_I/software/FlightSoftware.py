# FlightSoftware
#
#
# Created by: Nathan Graham Busse
#
# Script created on: 30 August 2025

#!/usr/bin/env python3
import os
import sys
import time
import json
import csv
import math
import glob
import queue
import shutil
import signal
import hashlib
import socket
import threading
import subprocess
from datetime import datetime, timezone

# import child scripts
from battery.battery import *
from communication.communication import *
from environmental.environment import *
from location.location import *

# import child util scrips
from battery.util.battery_backup import *
from battery.util.battery_truncate import *
from environmental.util.environment_backup import *
from environmental.util.environment_truncate import *
from location.util.location_backup import *
from location.util.location_export_to_kml import *
from location.util.location_truncate import *

# import lib child scripts
from battery.lib.battery_common import *
from battery.lib.battery_log import *
from battery.lib.battery_unix_socket import *
from communication.lib.communication_common import *
from communication.lib.communication_log import *
from environmental.lib.environment_common import *
from environmental.lib.environment_log import *
from environmental.lib.environment_pi import *
from location.lib.location_common import *
from location.lib.location_gps import *
from location.lib.location_log import *

# Optional sensor libs
try:
    import smbus2
    import bme280  # pip install bme280 smbus2
    _HAS_BME280 = True
except Exception:
    _HAS_BME280 = False

# GPIO (heater control)
try:
    import RPi.GPIO as GPIO
    _HAS_GPIO = True
except Exception:
    _HAS_GPIO = False

# GPS over serial
try:
    import serial
    _HAS_SERIAL = True
except Exception:
    _HAS_SERIAL = False

# ----------------------------
# Configuration
# ----------------------------
CONFIG = {
    "mission_id": "BalloonSat-1",
    "data_root": "/home/payload/storage",
    "image_interval_sec": 15,           # Still image cadence in seconds
    "audio_segment_sec": 60,            # WAV segment length in seconds
    "sensor_interval_sec": 2,           # Sensor sample cadence in seconds
    "gps_port": "/dev/serial0",
    "gps_baud": 9600,
    "heater_pin": 18,                   # BCM numbering
    "heater_enable": True,
    "temp_target_c": 15.0,
    "temp_band_c": 6.0,                 # +/- band for hysteresis
    "min_safe_c": -10.0,                # Heater forced ON below this
    "max_safe_c": 45.0,                 # Heater forced OFF above this
    "file_time_bucket_min": 30,         # Start new CSV every N minutes
    "max_images": 0,                    # 0 = unlimited
    "camera_w": 1920,
    "camera_h": 1080,
    "camera_quality": 90,
    "camera_awb": "auto",
    "camera_iso": 100,
    "camera_rotate": 0,                 # 0/90/180/270
    "alsa_device": "default",
    "timezone": "UTC",
    "log_level": "INFO"                 # INFO/DEBUG
}

# ----------------------------
# Utilities
# ----------------------------
def log(level, msg):
    if CONFIG["log_level"] == "DEBUG" or level != "DEBUG":
        ts = datetime.now(timezone.utc).isoformat()
        print(f"{ts} [{level}] {msg}", flush=True)

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def ts_for_filename():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def which(cmds):
    for c in cmds:
        if shutil.which(c):
            return c
    return None

def bytes_free(path):
    s = shutil.disk_usage(path)
    return s.free

# ----------------------------
# DS18B20 (1-Wire) internal temperature
# ----------------------------
def find_ds18b20_path():
    base = "/sys/bus/w1/devices"
    try:
        candidates = glob.glob(os.path.join(base, "28-*", "w1_slave"))
        return candidates[0] if candidates else None
    except Exception:
        return None

def read_ds18b20_c(path):
    try:
        with open(path, "r") as f:
            lines = f.read().strip().splitlines()
        if len(lines) >= 2 and "YES" in lines[0]:
            idx = lines[1].find("t=")
            if idx != -1:
                t_milli = int(lines[1][idx+2:].strip())
                return t_milli / 1000.0
    except Exception:
        pass
    return None

# ----------------------------
# BME280/BMP280 environmental reading (optional)
# ----------------------------
class EnvSensor:
    def __init__(self):
        self.available = False
        self.address = None
        self.bus = None
        self.cal = None
        if _HAS_BME280:
            try:
                self.bus = smbus2.SMBus(1)
                for addr in (0x76, 0x77):
                    try:
                        self.cal = bme280.load_calibration_params(self.bus, addr)
                        self.address = addr
                        self.available = True
                        log("INFO", f"BME280 detected at 0x{addr:02X}")
                        break
                    except Exception:
                        continue
            except Exception as e:
                log("DEBUG", f"BME280 init failed: {e}")

    def read(self):
        if not self.available:
            return None
        try:
            sample = bme280.sample(self.bus, self.address, self.cal)
            return {
                "env_temp_c": round(sample.temperature, 2),
                "pressure_hpa": round(sample.pressure, 2),
                "humidity_pct": round(sample.humidity, 2) if hasattr(sample, "humidity") else None
            }
        except Exception as e:
            log("DEBUG", f"BME280 read error: {e}")
            return None

# ----------------------------
# GPS NMEA parsing (lightweight)
# ----------------------------
def knots_to_mps(knots):
    return knots * 0.514444

def nmea_coord_to_deg(value, hemi):
    # value like ddmm.mmmm (lat) or dddmm.mmmm (lon)
    if not value or value == "":
        return None
    try:
        if "." not in value:
            return None
        head, tail = value.split(".")
        deg_len = 2 if len(head) in (4, 5) else 3
        deg = float(head[:-2])
        minutes = float(head[-2:] + "." + tail)
        out = deg + minutes / 60.0
        if hemi in ("S", "W"):
            out = -out
        return out
    except Exception:
        return None

def parse_rmc(fields):
    # $GxRMC,UTC,status,lat,N|S,lon,E|W,speed,course,date,...
    try:
        status = fields[2]
        if status != "A":
            return None
        lat = nmea_coord_to_deg(fields[3], fields[4])
        lon = nmea_coord_to_deg(fields[5], fields[6])
        speed_kn = float(fields[7]) if fields[7] else 0.0
        course = float(fields[8]) if fields[8] else None
        return {"lat": lat, "lon": lon, "speed_kn": speed_kn, "course_deg": course}
    except Exception:
        return None

def parse_gga(fields):
    # $GxGGA,UTC,lat,N|S,lon,E|W,fix,nsat,hdop,alt,M,...
    try:
        fix = int(fields[6])
        if fix == 0:
            return None
        lat = nmea_coord_to_deg(fields[2], fields[3])
        lon = nmea_coord_to_deg(fields[4], fields[5])
        alt = float(fields[9]) if fields[9] else None
        nsat = int(fields[7]) if fields[7] else None
        hdop = float(fields[8]) if fields[8] else None
        return {"lat": lat, "lon": lon, "alt_m": alt, "nsat": nsat, "hdop": hdop}
    except Exception:
        return None

# ----------------------------
# Camera worker (libcamera/raspistill)
# ----------------------------
class CameraWorker(threading.Thread):
    def __init__(self, root_dir, interval_sec, stop_event):
        super().__init__(daemon=True)
        self.root_dir = root_dir
        self.interval = interval_sec
        self.stop_event = stop_event
        self.cmd = self._detect_cmd()
        self.count = 0
        ensure_dir(self.root_dir)

    def _detect_cmd(self):
        # Prefer libcamera, fallback to raspistill
        cmd = which(["libcamera-still", "libcamera-jpeg", "raspistill"])
        if cmd is None:
            log("INFO", "No camera CLI found; camera disabled")
        else:
            log("INFO", f"Camera command: {cmd}")
        return cmd

    def run(self):
        if not self.cmd or self.interval <= 0:
            return
        while not self.stop_event.is_set():
            try:
                timestamp = ts()
                ts = ts_for_filename()
                jpg_save_location = "storage/supercam/image"
                
                path = os.path.join(self.root_dir, f"img_{ts}.jpg")
                if self.cmd.startswith("libcamera"):
                    args = [
                        self.cmd, "-n",
                        "--width", str(CONFIG["camera_w"]),
                        "--height", str(CONFIG["camera_h"]),
                        "--quality", str(CONFIG["camera_quality"]),
                        "--awb", CONFIG["camera_awb"],
                        "--timeout", "1000",
                        "-o", path
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
                        "-o", path
                    ]
                    if CONFIG["camera_rotate"] in (90, 180, 270):
                        args += ["-rot", str(CONFIG["camera_rotate"])]
                    if CONFIG["camera_iso"]:
                        args += ["-ISO", str(CONFIG["camera_iso"])]
                subprocess.run(args, check=False)
                if os.path.exists(path):
                    # Hash sidecar
                    h = sha256_file(path)
                    with open(path + ".sha256", "w") as f:
                        f.write(h + "  " + os.path.basename(path) + "\n")
                    self.count += 1
                    if CONFIG["max_images"] and self.count >= CONFIG["max_images"]:
                        log("INFO", "Reached max_images; stopping camera")
                        break
                else:
                    log("DEBUG", "Camera image not created")
            except Exception as e:
                log("DEBUG", f"Camera error: {e}")
            self.stop_event.wait(self.interval)

# ----------------------------
# Audio worker (arecord WAV segments)
# ----------------------------
class AudioWorker(threading.Thread):
    def __init__(self, root_dir, segment_sec, stop_event):
        super().__init__(daemon=True)
        self.root_dir = root_dir
        self.segment = segment_sec
        self.stop_event = stop_event
        ensure_dir(self.root_dir)
        self.cmd = which(["arecord"])
        if not self.cmd:
            log("INFO", "arecord not found; audio disabled")

    def run(self):
        if not self.cmd or self.segment <= 0:
            return
        while not self.stop_event.is_set():
            try:
                ts = ts_for_filename()
                wav_save_location = "storage/supercam/audio"
                path = os.path.join(self.root_dir, f"aud_{ts}.wav")
                args = [
                    self.cmd,
                    "-q",
                    "-D", CONFIG["alsa_device"],
                    "-d", str(self.segment),
                    "-f", "S16_LE",
                    "-c", "1",
                    "-r", "16000",
                    path
                ]
                subprocess.run(args, check=False)
                if os.path.exists(path):
                    h = sha256_file(path)
                    with open(path + ".sha256", "w") as f:
                        f.write(h + "  " + os.path.basename(path) + "\n")
            except Exception as e:
                log("DEBUG", f"Audio error: {e}")

# ----------------------------
# Sensor + heater + GPS
# ----------------------------
class FlightState:
    def __init__(self):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.ds18b20_path = find_ds18b20_path()
        self.env = EnvSensor()
        self.gps = {"lat": None, "lon": None, "alt_m": None, "speed_kn": None, "nsat": None, "hdop": None}
        self.internal_c = None
        self.heater_on = False

    def uptime(self):
        return time.time() - self.start_time

class HeaterController(threading.Thread):
    def __init__(self, state, stop_event):
        super().__init__(daemon=True)
        self.state = state
        self.stop_event = stop_event
        self.pin = CONFIG["heater_pin"]
        self.enabled = CONFIG["heater_enable"] and _HAS_GPIO
        if self.enabled:
            GPIO.setmode(GPIO.BCM)
            GPIO.setwarnings(False)
            GPIO.setup(self.pin, GPIO.OUT)
            GPIO.output(self.pin, GPIO.LOW)
        else:
            log("INFO", "Heater disabled (GPIO missing or config)")

    def run(self):
        if not self.enabled:
            return
        target = CONFIG["temp_target_c"]
        band = CONFIG["temp_band_c"]
        low = target - band / 2.0
        high = target + band / 2.0
        while not self.stop_event.is_set():
            with self.state.lock:
                t = self.state.internal_c
                curr = self.state.heater_on
            if t is None:
                # Fail-safe: keep previous state, try again soon
                time.sleep(1)
                continue
            if t <= CONFIG["min_safe_c"]:
                demand = True
            elif t >= CONFIG["max_safe_c"]:
                demand = False
            else:
                # Hysteresis
                if curr:
                    demand = t < high
                else:
                    demand = t < low
            if demand != curr:
                GPIO.output(self.pin, GPIO.HIGH if demand else GPIO.LOW)
                with self.state.lock:
                    self.state.heater_on = demand
                log("INFO", f"Heater {'ON' if demand else 'OFF'} at {t:.1f} C")
            time.sleep(1)

    def cleanup(self):
        if self.enabled:
            GPIO.output(self.pin, GPIO.LOW)
            GPIO.cleanup(self.pin)

class GPSWorker(threading.Thread):
    def __init__(self, state, stop_event):
        super().__init__(daemon=True)
        self.state = state
        self.stop_event = stop_event
        self.port = CONFIG["gps_port"]
        self.baud = CONFIG["gps_baud"]
        self.ok = _HAS_SERIAL

    def run(self):
        if not self.ok:
            log("INFO", "pyserial not available; GPS disabled")
            return
        try:
            with serial.Serial(self.port, self.baud, timeout=1) as ser:
                log("INFO", f"GPS opened on {self.port} @ {self.baud}")
                while not self.stop_event.is_set():
                    line = ser.readline().decode(errors="ignore").strip()
                    if not line.startswith("$"):
                        continue
                    try:
                        base, *_ = line.split("*")
                        fields = base.split(",")
                        talker = fields[0][3:]  # e.g., RMC/GGA
                        if talker == "RMC":
                            r = parse_rmc(fields)
                            if r:
                                with self.state.lock:
                                    self.state.gps.update(r)
                        elif talker == "GGA":
                            g = parse_gga(fields)
                            if g:
                                with self.state.lock:
                                    # prefer altitude + quality from GGA
                                    self.state.gps.update({k: g[k] for k in ("alt_m", "nsat", "hdop")})
                                    # only update lat/lon if present
                                    if g.get("lat") and g.get("lon"):
                                        self.state.gps["lat"] = g["lat"]
                                        self.state.gps["lon"] = g["lon"]
                    except Exception:
                        continue
        except Exception as e:
            log("INFO", f"GPS disabled: {e}")

class SensorLogger(threading.Thread):
    def __init__(self, state, root_dir, interval_sec, stop_event):
        super().__init__(daemon=True)
        self.state = state
        self.root_dir = root_dir
        self.interval = interval_sec
        self.stop_event = stop_event
        ensure_dir(self.root_dir)
        self.current_csv = None
        self.current_writer = None
        self.bucket_start = None

    def rotate_csv_if_needed(self):
        now = datetime.now(timezone.utc)
        if self.current_csv is None or (now - self.bucket_start).total_seconds() > CONFIG["file_time_bucket_min"] * 60:
            if self.current_csv:
                self.current_csv.flush()
                path = self.current_csv.name
                self.current_csv.close()
                h = sha256_file(path)
                with open(path + ".sha256", "w") as f:
                    f.write(h + "  " + os.path.basename(path) + "\n")
            ts = ts_for_filename()
            path = os.path.join(self.root_dir, f"sensors_{ts}.csv")
            self.current_csv = open(path, "w", newline="")
            self.current_writer = csv.writer(self.current_csv)
            self.current_writer.writerow([
                "timestamp_utc",
                "uptime_s",
                "env_temp_c","pressure_hpa","humidity_pct",
                "internal_temp_c",
                "gps_lat","gps_lon","gps_alt_m","gps_speed_kn","gps_nsat","gps_hdop",
                "heater_on",
                "free_bytes"
            ])
            self.bucket_start = now

    def run(self):
        while not self.stop_event.is_set():
            self.rotate_csv_if_needed()
            # Read sensors
            env = self.state.env.read() if self.state.env else None
            internal_c = None
            if self.state.ds18b20_path:
                internal_c = read_ds18b20_c(self.state.ds18b20_path)
            with self.state.lock:
                if internal_c is not None:
                    self.state.internal_c = internal_c
                gps = dict(self.state.gps)
                heater_on = self.state.heater_on
            # Log row
            row = [
                iso_now(),
                round(self.state.uptime(), 2),
                env["env_temp_c"] if env else None,
                env["pressure_hpa"] if env else None,
                env["humidity_pct"] if env else None,
                round(internal_c, 2) if internal_c is not None else None,
                gps.get("lat"), gps.get("lon"), gps.get("alt_m"), gps.get("speed_kn"), gps.get("nsat"), gps.get("hdop"),
                int(heater_on),
                bytes_free(self.root_dir)
            ]
            try:
                self.current_writer.writerow(row)
                self.current_csv.flush()
                os.fsync(self.current_csv.fileno())
            except Exception as e:
                log("DEBUG", f"CSV write error: {e}")
            time.sleep(self.interval)
        # Close out
        if self.current_csv:
            path = self.current_csv.name
            self.current_csv.flush()
            os.fsync(self.current_csv.fileno())
            self.current_csv.close()
            h = sha256_file(path)
            with open(path + ".sha256", "w") as f:
                f.write(h + "  " + os.path.basename(path) + "\n")

# ----------------------------
# Flight app
# ----------------------------
class FlightApp:
    def __init__(self):
        self.stop_event = threading.Event()
        self.state = FlightState()
        root = CONFIG["data_root"]
        self.dir_images = os.path.join(root, "images")
        self.dir_audio = os.path.join(root, "audio")
        self.dir_sensors = os.path.join(root, "sensors")
        ensure_dir(root)
        ensure_dir(self.dir_images)
        ensure_dir(self.dir_audio)
        ensure_dir(self.dir_sensors)
        self.heater = HeaterController(self.state, self.stop_event)
        self.gps = GPSWorker(self.state, self.stop_event)
        self.camera = CameraWorker(self.dir_images, CONFIG["image_interval_sec"], self.stop_event)
        self.audio = AudioWorker(self.dir_audio, CONFIG["audio_segment_sec"], self.stop_event)
        self.logger = SensorLogger(self.state, self.dir_sensors, CONFIG["sensor_interval_sec"], self.stop_event)
        self._write_manifest()

    def _write_manifest(self):
        manifest = {
            "mission_id": CONFIG["mission_id"],
            "started_utc": iso_now(),
            "hostname": socket.gethostname(),
            "config": CONFIG,
            "sensors": {
                "bme280": self.state.env.available,
                "ds18b20": bool(self.state.ds18b20_path)
            }
        }
        path = os.path.join(CONFIG["data_root"], f"manifest_{ts_for_filename()}.json")
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)
        with open(path + ".sha256", "w") as s:
            s.write(sha256_file(path) + "  " + os.path.basename(path) + "\n")

    def start(self):
        log("INFO", "Flight software starting")
        self.gps.start()
        self.heater.start()
        self.camera.start()
        self.audio.start()
        self.logger.start()

    def wait(self):
        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            log("INFO", "Keyboard interrupt - stopping")
            self.stop()

    def stop(self):
        self.stop_event.set()
        try:
            self.camera.join(timeout=3)
            self.audio.join(timeout=3)
            self.gps.join(timeout=3)
            self.logger.join(timeout=3)
        except Exception:
            pass
        self.heater.cleanup()
        log("INFO", "Flight software stopped")

# ----------------------------
# Entrypoint
# ----------------------------
def main():
    app = FlightApp()

    def handle_signal(signum, frame):
        log("INFO", f"Signal {signum} received - stopping")
        app.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    app.start()
    app.wait()

if __name__ == "__main__":
    main()
    
    # End of script
