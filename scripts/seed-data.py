"""Generate synthetic sensor data for local testing.

Writes 24h of realistic data to InfluxDB at 1-minute intervals.
Patterns:
  - Temperature: sinusoidal 18-28°C (day/night cycle)
  - Humidity: inversely correlated with temperature, 30-70%
  - PM2.5: low base (5-15) with random spikes
  - Noise: day/night pattern (30-60 dBA)
  - Pressure: slow drift (101.0-102.5 kPa)
  - UV: diurnal bell curve (0 at night, peak at noon)

Usage:
  python scripts/seed-data.py [--influxdb-url URL] [--token TOKEN]
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def generate_data(
    start: datetime,
    end: datetime,
    interval_minutes: int = 1,
    seed: int = 42,
) -> list[str]:
    """Generate InfluxDB line protocol strings for all sensors."""
    rng = random.Random(seed)
    lines: list[str] = []
    device_id = "19396"
    current = start

    while current <= end:
        ts_ns = int(current.timestamp() * 1_000_000_000)
        hour = current.hour + current.minute / 60.0
        day_frac = hour / 24.0  # 0..1

        # Temperature: sinusoidal 18-28°C, peak ~15h
        temp = 23.0 + 5.0 * math.sin(2 * math.pi * (day_frac - 0.25)) + rng.gauss(0, 0.3)

        # Humidity: inversely correlated with temperature
        hum = max(25, min(85, 75.0 - 1.5 * (temp - 18.0) + rng.gauss(0, 2.0)))

        # Noise: higher during day (8-22h)
        if 8 <= current.hour < 22:
            noise = 45 + rng.gauss(0, 5)
        else:
            noise = 32 + rng.gauss(0, 3)
        noise = max(20, min(80, noise))

        # Light: bell curve during day, 0 at night
        if 6 <= current.hour <= 20:
            light_frac = math.sin(math.pi * (hour - 6) / 14)
            light = max(0, 50000 * light_frac + rng.gauss(0, 500))
        else:
            light = max(0, rng.gauss(0, 2))

        # Pressure: slow sinusoidal drift 101.0-102.5 kPa
        pressure = 101.75 + 0.75 * math.sin(2 * math.pi * day_frac / 3 + rng.gauss(0, 0.1))

        # UV-A: diurnal, peak at noon
        if 7 <= current.hour <= 19:
            uv_frac = math.sin(math.pi * (hour - 7) / 12)
            uv_a = max(0, 8.0 * uv_frac + rng.gauss(0, 0.2))
            uv_b = max(0, 3.0 * uv_frac + rng.gauss(0, 0.1))
            uv_c = max(0, 0.5 * uv_frac + rng.gauss(0, 0.02))
        else:
            uv_a = max(0, rng.gauss(0.02, 0.01))
            uv_b = max(0, rng.gauss(0.01, 0.005))
            uv_c = max(0, rng.gauss(0.005, 0.002))

        # PM: low base with occasional spikes
        pm_base = 5 + rng.gauss(0, 2)
        if rng.random() < 0.05:  # 5% chance of spike
            pm_base += rng.uniform(15, 40)
        pm_1 = max(0, pm_base * 0.6 + rng.gauss(0, 0.5))
        pm_2_5 = max(0, pm_base + rng.gauss(0, 1))
        pm_4 = max(0, pm_base * 1.1 + rng.gauss(0, 1))
        pm_10 = max(0, pm_base * 1.2 + rng.gauss(0, 1.5))

        # Battery: slow decline 100->94 over 24h
        minutes_elapsed = (current - start).total_seconds() / 60
        battery = max(90, 100 - minutes_elapsed / 240 + rng.gauss(0, 0.1))

        # WiFi RSSI
        rssi = -65 + rng.gauss(0, 8)

        sensors = {
            "temperature": round(temp, 2),
            "humidity": round(hum, 2),
            "noise_dba": round(noise, 2),
            "light": round(light, 2),
            "pressure": round(pressure, 2),
            "uv_a": round(uv_a, 4),
            "uv_b": round(uv_b, 4),
            "uv_c": round(uv_c, 4),
            "pm_1": round(pm_1, 2),
            "pm_2_5": round(pm_2_5, 2),
            "pm_4": round(pm_4, 2),
            "pm_10": round(pm_10, 2),
            "battery": round(battery, 1),
            "wifi_rssi": round(rssi, 1),
        }

        for name, value in sensors.items():
            lines.append(
                f"sck_sensors,device_id={device_id},sensor_name={name} "
                f"value={value} {ts_ns}"
            )

        current += timedelta(minutes=interval_minutes)

    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed InfluxDB with synthetic SCK data")
    parser.add_argument("--influxdb-url", default="http://localhost:8086")
    parser.add_argument("--token", default="my-super-secret-token")
    parser.add_argument("--org", default="sck")
    parser.add_argument("--bucket", default="sck_data")
    parser.add_argument("--hours", type=int, default=24, help="Hours of data to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(hours=args.hours)

    print(f"Generating {args.hours}h of data ({start} -> {end}) ...")
    lines = generate_data(start, end, interval_minutes=1, seed=args.seed)
    print(f"Generated {len(lines)} line protocol records")

    client = InfluxDBClient(url=args.influxdb_url, token=args.token, org=args.org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Write in batches of 1000
    batch_size = 1000
    for i in range(0, len(lines), batch_size):
        batch = lines[i : i + batch_size]
        write_api.write(bucket=args.bucket, org=args.org, record="\n".join(batch))
        written = min(i + batch_size, len(lines))
        print(f"  Written {written}/{len(lines)} records")

    client.close()
    print("Done!")


if __name__ == "__main__":
    main()
