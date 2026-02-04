"""Import historical data from the Smart Citizen API into InfluxDB.

Iterates over each sensor in SENSOR_NAME_MAP, fetches readings via
the /devices/{id}/readings endpoint, and writes them to InfluxDB.

Usage:
  python scripts/backfill.py --from 2025-02-01 --to 2025-02-04
  python scripts/backfill.py --from 2025-02-01 --to 2025-02-04 --rollup 5m
  python scripts/backfill.py --influxdb-url http://localhost:8086 --token <TOKEN> --from 2025-01-01 --to 2025-02-01
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

import requests
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Sensor ID -> normalized name (same as collector/config.py)
SENSOR_NAME_MAP: dict[int, str] = {
    55: "temperature",
    56: "humidity",
    53: "noise_dba",
    14: "light",
    58: "pressure",
    214: "uv_a",
    215: "uv_b",
    216: "uv_c",
    193: "pm_1",
    194: "pm_2_5",
    195: "pm_4",
    196: "pm_10",
    197: "pn_0_5",
    198: "pn_1",
    199: "pn_2_5",
    200: "pn_4",
    201: "pn_10",
    202: "typical_particle_size",
    10: "battery",
    220: "wifi_rssi",
}


def fetch_readings(
    api_base: str,
    device_id: str,
    sensor_id: int,
    rollup: str,
    from_date: str,
    to_date: str,
) -> list[list]:
    """Fetch historical readings for a single sensor.

    Returns list of [timestamp_iso, value] tuples from the API.
    """
    url = f"{api_base}/devices/{device_id}/readings"
    params = {
        "sensor_id": sensor_id,
        "rollup": rollup,
        "from": from_date,
        "to": to_date,
        "function": "avg",
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("readings", [])


def iso_to_ns(iso_str: str) -> int:
    """Convert ISO 8601 timestamp to nanoseconds since epoch."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill InfluxDB with historical Smart Citizen data"
    )
    parser.add_argument(
        "--from", dest="from_date", required=True,
        help="Start date (YYYY-MM-DD or ISO 8601, e.g. 2025-02-01)",
    )
    parser.add_argument(
        "--to", dest="to_date", required=True,
        help="End date (YYYY-MM-DD or ISO 8601, e.g. 2025-02-04)",
    )
    parser.add_argument("--rollup", default="1m", help="Rollup interval (default: 1m)")
    parser.add_argument("--device-id", default="19396")
    parser.add_argument("--api-base", default="https://api.smartcitizen.me/v0")
    parser.add_argument("--influxdb-url", default="http://localhost:8086")
    parser.add_argument("--token", default="my-super-secret-token")
    parser.add_argument("--org", default="sck")
    parser.add_argument("--bucket", default="sck_data")
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Delay between API requests in seconds (default: 1.0)",
    )
    args = parser.parse_args()

    # Normalize dates to ISO 8601 with timezone for the API
    def normalize_date(date_str: str, end_of_day: bool = False) -> str:
        if "T" in date_str:
            return date_str
        if end_of_day:
            return f"{date_str}T23:59:59Z"
        return f"{date_str}T00:00:00Z"

    args.from_date = normalize_date(args.from_date)
    args.to_date = normalize_date(args.to_date, end_of_day=True)

    client = InfluxDBClient(url=args.influxdb_url, token=args.token, org=args.org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    total_sensors = len(SENSOR_NAME_MAP)
    total_points = 0

    print(f"Backfilling device {args.device_id}: {args.from_date} -> {args.to_date} (rollup={args.rollup})")
    print(f"Sensors to process: {total_sensors}")
    print()

    for idx, (sensor_id, sensor_name) in enumerate(SENSOR_NAME_MAP.items(), 1):
        print(f"[{idx}/{total_sensors}] Fetching sensor {sensor_id} ({sensor_name})...", end=" ")
        sys.stdout.flush()

        try:
            readings = fetch_readings(
                api_base=args.api_base,
                device_id=args.device_id,
                sensor_id=sensor_id,
                rollup=args.rollup,
                from_date=args.from_date,
                to_date=args.to_date,
            )
        except requests.RequestException as exc:
            print(f"ERROR: {exc}")
            time.sleep(args.delay)
            continue

        if not readings:
            print("no data")
            time.sleep(args.delay)
            continue

        # Build line protocol
        lines: list[str] = []
        for reading in readings:
            if len(reading) < 2 or reading[1] is None:
                continue
            ts_iso, value = reading[0], reading[1]
            ts_ns = iso_to_ns(ts_iso)
            lines.append(
                f"sck_sensors,device_id={args.device_id},sensor_name={sensor_name} "
                f"value={float(value)} {ts_ns}"
            )

        if lines:
            # Write in batches of 5000
            batch_size = 5000
            for i in range(0, len(lines), batch_size):
                batch = lines[i : i + batch_size]
                write_api.write(bucket=args.bucket, org=args.org, record="\n".join(batch))

        total_points += len(lines)
        print(f"{len(lines)} points written")

        # Rate limiting
        time.sleep(args.delay)

    client.close()
    print(f"\nBackfill complete: {total_points} total points written")


if __name__ == "__main__":
    main()
