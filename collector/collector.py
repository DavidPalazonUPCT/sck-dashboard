"""Smart Citizen Kit data collector.

Polls the Smart Citizen API, parses sensor readings, and writes them
to InfluxDB using the line protocol.  Runs a lightweight healthcheck
HTTP server in a background thread.

Design constraints (from guidelines):
- No Flask/Django — stdlib http.server for healthcheck.
- Idempotent — skips duplicate timestamps.
- Graceful shutdown on SIGTERM/SIGINT.
- Exponential backoff on network errors (max 5 min).
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import requests
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

import config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("collector")

# ---------------------------------------------------------------------------
# Shared mutable state (thread-safe via GIL for simple reads/writes)
# ---------------------------------------------------------------------------
_last_poll_ts: str | None = None  # ISO timestamp of last successful poll
_polls_total: int = 0
_last_reading_at: str | None = None  # Last processed reading timestamp
_shutdown_event = threading.Event()


# ---------------------------------------------------------------------------
# Healthcheck HTTP server
# ---------------------------------------------------------------------------
class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal healthcheck endpoint."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            body = json.dumps(
                {
                    "status": "ok",
                    "last_poll": _last_poll_ts,
                    "polls_total": _polls_total,
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Silence default stderr logging."""


def _run_health_server() -> None:
    """Run the healthcheck server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", config.HEALTH_PORT), _HealthHandler)
    server.timeout = 1
    logger.info("Healthcheck server listening on :%d", config.HEALTH_PORT)
    while not _shutdown_event.is_set():
        server.handle_request()
    server.server_close()


# ---------------------------------------------------------------------------
# API polling
# ---------------------------------------------------------------------------
def fetch_device_data() -> dict[str, Any]:
    """GET /devices/{id} and return parsed JSON.

    Raises:
        requests.RequestException: on any HTTP / network error.
    """
    url = f"{config.SCK_API_BASE}/devices/{config.SCK_DEVICE_ID}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_sensors(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract sensor readings from the API response.

    Returns a list of dicts with keys: sensor_id, sensor_name, value, timestamp.
    Only sensors present in SENSOR_NAME_MAP are included.
    Sensors with null values are skipped.
    """
    sensors_raw: list[dict[str, Any]] = data.get("data", {}).get("sensors", [])
    readings: list[dict[str, Any]] = []

    for s in sensors_raw:
        sid: int = s.get("id", -1)
        sensor_name = config.SENSOR_NAME_MAP.get(sid)
        if sensor_name is None:
            continue  # Sensor not in our map — skip

        value = s.get("value")
        if value is None:
            continue  # No reading — skip

        timestamp_str = s.get("last_reading_at") or data.get("last_reading_at")
        if timestamp_str is None:
            continue

        readings.append(
            {
                "sensor_id": sid,
                "sensor_name": sensor_name,
                "value": float(value),
                "timestamp": timestamp_str,
            }
        )

    return readings


def _iso_to_ns(iso_str: str) -> int:
    """Convert ISO 8601 timestamp to nanoseconds since epoch (InfluxDB precision)."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)


def write_to_influxdb(
    client: InfluxDBClient,
    readings: list[dict[str, Any]],
    device_id: str,
) -> int:
    """Write sensor readings to InfluxDB using line protocol.

    Returns the number of points written.
    """
    if not readings:
        return 0

    write_api = client.write_api(write_options=SYNCHRONOUS)
    lines: list[str] = []

    for r in readings:
        # Escape tag values (sensor_name is already snake_case, safe)
        ts_ns = _iso_to_ns(r["timestamp"])
        line = (
            f'sck_sensors,device_id={device_id},sensor_name={r["sensor_name"]} '
            f'value={r["value"]} {ts_ns}'
        )
        lines.append(line)

    write_api.write(
        bucket=config.INFLUXDB_BUCKET,
        org=config.INFLUXDB_ORG,
        record="\n".join(lines),
    )
    return len(lines)


# ---------------------------------------------------------------------------
# Main poll cycle
# ---------------------------------------------------------------------------
def poll_once(client: InfluxDBClient) -> None:
    """Execute a single poll-parse-write cycle."""
    global _last_poll_ts, _polls_total, _last_reading_at  # noqa: PLW0603

    data = fetch_device_data()
    device_reading_at = data.get("last_reading_at")

    # Duplicate detection: skip if we already processed this timestamp
    if device_reading_at and device_reading_at == _last_reading_at:
        logger.debug(
            "Skipping duplicate reading (timestamp=%s)", device_reading_at
        )
        return

    readings = parse_sensors(data)
    if not readings:
        logger.warning("No valid sensor readings in API response")
        return

    count = write_to_influxdb(client, readings, config.SCK_DEVICE_ID)
    _last_reading_at = device_reading_at
    _last_poll_ts = datetime.now(timezone.utc).isoformat()
    _polls_total += 1

    logger.info(
        "Written %d points (reading_at=%s, poll #%d)",
        count,
        device_reading_at,
        _polls_total,
    )


def _backoff_sleep(attempt: int) -> float:
    """Exponential backoff: 2^attempt seconds, capped at 300s (5 min)."""
    return min(2**attempt, 300)


def run() -> None:
    """Main loop: poll API → write InfluxDB, respecting interval and backoff."""
    logger.info(
        "Collector starting — device=%s, interval=%ds, influx=%s",
        config.SCK_DEVICE_ID,
        config.POLL_INTERVAL_SECONDS,
        config.INFLUXDB_URL,
    )

    client = InfluxDBClient(
        url=config.INFLUXDB_URL,
        token=config.INFLUXDB_TOKEN,
        org=config.INFLUXDB_ORG,
    )

    # Verify InfluxDB connectivity before entering loop
    try:
        health = client.health()
        if health.status == "pass":
            logger.info("InfluxDB connection OK (version=%s)", health.version)
        else:
            logger.warning("InfluxDB health check: %s", health.message)
    except Exception:
        logger.exception("Cannot reach InfluxDB — will retry in the loop")

    consecutive_errors = 0

    while not _shutdown_event.is_set():
        try:
            poll_once(client)
            consecutive_errors = 0
        except requests.RequestException as exc:
            consecutive_errors += 1
            wait = _backoff_sleep(consecutive_errors)
            logger.error(
                "API request failed (attempt %d, backoff %.0fs): %s",
                consecutive_errors,
                wait,
                exc,
            )
            _shutdown_event.wait(wait)
            continue
        except Exception:
            consecutive_errors += 1
            wait = _backoff_sleep(consecutive_errors)
            logger.exception(
                "Unexpected error (attempt %d, backoff %.0fs)",
                consecutive_errors,
                wait,
            )
            _shutdown_event.wait(wait)
            continue

        # Wait for next poll interval (interruptible)
        _shutdown_event.wait(config.POLL_INTERVAL_SECONDS)

    logger.info("Shutdown complete")
    client.close()


# ---------------------------------------------------------------------------
# Signal handling & entrypoint
# ---------------------------------------------------------------------------
def _handle_signal(signum: int, _frame: Any) -> None:
    sig_name = signal.Signals(signum).name
    logger.info("Received %s — initiating graceful shutdown", sig_name)
    _shutdown_event.set()


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Start healthcheck server in background
    health_thread = threading.Thread(target=_run_health_server, daemon=True)
    health_thread.start()

    run()


if __name__ == "__main__":
    main()
