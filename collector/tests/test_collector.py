"""Unit tests for the SCK collector.

Tests parse_sensors, _iso_to_ns, write_to_influxdb (mocked),
duplicate detection, and edge cases.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure collector package is importable
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import collector  # noqa: E402
import config  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
FIXTURE_PATH = Path(__file__).parent / "mock_api_response.json"


@pytest.fixture()
def api_response() -> dict:
    """Load the mock API response fixture."""
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# parse_sensors
# ---------------------------------------------------------------------------
class TestParseSensors:
    def test_returns_only_mapped_sensors(self, api_response: dict) -> None:
        """Sensors not in SENSOR_NAME_MAP (id=9999) must be excluded."""
        readings = collector.parse_sensors(api_response)
        sensor_ids = {r["sensor_id"] for r in readings}
        assert 9999 not in sensor_ids

    def test_all_mapped_sensors_present(self, api_response: dict) -> None:
        """All sensors from the fixture that ARE in the map should appear."""
        readings = collector.parse_sensors(api_response)
        sensor_ids = {r["sensor_id"] for r in readings}
        # The fixture has sensors: 55,56,53,14,58,214,215,216,193,194,195,196,10,220
        expected = {55, 56, 53, 14, 58, 214, 215, 216, 193, 194, 195, 196, 10, 220}
        assert sensor_ids == expected

    def test_sensor_name_normalized(self, api_response: dict) -> None:
        """Sensor names must match SENSOR_NAME_MAP values."""
        readings = collector.parse_sensors(api_response)
        names = {r["sensor_name"] for r in readings}
        assert "temperature" in names
        assert "humidity" in names
        assert "pm_2_5" in names
        assert "noise_dba" in names
        assert "battery" in names

    def test_values_are_floats(self, api_response: dict) -> None:
        """All values must be float."""
        readings = collector.parse_sensors(api_response)
        for r in readings:
            assert isinstance(r["value"], float), f"Sensor {r['sensor_name']} value is not float"

    def test_timestamp_present(self, api_response: dict) -> None:
        """Every reading must have a timestamp."""
        readings = collector.parse_sensors(api_response)
        for r in readings:
            assert r["timestamp"] is not None
            assert "2026-02-04" in r["timestamp"]

    def test_null_value_skipped(self, api_response: dict) -> None:
        """A sensor with value=None should be excluded."""
        data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        # Inject a sensor with null value
        data["data"]["sensors"].append(
            {
                "id": 56,
                "name": "Sensirion SHT31 - Humidity",
                "unit": "%",
                "value": None,
                "last_reading_at": "2026-02-04T17:00:47Z",
                "measurement": {"id": 2, "name": "Relative Humidity"},
            }
        )
        readings = collector.parse_sensors(data)
        # The original id=56 has value=70.44, the null one should be skipped
        humidity_readings = [r for r in readings if r["sensor_id"] == 56]
        # Original valid reading still present
        assert len(humidity_readings) >= 1
        for r in humidity_readings:
            assert r["value"] is not None

    def test_empty_sensors_array(self) -> None:
        """Empty sensors array → empty result."""
        data = {"data": {"sensors": []}, "last_reading_at": "2026-01-01T00:00:00Z"}
        assert collector.parse_sensors(data) == []

    def test_missing_data_key(self) -> None:
        """Missing 'data' key → empty result (no crash)."""
        assert collector.parse_sensors({}) == []


# ---------------------------------------------------------------------------
# _iso_to_ns
# ---------------------------------------------------------------------------
class TestIsoToNs:
    def test_converts_z_suffix(self) -> None:
        ns = collector._iso_to_ns("2026-02-04T17:00:47Z")
        # 2026-02-04T17:00:47Z in seconds: known epoch value
        assert ns > 0
        assert isinstance(ns, int)
        # Nanosecond precision: should end with many digits
        assert ns > 1_000_000_000_000_000_000  # > 2001 in ns

    def test_converts_offset_suffix(self) -> None:
        ns = collector._iso_to_ns("2026-02-04T17:00:47+00:00")
        ns_z = collector._iso_to_ns("2026-02-04T17:00:47Z")
        assert ns == ns_z

    def test_different_timestamps_differ(self) -> None:
        ns1 = collector._iso_to_ns("2026-02-04T17:00:47Z")
        ns2 = collector._iso_to_ns("2026-02-04T17:01:47Z")
        assert ns2 - ns1 == 60_000_000_000  # 60 seconds in ns


# ---------------------------------------------------------------------------
# write_to_influxdb (mocked)
# ---------------------------------------------------------------------------
class TestWriteToInfluxdb:
    def test_writes_correct_count(self, api_response: dict) -> None:
        """write_to_influxdb should return count of lines written."""
        readings = collector.parse_sensors(api_response)
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_client.write_api.return_value = mock_write_api

        count = collector.write_to_influxdb(mock_client, readings, "19396")
        assert count == len(readings)
        mock_write_api.write.assert_called_once()

    def test_empty_readings_writes_nothing(self) -> None:
        mock_client = MagicMock()
        count = collector.write_to_influxdb(mock_client, [], "19396")
        assert count == 0
        mock_client.write_api.assert_not_called()

    def test_line_protocol_format(self, api_response: dict) -> None:
        """Verify the line protocol string passed to write_api."""
        readings = collector.parse_sensors(api_response)
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_client.write_api.return_value = mock_write_api

        collector.write_to_influxdb(mock_client, readings, "19396")

        call_kwargs = mock_write_api.write.call_args
        record = call_kwargs.kwargs.get("record") or call_kwargs[1].get("record")
        lines = record.split("\n")

        # Check first line structure
        first_line = lines[0]
        assert first_line.startswith("sck_sensors,")
        assert "device_id=19396" in first_line
        assert "sensor_name=" in first_line
        assert " value=" in first_line


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------
class TestDuplicateDetection:
    def test_poll_once_skips_duplicate(self, api_response: dict) -> None:
        """Second call with same timestamp should not write."""
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_client.write_api.return_value = mock_write_api

        with patch.object(collector, "fetch_device_data", return_value=api_response):
            # Reset global state
            collector._last_reading_at = None
            collector._polls_total = 0

            # First call — should write
            collector.poll_once(mock_client)
            assert collector._polls_total == 1
            assert mock_write_api.write.call_count == 1

            # Second call — same timestamp, should skip
            collector.poll_once(mock_client)
            assert collector._polls_total == 1  # No increment
            assert mock_write_api.write.call_count == 1  # No new write
