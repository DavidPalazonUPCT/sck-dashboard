"""Configuración del collector cargada desde variables de entorno.

Todas las variables tienen valores por defecto sensatos para desarrollo local.
En producción, se inyectan vía las env vars de Dokploy.
"""

from __future__ import annotations

import os


def _env(key: str, default: str) -> str:
    """Lee variable de entorno con fallback a default."""
    return os.environ.get(key, default)


# ---------------------------------------------------------------------------
# Smart Citizen API
# ---------------------------------------------------------------------------
SCK_DEVICE_ID: str = _env("SCK_DEVICE_ID", "19396")
SCK_API_BASE: str = _env("SCK_API_BASE", "https://api.smartcitizen.me/v0")
POLL_INTERVAL_SECONDS: int = int(_env("POLL_INTERVAL_SECONDS", "60"))

# ---------------------------------------------------------------------------
# InfluxDB
# ---------------------------------------------------------------------------
INFLUXDB_URL: str = _env("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN: str = _env("INFLUXDB_TOKEN", "my-super-secret-token")
INFLUXDB_ORG: str = _env("INFLUXDB_ORG", "sck")
INFLUXDB_BUCKET: str = _env("INFLUXDB_BUCKET", "sck_data")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = _env("LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# Healthcheck server
# ---------------------------------------------------------------------------
HEALTH_PORT: int = int(_env("HEALTH_PORT", "8000"))

# ---------------------------------------------------------------------------
# Sensor ID → nombre normalizado para InfluxDB tags
# Usamos el ID numérico (estable entre versiones de firmware) como clave.
# ---------------------------------------------------------------------------
SENSOR_NAME_MAP: dict[int, str] = {
    # Ambientales principales
    55: "temperature",
    56: "humidity",
    53: "noise_dba",
    14: "light",
    58: "pressure",
    # UV
    214: "uv_a",
    215: "uv_b",
    216: "uv_c",
    # Masa de partículas
    193: "pm_1",
    194: "pm_2_5",
    195: "pm_4",
    196: "pm_10",
    # Conteo de partículas
    197: "pn_0_5",
    198: "pn_1",
    199: "pn_2_5",
    200: "pn_4",
    201: "pn_10",
    202: "typical_particle_size",
    # Diagnóstico
    10: "battery",
    220: "wifi_rssi",
    221: "sd_card_present",
}
