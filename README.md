# SCK Dashboard

Data pipeline and visualization for the Smart Citizen Kit 2.3 (device #19396).

Polls the Smart Citizen API, stores readings in InfluxDB, and visualizes them in Grafana.

## Quick Start (local)

```bash
cp .env.example .env
docker compose up -d --build
```

- **Grafana:** http://localhost:3000 (admin/admin)
- **InfluxDB:** http://localhost:8086 (admin/adminpassword123)

### Load test data

```bash
pip install influxdb-client requests
python scripts/seed-data.py
```

### Run tests

```bash
cd collector
uv pip install --system -e ".[dev]"
pytest -v
```

## Production (Dokploy)

See `sck-dashboard-guidelines.md` section 6 for full deployment instructions.

## Architecture

```
SCK 2.3 → Smart Citizen API → Collector (Python) → InfluxDB → Grafana
```
