# Prediction Collector Watchdog - Production Ready

Production-grade process monitoring with auto-restart, circuit breaker protection, and detailed health metrics.

## Features

- **Process monitoring** — PID tracking, resource usage checks
- **Data freshness validation** — Heartbeat file + data file mtime
- **Auto-restart with backoff** — Prevents restart loops
- **Circuit breaker** — Stops restarting after 5 failures per hour, waits 30 min
- **Self-monitoring** — Watchdog writes status JSON for external monitoring
- **Log rotation** — 10MB per file, keeps 5 backups
- **Notifications** — Telegram alerts on restart/error
- **Cross-platform** — Windows, Linux, macOS
- **CLI tools** — Status check, config validation, dependency install

## Quick Start

```bash
# 1. Install dependencies
python watchdog.py --install

# 2. Validate your config
python watchdog.py watchdog_config.json --validate

# 3. Start monitoring
python watchdog.py watchdog_config.json
```

## CLI Commands

```bash
# Check if collector is running (non-intrusive)
python watchdog.py --status

# Validate configuration without starting
python watchdog.py config.json --validate

# Install dependencies
python watchdog.py --install

# Run with config file
python watchdog.py /path/to/config.json
```

## Configuration

### Environment Variables (override config file)

| Variable | Description | Default |
|----------|-------------|---------|
| `WORKING_DIR` | Base directory for all paths | `.` |
| `COLLECTOR_SCRIPT` | Path to main.py | `src/main.py` |
| `DATA_FILE` | Parquet file pattern | `data/polymarket_{date}.parquet` |
| `DATA_FILE_PATTERN` | Glob pattern to find data files | `data/polymarket_*.parquet` |
| `HEARTBEAT_FILE` | Heartbeat timestamp | `data/collector_heartbeat.txt` |
| `CHECK_INTERVAL` | Seconds between health checks | `30` |
| `STALE_THRESHOLD` | Max seconds without update | `120` |
| `MAX_RESTARTS` | Per hour limit | `5` |
| `CIRCUIT_BREAKER_MINUTES` | Cooldown after max restarts | `30` |
| `MAX_MEMORY_MB` | Memory threshold warning | `512` |
| `MAX_CPU_PERCENT` | CPU threshold warning | `80` |
| `TELEGRAM_BOT_TOKEN` | Bot token for alerts | `null` |
| `TELEGRAM_CHAT_ID` | Chat ID for alerts | `null` |

### Config File Example

See `watchdog_config.json.example`

## Production Deployment

### 1. Linux (systemd)

Create `/etc/systemd/system/collector-watchdog.service`:

```ini
[Unit]
Description=Prediction Collector Watchdog
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/opt/prediction_collector
Environment="CONFIG_PATH=/opt/prediction_collector/watchdog_config.json"
ExecStart=/usr/bin/python3 /opt/prediction_collector/watchdog.py /opt/prediction_collector/watchdog_config.json
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=collector-watchdog

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable collector-watchdog
sudo systemctl start collector-watchdog
sudo systemctl status collector-watchdog
```

View logs:
```bash
sudo journalctl -u collector-watchdog -f
```

### 2. Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install -r requirements-watchdog.txt

# Health check via status file
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python watchdog.py --status || exit 1

CMD ["python", "watchdog.py", "watchdog_config.json"]
```

### 3. Windows (Background Service)

Option A: Task Scheduler (runs on login)
```powershell
# Create scheduled task
$action = New-ScheduledTaskAction -Execute "pythonw.exe" -Argument "C:\path\to\watchdog.py C:\path\to\config.json"
$trigger = New-ScheduledTaskTrigger -AtLogon
$settings = New-ScheduledTaskSettingsSet -Hidden -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName "CollectorWatchdog" -Action $action -Trigger $trigger -Settings $settings
```

Option B: NSSM (proper Windows service)
```cmd
nssm install CollectorWatchdog python C:\path\to\watchdog.py C:\path\to\config.json
nssm set CollectorWatchdog AppDirectory C:\path\to\prediction_collector
nssm start CollectorWatchdog
```

### 4. Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prediction-collector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prediction-collector
  template:
    metadata:
      labels:
        app: prediction-collector
    spec:
      containers:
      - name: collector
        image: your-registry/prediction-collector:latest
        command: ["python", "src/main.py"]
      - name: watchdog
        image: your-registry/prediction-collector:latest
        command: ["python", "watchdog.py", "watchdog_config.json"]
        livenessProbe:
          exec:
            command:
            - python
            - watchdog.py
            - --status
          initialDelaySeconds: 30
          periodSeconds: 30
```

## Monitoring & Alerting

### Data File Monitoring

The collector creates date-based files: `data/polymarket_YYYY-MM-DD.parquet`

The watchdog:
1. **Primary check:** Heartbeat file (written every 30s by collector)
2. **Secondary check:** Most recent file matching `data/polymarket_*.parquet`
3. **Tertiary check:** Today's specific file `data/polymarket_2026-02-15.parquet`

### Data File Monitoring

The collector creates date-based files: `data/polymarket_YYYY-MM-DD.parquet`

The watchdog:
1. **Primary check:** Heartbeat file `data/monitor/collector_heartbeat.txt` (written every 30s)
2. **Secondary check:** Most recent file matching `data/polymarket_*.parquet`
3. **Tertiary check:** Today's specific file

### Monitor Directory

Watchdog and collector monitor files in `data/monitor/`:

| File | Purpose |
|------|---------|
| `collector_heartbeat.txt` | Timestamp updated every 30s by collector |
| `collector.pid` | Process ID of running collector |
| `watchdog_status.json` | JSON status updated every 10s by watchdog |

### Status File

Watchdog writes `data/monitor/watchdog_status.json` every 10 seconds:

```json
{
  "timestamp": "2026-02-15T18:00:00+00:00",
  "status": "healthy",
  "collector_running": true,
  "data_fresh": true,
  "restart_count": 0,
  "metrics": {
    "checks_total": 120,
    "checks_failed": 0,
    "restarts_total": 0,
    "last_healthy": "2026-02-15T18:00:00+00:00",
    "current_status": "healthy"
  }
}
```

### Prometheus / Grafana

For Prometheus metrics, add a sidecar that parses `watchdog_status.json` and exposes:

```python
# metrics_exporter.py
from prometheus_client import Gauge, start_http_server
import json

WATCHDOG_STATUS = Gauge('watchdog_status', '1 if healthy, 0 if not')
COLLECTOR_UP = Gauge('collector_up', '1 if collector process running')
DATA_FRESH = Gauge('data_fresh', '1 if data updated within threshold')
RESTART_COUNT = Gauge('restart_count', 'Number of restarts in last hour')

def update_metrics():
    with open('data/watchdog_status.json') as f:
        status = json.load(f)
    
    WATCHDOG_STATUS.set(1 if status['status'] == 'healthy' else 0)
    COLLECTOR_UP.set(1 if status['collector_running'] else 0)
    DATA_FRESH.set(1 if status['data_fresh'] else 0)
    RESTART_COUNT.set(status['restart_count'])

start_http_server(9090)
```

### Telegram Alerts

Set these environment variables:
```bash
export TELEGRAM_BOT_TOKEN="123456789:ABCdefGHIjklMNOpqrSTU"
export TELEGRAM_CHAT_ID="-1001234567890"
export NOTIFY_ON_RESTART="true"
export NOTIFY_ON_ERROR="true"
```

To get your chat ID:
1. Message @userinfobot on Telegram
2. Or use: `https://api.telegram.org/bot<TOKEN>/getUpdates`

## Troubleshooting

### Circuit breaker opened

Watchdog stops restarting after 5 failures in an hour. Check logs:
```bash
python watchdog.py --status  # See last errors
```

Manual reset:
```bash
rm data/watchdog_status.json  # Reset circuit
python watchdog.py config.json  # Restart
```

### Watchdog itself crashes

Use systemd/supervisor to restart the watchdog:
```ini
[program:watchdog]
command=python watchdog.py config.json
autorestart=true
startretries=10
```

### Data file not updating

Check collector logs:
```bash
tail -f logs/watchdog.log  # Watchdog perspective
tail -f data/collector.log  # Collector logs (if configured)
```

### Permission errors

Ensure watchdog has write access to:
- `data/` (for PID, heartbeat, status files)
- `logs/` (for watchdog.log)
- Can execute `src/main.py`

## Architecture

```
┌─────────────┐     Heartbeat      ┌─────────────────┐
│  Collector  │ ─────────────────→ │  Heartbeat File │
│  (main.py)  │                    │  (30s interval) │
└─────────────┘                    └─────────────────┘
         ↓                         ↓
┌─────────────────────────────────────────────────────┐
│                    Watchdog                          │
│  ┌─────────────┐  ┌─────────────┐  ┌────────────┐ │
│  │ Process     │  │ Data Fresh  │  │ Resource   │ │
│  │ Check (PID) │  │ Check (file)│  │ Check      │ │
│  └─────────────┘  └─────────────┘  └────────────┘ │
│              ↓              ↓              ↓         │
│         Decision Engine (circuit breaker logic)      │
│                      ↓                              │
│         Restart Collector (with backoff)           │
└─────────────────────────────────────────────────────┘
         ↓
┌─────────────────┐     ┌──────────────┐
│  Status JSON    │     │  Telegram    │
│  (10s interval) │     │  Alerts      │
└─────────────────┘     └──────────────┘
```

## Migration from v1

If upgrading from previous watchdog:
1. Backup your config
2. Install new dependencies: `python watchdog.py --install`
3. Validate config: `python watchdog.py config.json --validate`
4. Update config with new fields (see example)
5. Restart

## License

Same as prediction_collector project.
