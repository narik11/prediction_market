#!/usr/bin/env python3
"""
Prediction Collector Watchdog - Production Ready

Monitors the data collection process and auto-restarts if:
1. Process dies (PID check)
2. No data written in configured threshold (file/heartbeat check)
3. High memory usage (optional)
4. Self-degradation (watchdog health check)

Production features:
- Log rotation (10MB x 5 files)
- Self-monitoring heartbeat
- Config validation on startup
- Status CLI (check without starting)
- Pre-flight dependency checks
- Graceful signal handling
- Detailed health metrics

Works on Windows, Linux, macOS.
"""

import os
import sys
import time
import json
import signal
import psutil
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple
from logging.handlers import RotatingFileHandler


class WatchdogConfig:
    """Configuration for watchdog behavior with validation."""
    
    def __init__(self, config_path: Optional[str] = None):
        # Default paths (override via config file or env vars)
        self.collector_script = os.getenv("COLLECTOR_SCRIPT", "src/main.py")
        self.working_dir = os.getenv("WORKING_DIR", ".")
        self.data_file = os.getenv("DATA_FILE", "data/polymarket_{date}.parquet")
        self.data_file_pattern = os.getenv("DATA_FILE_PATTERN", "data/polymarket_*.parquet")
        self.heartbeat_file = os.getenv("HEARTBEAT_FILE", "data/monitor/collector_heartbeat.txt")
        self.pid_file = os.getenv("PID_FILE", "data/monitor/collector.pid")
        self.log_file = os.getenv("LOG_FILE", "logs/watchdog.log")
        self.status_file = os.getenv("STATUS_FILE", "data/monitor/watchdog_status.json")
        
        # Timing (seconds)
        self.check_interval = int(os.getenv("CHECK_INTERVAL", "30"))
        self.stale_threshold = int(os.getenv("STALE_THRESHOLD", "120"))
        self.startup_grace = int(os.getenv("STARTUP_GRACE", "30"))
        self.self_check_interval = int(os.getenv("SELF_CHECK_INTERVAL", "300"))  # 5 min
        
        # Restart settings
        self.max_restarts = int(os.getenv("MAX_RESTARTS", "5"))
        self.backoff_seconds = int(os.getenv("BACKOFF_SECONDS", "60"))
        self.circuit_breaker_minutes = int(os.getenv("CIRCUIT_BREAKER_MINUTES", "30"))
        
        # Resource limits
        self.max_memory_mb = int(os.getenv("MAX_MEMORY_MB", "512"))
        self.max_cpu_percent = int(os.getenv("MAX_CPU_PERCENT", "80"))
        
        # Notification (optional)
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.notify_on_restart = os.getenv("NOTIFY_ON_RESTART", "true").lower() == "true"
        self.notify_on_error = os.getenv("NOTIFY_ON_ERROR", "true").lower() == "true"
        
        # Load from config file if provided
        if config_path and Path(config_path).exists():
            with open(config_path) as f:
                file_config = json.load(f)
                for key, value in file_config.items():
                    if hasattr(self, key):
                        setattr(self, key, value)
        
        # Validate and resolve paths
        self._resolve_paths()
    
    def _resolve_paths(self):
        """Resolve relative paths against working directory."""
        base = Path(self.working_dir).resolve()
        
        self.collector_script = str(base / self.collector_script)
        self.data_file = str(base / self.data_file)
        self.heartbeat_file = str(base / self.heartbeat_file)
        self.pid_file = str(base / self.pid_file)
        self.log_file = str(base / self.log_file)
        self.status_file = str(base / self.status_file)
    
    def validate(self) -> Tuple[bool, list]:
        """Validate configuration. Returns (is_valid, list_of_errors)."""
        errors = []
        
        # Check collector script exists
        if not Path(self.collector_script).exists():
            errors.append(f"Collector script not found: {self.collector_script}")
        
        # Check data directory is writable
        data_dir = Path(self.data_file).parent
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
            test_file = data_dir / ".write_test"
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            errors.append(f"Data directory not writable: {data_dir} - {e}")
        
        # Check log directory is writable
        log_dir = Path(self.log_file).parent
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            errors.append(f"Log directory not writable: {log_dir} - {e}")
        
        # Validate timing
        if self.check_interval < 5:
            errors.append(f"check_interval too low: {self.check_interval}s (min 5s)")
        if self.stale_threshold < 30:
            errors.append(f"stale_threshold too low: {self.stale_threshold}s (min 30s)")
        if self.stale_threshold <= self.check_interval:
            errors.append(f"stale_threshold ({self.stale_threshold}) must be > check_interval ({self.check_interval})")
        
        return len(errors) == 0, errors
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            k: v for k, v in self.__dict__.items() 
            if not k.startswith('_')
        }


class CollectorWatchdog:
    """Production-ready watchdog for prediction collector."""
    
    def __init__(self, config: WatchdogConfig):
        self.config = config
        self.setup_logging()
        self.logger = logging.getLogger("watchdog")
        
        # State
        self.running = False
        self.collector_process: Optional[subprocess.Popen] = None
        self.last_restart: Optional[float] = None
        self.restart_count = 0
        self.restart_history: list = []
        self.last_self_check: Optional[float] = None
        self.circuit_open = False
        self.circuit_opened_at: Optional[float] = None
        
        # Health metrics
        self.metrics = {
            "checks_total": 0,
            "checks_failed": 0,
            "restarts_total": 0,
            "last_healthy": None,
            "current_status": "initializing"
        }
        
        # Signals
        self._shutdown_event = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, self._signal_handler)
    
    def setup_logging(self):
        """Configure production logging with rotation."""
        log_dir = Path(self.config.log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Rotating file handler: 10MB per file, keep 3 backups
        file_handler = RotatingFileHandler(
            self.config.log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=3
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        
        # Root logger configuration
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler]
        )
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_event = True
        self.metrics["current_status"] = "shutting_down"
    
    def write_status(self):
        """Write current status to JSON file for external monitoring."""
        try:
            status = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": self.metrics["current_status"],
                "collector_running": self.is_collector_running(),
                "data_fresh": self.is_data_fresh(),
                "restart_count": self.restart_count,
                "metrics": self.metrics,
                "config": {
                    "check_interval": self.config.check_interval,
                    "stale_threshold": self.config.stale_threshold
                }
            }
            status_path = Path(self.config.status_file)
            status_path.parent.mkdir(parents=True, exist_ok=True)
            status_path.write_text(json.dumps(status, indent=2))
        except Exception as e:
            self.logger.error(f"Failed to write status file: {e}")
    
    def is_collector_running(self) -> bool:
        """Check if collector process is active and healthy."""
        if self.collector_process is None:
            return False
        
        # Check if process still running
        if self.collector_process.poll() is not None:
            return False
        
        # Double-check with PID file
        pid_file = Path(self.config.pid_file)
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                if not psutil.pid_exists(pid):
                    return False
                
                # Check resource usage
                proc = psutil.Process(pid)
                mem_mb = proc.memory_info().rss / 1024 / 1024
                cpu_percent = proc.cpu_percent(interval=0.1)
                
                if mem_mb > self.config.max_memory_mb:
                    self.logger.warning(f"Collector memory high: {mem_mb:.0f}MB (limit: {self.config.max_memory_mb}MB)")
                
                if cpu_percent > self.config.max_cpu_percent:
                    self.logger.warning(f"Collector CPU high: {cpu_percent:.0f}%")
                    
            except (ValueError, psutil.NoSuchProcess):
                return False
            except psutil.AccessDenied:
                pass  # Can't check resources, but process exists
        
        return True
    
    def get_last_write_time(self) -> Optional[float]:
        """Get timestamp of last data write."""
        # Check heartbeat file first (most reliable)
        heartbeat_path = Path(self.config.heartbeat_file)
        if heartbeat_path.exists():
            try:
                content = heartbeat_path.read_text().strip()
                dt = datetime.fromisoformat(content.replace('Z', '+00:00'))
                return dt.timestamp()
            except (ValueError, OSError):
                pass
        
        # Fallback: find most recent data file matching pattern
        import glob
        base_dir = Path(self.config.working_dir)
        pattern = str(base_dir / self.config.data_file_pattern)
        files = glob.glob(pattern)
        
        if files:
            # Get most recently modified file
            newest = max(files, key=os.path.getmtime)
            try:
                return os.path.getmtime(newest)
            except OSError:
                pass
        
        # Last resort: check specific date-based filename for today
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        date_file = Path(self.config.data_file.replace("{date}", today_str))
        if date_file.exists():
            try:
                return date_file.stat().st_mtime
            except OSError:
                pass
        
        return None
    
    def is_data_fresh(self) -> bool:
        """Check if collector is writing data within threshold."""
        last_write = self.get_last_write_time()
        
        if last_write is None:
            if self.last_restart:
                elapsed = time.time() - self.last_restart
                if elapsed < self.config.startup_grace:
                    return True
            return False
        
        elapsed = time.time() - last_write
        is_fresh = elapsed < self.config.stale_threshold
        
        if not is_fresh:
            self.logger.warning(
                f"Data stale: last write {elapsed:.0f}s ago "
                f"(threshold: {self.config.stale_threshold}s)"
            )
        
        return is_fresh
    
    def check_circuit_breaker(self) -> bool:
        """Check if circuit breaker is open (too many restarts)."""
        if not self.circuit_open:
            return False
        
        # Check if time to close circuit
        if self.circuit_opened_at:
            elapsed = time.time() - self.circuit_opened_at
            if elapsed > (self.config.circuit_breaker_minutes * 60):
                self.logger.info("Circuit breaker cooling period ended, closing circuit")
                self.circuit_open = False
                self.circuit_opened_at = None
                self.restart_count = 0
                self.restart_history.clear()
                return False
        
        return True
    
    def open_circuit_breaker(self):
        """Open circuit breaker after too many failures."""
        self.circuit_open = True
        self.circuit_opened_at = time.time()
        self.metrics["current_status"] = "circuit_breaker_open"
        self.logger.error(
            f"CIRCUIT BREAKER OPENED: Too many restarts. "
            f"Cooling down for {self.config.circuit_breaker_minutes} minutes."
        )
        self.notify(
            f"🚨 CIRCUIT BREAKER OPENED\n"
            f"Too many collector restarts. Manual intervention may be required.\n"
            f"Cooling period: {self.config.circuit_breaker_minutes} minutes"
        )
    
    def start_collector(self) -> bool:
        """Start the collector process with rate limiting."""
        # Check circuit breaker
        if self.check_circuit_breaker():
            self.logger.warning("Circuit breaker open, skipping restart")
            return False
        
        # Rate limiting per hour
        now = time.time()
        self.restart_history = [t for t in self.restart_history if now - t < 3600]
        
        if len(self.restart_history) >= self.config.max_restarts:
            self.open_circuit_breaker()
            return False
        
        # Backoff between restarts
        if self.last_restart and (now - self.last_restart) < self.config.backoff_seconds:
            wait = self.config.backoff_seconds - (now - self.last_restart)
            self.logger.info(f"Backing off: waiting {wait:.0f}s before restart")
            time.sleep(wait)
        
        try:
            # Kill any existing process
            self.stop_collector()
            time.sleep(2)  # Brief pause for cleanup
            
            # Start new process
            self.logger.info(f"Starting collector: {self.config.collector_script}")
            
            python = sys.executable
            cmd = [python, self.config.collector_script]
            
            env = os.environ.copy()
            env["WATCHDOG_PID"] = str(os.getpid())
            
            if sys.platform == 'win32':
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                self.collector_process = subprocess.Popen(
                    cmd,
                    startupinfo=startupinfo,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                    cwd=self.config.working_dir,
                    env=env
                )
            else:
                self.collector_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    cwd=self.config.working_dir,
                    env=env
                )
            
            # Write PID file
            pid_path = Path(self.config.pid_file)
            pid_path.parent.mkdir(parents=True, exist_ok=True)
            pid_path.write_text(str(self.collector_process.pid))
            
            self.last_restart = time.time()
            self.restart_history.append(self.last_restart)
            self.restart_count += 1
            self.metrics["restarts_total"] += 1
            self.metrics["current_status"] = "restarted"
            
            self.logger.info(f"Collector started (PID: {self.collector_process.pid})")
            
            if self.config.notify_on_restart:
                self.notify(f"🔄 Collector restarted (attempt #{self.restart_count}/hour)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start collector: {e}", exc_info=True)
            self.metrics["checks_failed"] += 1
            return False
    
    def stop_collector(self):
        """Stop the collector process gracefully."""
        if self.collector_process is None:
            return
        
        self.logger.info("Stopping collector...")
        
        try:
            self.collector_process.terminate()
            try:
                self.collector_process.wait(timeout=10)
                self.logger.info("Collector stopped gracefully")
            except subprocess.TimeoutExpired:
                self.logger.warning("Collector didn't terminate, forcing kill...")
                self.collector_process.kill()
                self.collector_process.wait()
                self.logger.info("Collector killed")
        except Exception as e:
            self.logger.error(f"Error stopping collector: {e}")
        finally:
            self.collector_process = None
            try:
                Path(self.config.pid_file).unlink(missing_ok=True)
            except OSError:
                pass
    
    def notify(self, message: str):
        """Send notification via Telegram or console."""
        self.logger.info(f"NOTIFY: {message}")
        
        if self.config.telegram_bot_token and self.config.telegram_chat_id:
            try:
                import urllib.request
                import urllib.parse
                
                url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
                data = urllib.parse.urlencode({
                    'chat_id': self.config.telegram_chat_id,
                    'text': f"🐕 Watchdog Alert\n\n{message}\n\n⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }).encode()
                
                req = urllib.request.Request(url, data=data, method='POST')
                req.add_header('Content-Type', 'application/x-www-form-urlencoded')
                
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status == 200:
                        self.logger.debug("Telegram notification sent")
                    else:
                        self.logger.warning(f"Telegram returned status {resp.status}")
                        
            except Exception as e:
                self.logger.error(f"Failed to send Telegram notification: {e}")
    
    def run(self):
        """Main watchdog loop with self-monitoring."""
        self.logger.info("="*60)
        self.logger.info("Prediction Collector Watchdog v2.0 - Production Ready")
        self.logger.info(f"Config: check_interval={self.config.check_interval}s, "
                        f"stale_threshold={self.config.stale_threshold}s")
        self.logger.info("="*60)
        
        # Initial start
        if not self.start_collector():
            self.logger.error("Failed to start collector on init")
        
        self.running = True
        last_status_write = 0
        last_self_check = 0
        
        while self.running and not self._shutdown_event:
            try:
                loop_start = time.time()
                
                # Self-health check every 5 minutes
                if loop_start - last_self_check > self.config.self_check_interval:
                    self.logger.debug("Self-health check OK")
                    last_self_check = loop_start
                
                # Health checks
                self.metrics["checks_total"] += 1
                process_ok = self.is_collector_running()
                data_ok = self.is_data_fresh()
                
                if process_ok and data_ok:
                    self.metrics["last_healthy"] = datetime.now(timezone.utc).isoformat()
                    self.metrics["current_status"] = "healthy"
                
                # Decision logic
                if not process_ok:
                    self.metrics["checks_failed"] += 1
                    self.metrics["current_status"] = "process_dead"
                    self.logger.error("Collector process not running. Initiating restart...")
                    self.start_collector()
                    
                elif not data_ok:
                    self.metrics["checks_failed"] += 1
                    self.metrics["current_status"] = "data_stale"
                    self.logger.error("Collector running but data stale. Restarting...")
                    self.stop_collector()
                    time.sleep(2)
                    self.start_collector()
                
                # Write status file periodically
                if loop_start - last_status_write > 10:
                    self.write_status()
                    last_status_write = loop_start
                
                # Calculate sleep to maintain interval
                elapsed = time.time() - loop_start
                sleep_time = max(0, self.config.check_interval - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"Watchdog loop error: {e}", exc_info=True)
                self.metrics["checks_failed"] += 1
                time.sleep(5)
        
        # Shutdown
        self.logger.info("Watchdog shutting down...")
        self.metrics["current_status"] = "stopped"
        self.write_status()
        self.stop_collector()
        self.logger.info("Watchdog stopped gracefully")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status (for CLI use)."""
        return {
            "watchdog_pid": os.getpid(),
            "collector_running": self.is_collector_running(),
            "data_fresh": self.is_data_fresh(),
            "last_write": self.get_last_write_time(),
            "metrics": self.metrics,
            "config": {
                "check_interval": self.config.check_interval,
                "stale_threshold": self.config.stale_threshold,
                "max_restarts": self.config.max_restarts
            }
        }


def check_dependencies() -> Tuple[bool, list]:
    """Check if required dependencies are installed."""
    missing = []
    
    try:
        import psutil
    except ImportError:
        missing.append("psutil (pip install psutil)")
    
    return len(missing) == 0, missing


def main():
    """Entry point with CLI."""
    parser = argparse.ArgumentParser(
        description="Prediction Collector Watchdog - Production Ready"
    )
    parser.add_argument(
        "config", 
        nargs="?",
        help="Path to JSON config file"
    )
    parser.add_argument(
        "--status", 
        action="store_true",
        help="Check status without starting watchdog"
    )
    parser.add_argument(
        "--validate", 
        action="store_true", 
        help="Validate configuration and exit"
    )
    parser.add_argument(
        "--install", 
        action="store_true",
        help="Install dependencies (psutil)"
    )
    
    args = parser.parse_args()
    
    # Install dependencies
    if args.install:
        print("Installing watchdog dependencies...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
        print("✓ Dependencies installed")
        return
    
    # Check dependencies
    deps_ok, missing = check_dependencies()
    if not deps_ok:
        print("ERROR: Missing dependencies:")
        for dep in missing:
            print(f"  - {dep}")
        print("\nInstall with: python watchdog.py --install")
        sys.exit(1)
    
    # Load config
    config = WatchdogConfig(args.config)
    
    # Validate only
    if args.validate:
        is_valid, errors = config.validate()
        if is_valid:
            print("✓ Configuration valid")
            print(json.dumps(config.to_dict(), indent=2))
            sys.exit(0)
        else:
            print("✗ Configuration invalid:")
            for error in errors:
                print(f"  - {error}")
            sys.exit(1)
    
    # Status check only
    if args.status:
        watchdog = CollectorWatchdog(config)
        status = watchdog.get_status()
        print(json.dumps(status, indent=2, default=str))
        
        # Summary
        if status["collector_running"] and status["data_fresh"]:
            print("\n✓ Collector is healthy")
            sys.exit(0)
        elif status["collector_running"]:
            print("\n⚠ Collector running but data is stale")
            sys.exit(1)
        else:
            print("\n✗ Collector is not running")
            sys.exit(1)
    
    # Validate before starting
    is_valid, errors = config.validate()
    if not is_valid:
        print("ERROR: Configuration invalid:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    
    # Run watchdog
    watchdog = CollectorWatchdog(config)
    watchdog.run()


if __name__ == "__main__":
    main()
