"""Flask application factory for the investment dashboard."""
from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from flask import Flask, render_template

from .data_store import DataStore
from . import filters as jinja_filters

try:
    from dotenv import load_dotenv
except ImportError:  # dotenv is in requirements.txt; tolerate dev installs without it
    load_dotenv = None


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOG_DIR = _PROJECT_ROOT / "logs"
_ENV_PATH = _PROJECT_ROOT / ".env"
_LOGGING_CONFIGURED = False


def _configure_logging() -> None:
    """Install one stdout handler and one rotating-file handler at INFO."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _LOG_DIR / "daily.log"

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers when create_app() is called multiple times in tests
    root.handlers = [file_handler, stream_handler]

    _LOGGING_CONFIGURED = True


def _load_env() -> None:
    """Load .env without overriding real shell env vars (per Phase 0 risk note)."""
    if load_dotenv is None or not _ENV_PATH.exists():
        return
    load_dotenv(_ENV_PATH, override=False)


def create_app(data_path: Path | str | None = None) -> Flask:
    _load_env()
    _configure_logging()
    log = logging.getLogger("app")
    log.info("create_app: BACKFILL_ON_STARTUP=%s", os.environ.get("BACKFILL_ON_STARTUP", "false"))

    project_root = _PROJECT_ROOT
    data_path = Path(data_path) if data_path else project_root / "data" / "portfolio.json"

    app = Flask(
        __name__,
        template_folder=str(project_root / "templates"),
        static_folder=str(project_root / "static"),
    )

    app.config["DATA_PATH"] = data_path
    app.extensions["store"] = DataStore(data_path)

    jinja_filters.register(app)

    from .api import summary, holdings, performance, transactions, cashflows
    from .api import dividends, risk, fx, tax, tickers, benchmarks

    app.register_blueprint(summary.bp)
    app.register_blueprint(holdings.bp)
    app.register_blueprint(performance.bp)
    app.register_blueprint(transactions.bp)
    app.register_blueprint(cashflows.bp)
    app.register_blueprint(dividends.bp)
    app.register_blueprint(risk.bp)
    app.register_blueprint(fx.bp)
    app.register_blueprint(tax.bp)
    app.register_blueprint(tickers.bp)
    app.register_blueprint(benchmarks.bp)

    @app.get("/api/health")
    def health():
        store = app.extensions["store"]
        return {
            "ok": True,
            "data": {
                "months_loaded": len(store.months),
                "as_of": store.as_of,
            },
        }

    @app.get("/")
    def overview():
        return render_template("overview.html", page="overview")

    @app.get("/holdings")
    def holdings_page():
        return render_template("holdings.html", page="holdings")

    @app.get("/performance")
    def performance_page():
        return render_template("performance.html", page="performance")

    @app.get("/transactions")
    def transactions_page():
        return render_template("transactions.html", page="transactions")

    @app.get("/cashflows")
    def cashflows_page():
        return render_template("cashflows.html", page="cashflows")

    @app.get("/dividends")
    def dividends_page():
        return render_template("dividends.html", page="dividends")

    @app.get("/risk")
    def risk_page():
        return render_template("risk.html", page="risk")

    @app.get("/fx")
    def fx_page():
        return render_template("fx.html", page="fx")

    @app.get("/tax")
    def tax_page():
        return render_template("tax.html", page="tax")

    @app.get("/ticker/<code>")
    def ticker_page(code: str):
        return render_template("ticker.html", page="ticker", code=code)

    @app.get("/benchmark")
    def benchmark_page():
        return render_template("benchmark.html", page="benchmark")

    return app
