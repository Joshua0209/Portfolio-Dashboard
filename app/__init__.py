"""Flask application factory for the investment dashboard."""
from __future__ import annotations

from pathlib import Path

from flask import Flask, render_template

from .data_store import DataStore
from . import filters as jinja_filters


def create_app(data_path: Path | str | None = None) -> Flask:
    project_root = Path(__file__).resolve().parent.parent
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
