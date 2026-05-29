"""
Entry point for the E-Commerce Product Intelligence Dashboard.
Development:  python app.py
Production:   gunicorn -b 0.0.0.0:8050 --workers 4 app:server
"""
import sys
import os

# Make src/ importable without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import dash_bootstrap_components as dbc
from dash import Dash
from dashboard import create_layout, register_callbacks

# ── Create app ────────────────────────────────────────────────
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="E-Commerce Product Intelligence",
    suppress_callback_exceptions=True,
)

# Expose Flask for Gunicorn
server = app.server

# ── Set layout ────────────────────────────────────────────────
app.layout = create_layout()

# ── Register callbacks ────────────────────────────────────────
register_callbacks(app)

# ── Run ───────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        debug=False,
        host="127.0.0.1",
        port=int(os.environ.get("PORT", 8050)),
        use_reloader=False,
    )