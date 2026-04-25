#!/usr/bin/env bash
# =============================================================================
# setup_agent.sh — creates a Python virtual environment and installs agent deps
#
# Usage:
#   bash setup_agent.sh          # create venv + install deps
#   source venv/bin/activate     # activate (run this separately)
# =============================================================================
set -euo pipefail

VENV_DIR="venv"

echo "=== CDC Pipeline Agent — Environment Setup ==="
echo ""

# ── Check Python version ─────────────────────────────────────────────────────
PYTHON=$(command -v python3 || command -v python || true)
if [[ -z "$PYTHON" ]]; then
    echo "ERROR: Python 3 not found. Install Python 3.9+ and retry."
    exit 1
fi

PY_VERSION=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Python found: $PYTHON ($PY_VERSION)"

MAJOR=$("$PYTHON" -c "import sys; print(sys.version_info.major)")
MINOR=$("$PYTHON" -c "import sys; print(sys.version_info.minor)")
if [[ "$MAJOR" -lt 3 || ( "$MAJOR" -eq 3 && "$MINOR" -lt 9 ) ]]; then
    echo "ERROR: Python 3.9+ required. Found $PY_VERSION."
    exit 1
fi

# ── Create virtual environment ────────────────────────────────────────────────
if [[ -d "$VENV_DIR" ]]; then
    echo "Virtual environment already exists at ./$VENV_DIR — skipping creation."
else
    echo "Creating virtual environment at ./$VENV_DIR ..."
    "$PYTHON" -m venv "$VENV_DIR"
    echo "  Done."
fi

# ── Install dependencies ──────────────────────────────────────────────────────
echo ""
echo "Installing dependencies from requirements.txt ..."
"$VENV_DIR/bin/pip" install --upgrade pip --quiet
"$VENV_DIR/bin/pip" install -r requirements.txt
echo "  Done."

# ── .env check ───────────────────────────────────────────────────────────────
echo ""
if [[ ! -f ".env" ]]; then
    echo "No .env file found — copying .env.example to .env ..."
    cp .env.example .env
    echo "  Created .env. Open it and set your ANTHROPIC_API_KEY."
else
    echo ".env file already exists."
fi

# ── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "    1. Activate the venv:"
echo "         source venv/bin/activate"
echo ""
echo "    2. Set your API key in .env:"
echo "         ANTHROPIC_API_KEY=sk-ant-..."
echo ""
echo "    3. Start the Docker stack:"
echo "         cd demo && docker-compose up -d"
echo ""
echo "    4. Register the Debezium connector:"
echo "         bash demo/setup.sh"
echo ""
echo "    5. Run the agent:"
echo "         python main.py"
echo "============================================================"
