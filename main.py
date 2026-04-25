#!/usr/bin/env python3
"""
CDC Pipeline Debug Agent — entry point.

Usage:
    python main.py               # local Docker demo (default)
    python main.py --env local   # explicit local mode
"""

import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AI debug agent for the local CDC pipeline demo."
    )
    parser.add_argument(
        "--env",
        choices=["local"],
        default="local",
        help="Target environment (default: local)",
    )
    args = parser.parse_args()

    # Insert the project directory BEFORE importing any agent code so that
    # `import registry` and `from knowledge import SYSTEM_PROMPT` resolve
    # to the correct project module (not a stale cached import).
    project_dir = os.path.join(_ROOT, "projects", "local")
    sys.path.insert(0, project_dir)

    from interfaces.cli import run
    run(env=args.env)


if __name__ == "__main__":
    main()
