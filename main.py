"""Root entrypoint forwarding to the production modular application in backend/app."""

import os
import sys

# Ensure backend directory is in python path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_path = os.path.join(current_dir, "backend")
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

from app.main import app  # noqa: F401
