"""Sequential SQL migration runner + client migration files.

The runner (`runner.py`) is shared by both the client and the server — each
passes its own migrations directory. See `runner.apply_migrations`.
"""

from __future__ import annotations
