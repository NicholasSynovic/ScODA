"""
Top level db/__init__.py file.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from scoda.db.generic import DB
from scoda.db.results.implementation import Results

RESPONSE_TIMEOUT: int = 600

__all__: list[str] = ["DB", "Results"]
