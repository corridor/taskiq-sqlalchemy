"""tests/conftest.py

Top-level conftest — kept intentionally minimal.
Per-component fixtures live in their own sub-package conftest files
(e.g. tests/result_backend/conftest.py).
"""

import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"
