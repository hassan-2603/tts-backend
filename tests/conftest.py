"""Shared pytest fixtures for test isolation (never weakens production security/rate limits)."""
import pytest


@pytest.fixture(autouse=True)
def _isolate_login_rate_limiter():
    """Clear in-memory /api/login rate-limit buckets between tests so sequential test modules
    do not hit HTTP 429 from earlier tests' login calls."""
    try:
        import app.main as main_mod
        main_mod._login_rate_limits.clear()
    except Exception:
        pass
    yield
    try:
        import app.main as main_mod
        main_mod._login_rate_limits.clear()
    except Exception:
        pass
