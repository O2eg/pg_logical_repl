"""Serialize suites sharing cluster-wide roles on an owned PostgreSQL stand."""

import fcntl
import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def owned_stand_lock():
    if os.getenv("LC_INTEGRATION") != "1":
        yield
        return
    major = int(os.getenv("LC_TEST_MAJOR", "18"))
    root = Path(__file__).resolve().parents[2] / ".runtime"
    root.mkdir(exist_ok=True, mode=0o700)
    with (root / f"pytest-pg{major}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pytest.fail(f"Another integration suite owns the PostgreSQL {major} stand")
        yield
