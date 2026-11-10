"""The same installed-CLI scenario has a bounded smoke and an opt-in soak profile."""

import os
from pathlib import Path

import pytest
from support.network import routed
from support.workload import exercise
from test_lifecycle import MAJOR
from test_lifecycle import migration as migration_fixture

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.quality,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def test_cli_continuous_dml_ddl_recovery_and_cutover(migration):
    service, sc, tc = migration
    async with routed(service, MAJOR) as (service, proxies):
        result = await exercise(
            service,
            sc,
            tc,
            proxies,
            duration=30,
            rows=1000,
            writers=2,
            directory=service.config.path.parent / "workload",
        )
        assert result["passed"]


@pytest.mark.soak
@pytest.mark.skipif(os.getenv("LC_SOAK") != "1", reason="Explicit LC_SOAK=1 required")
async def test_cli_soak(migration):
    service, sc, tc = migration
    duration = int(os.getenv("LC_SOAK_SECONDS", "1800"))
    rows = int(os.getenv("LC_SOAK_ROWS", "10000"))
    writers = int(os.getenv("LC_SOAK_WRITERS", "4"))
    directory = Path(os.getenv("LC_QUALITY_ARTIFACTS", str(service.config.path.parent))) / "soak"
    async with routed(service, MAJOR) as (service, proxies):
        result = await exercise(
            service, sc, tc, proxies, duration=duration, rows=rows, writers=writers, directory=directory
        )
        assert result["duration_seconds"] >= duration
