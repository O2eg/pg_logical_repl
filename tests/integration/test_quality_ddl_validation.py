"""Deterministic reproductions of DDL racing background chunk validation."""

import asyncio
import os

import pytest
from support.oracle import compare
from test_lifecycle import migration as migration_fixture
from test_lifecycle import operate, ready

from pg_logical_repl import monitoring, recovery, validation
from pg_logical_repl.security import identifier

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.quality,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def setup(migration):
    service, sc, tc = migration
    service.config.raw["ddl"]["mode"] = "captured"
    service.config.raw["validation"] = dict(chunk_rows=2, max_retries=0)
    await sc.execute(
        "CREATE TABLE public.app(id int PRIMARY KEY,n int); INSERT INTO app VALUES(1,10),(2,20),(3,30)"
    )
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await reset(service)
    return service, sc, tc


async def reset(service):
    state = service.store.read()
    state.pop("validation_cursor", None)
    service.store.save(state)


@pytest.mark.parametrize("phase", ["source_digest", "target_digest"])
@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE public.app RENAME COLUMN n TO amount",
        "ALTER TABLE public.app DROP COLUMN n",
        "DROP TABLE public.app",
    ],
)
async def test_captured_ddl_between_catalog_and_digest_restarts_cursor(
    migration, monkeypatch, phase, statement
):
    service, sc, tc = await setup(migration)
    original = validation.digest_range
    original_keys = validation.primary_key
    fired = False

    async def alter():
        nonlocal fired
        fired = True
        async with service.source.connect() as writer, service.target.connect() as reader:
            await writer.execute(statement)
            await service.barrier(writer, reader)

    async def keys(c, *args, **kwargs):
        result = await original_keys(c, *args, **kwargs)
        if not fired and phase == "source_digest" and c is sc:
            await alter()
        return result

    async def inject(c, *args, **kwargs):
        if not fired and phase == "target_digest" and c is tc:
            await alter()
        return await original(c, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(validation, "primary_key", keys)
        patch.setattr(validation, "digest_range", inject)
        await monitoring.validation_chunk(service, sc, tc)
    assert fired
    assert service.store.read().get("validation_cursor") is None
    assert (await service.run_once())["state"] == "REPLICATING"
    await service.drain_ddl(sc, tc)
    assert (await compare(sc, tc, executor="lc_ddl_executor_" + service.config.migration_id))["passed"]


async def test_target_only_missing_column_still_blocks(migration):
    service, sc, tc = await setup(migration)
    await tc.execute("ALTER TABLE public.app DROP COLUMN n")
    for _ in range(50):
        state = await service.run_once()
        if state["state"] == "BLOCKED":
            break
        await asyncio.sleep(0.05)
    assert state["state"] == "BLOCKED"
    assert state["blocked_reason"] in {"DATABASE_SCHEMA_ERROR", "INTEGRITY_BLOCKED"}


async def test_same_schema_after_ddl_invalidates_pending_chunk(migration):
    service, sc, tc = await setup(migration)
    await tc.execute("ALTER SUBSCRIPTION " + identifier(service.config.subscription) + " DISABLE")
    await recovery.wait_stopped(service, tc)
    await sc.execute("UPDATE app SET n=n+1 WHERE id=1")
    await monitoring.validation_chunk(service, sc, tc)
    before = service.store.read()["validation_cursor"]
    assert before["pending"]
    async with sc.transaction():
        await sc.execute("ALTER TABLE public.app RENAME COLUMN n TO amount")
        await sc.execute("UPDATE public.app SET amount=amount+1")
        await sc.execute("ALTER TABLE public.app RENAME COLUMN amount TO n")
    await tc.execute("ALTER SUBSCRIPTION " + identifier(service.config.subscription) + " ENABLE")
    await service.barrier(sc, tc)
    await monitoring.validation_chunk(service, sc, tc)
    after = service.store.read()["validation_cursor"]
    assert after["schema"] == before["schema"]
    assert after["cycle"] != before["cycle"], "DDL epoch must invalidate even an identical schema fingerprint"
    assert (await compare(sc, tc, executor="lc_ddl_executor_" + service.config.migration_id))["passed"]
