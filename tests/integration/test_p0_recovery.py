import asyncio
import json
import os
from contextlib import asynccontextmanager

import pytest
from test_lifecycle import migration as migration_fixture
from test_lifecycle import operate, ready

from pg_logical_repl import emergency, monitoring, recovery, resync
from pg_logical_repl.errors import ConnectorError, OwnershipError, PreconditionError
from pg_logical_repl.security import identifier as qi
from pg_logical_repl.service import Service
from pg_logical_repl.state import now, write_private

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def started(migration, *, role=False):
    service, sc, tc = migration
    service.config.raw["daemon"] = dict(drift_interval_seconds=0, metrics_textfile="metrics.prom")
    service.config.raw["validation"] = dict(chunk_rows=2, max_retries=0)
    if role:
        await sc.execute("CREATE ROLE " + qi(service.config.migration_id + "_app") + " LOGIN")
    await sc.execute(
        "CREATE TABLE public.app(id int PRIMARY KEY,n int); INSERT INTO public.app VALUES(1,10),(2,20),(3,30)"
    )
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    return service, sc, tc


async def recover(service, kind="resume", **extra):
    plan = await service.plan(kind, extra=dict(reason="verified repair", **extra))
    return await recovery.execute(service, kind, plan["path"], plan["plan_hash"])


def unavailable():
    @asynccontextmanager
    async def connect(**kwargs):
        raise ConnectorError("DATABASE_UNAVAILABLE", "injected source outage")
        yield

    return connect


async def test_role_drift_blocks_and_resume_rechecks(migration):
    service, sc, tc = await started(migration, role=True)
    role = qi(service.config.migration_id + "_app")
    await tc.execute("ALTER ROLE " + role + " NOLOGIN")
    status = await service.run_once()
    assert status["state"] == "BLOCKED"
    assert "TARGET_ROLE_DRIFT" in status["blocked_details"]
    with pytest.raises(PreconditionError, match="ROLE"):
        await recover(service)
    await tc.execute("ALTER ROLE " + role + " LOGIN")
    assert (await recover(service))["state"] == "REPLICATING"
    await sc.execute("ALTER ROLE " + role + " NOLOGIN")
    await tc.execute("ALTER ROLE " + role + " NOLOGIN")
    status = await service.run_once()
    assert status["state"] == "BLOCKED" and "SOURCE_ROLE_DRIFT" in status["blocked_details"]


async def test_background_validation_and_manual_resolution(migration):
    service, sc, tc = await started(migration)
    for _ in range(100):
        if service.store.read()["validation_cursor"]["chunks"] == 1:
            break
        await service.run_once()
        await asyncio.sleep(0.05)
    assert service.store.read()["validation_cursor"]["chunks"] == 1
    await tc.execute("UPDATE app SET n=-1 WHERE id=3")
    for _ in range(100):
        status = await service.run_once()
        if status["state"] == "BLOCKED":
            break
        await asyncio.sleep(0.05)
    assert status["state"] == "BLOCKED" and status["blocked_reason"] == "VALIDATION_MISMATCH"
    with pytest.raises(PreconditionError, match="validation"):
        await recover(service)
    await tc.execute("UPDATE app SET n=30 WHERE id=3")
    assert (await recover(service, "resolve", action="applied-manually"))["state"] == "REPLICATING"
    assert service.store.read()["interventions"][0]["action"] == "applied-manually"
    assert "lc_blocked 1" in (service.config.path.parent / "metrics.prom").read_text()


async def test_pause_budget_and_resume(migration):
    service, sc, tc = await started(migration)
    with pytest.raises(PreconditionError, match="safe limit"):
        await recover(service, "pause", max_duration=1000000, wal_rate=1000000000)
    assert (await recover(service, "pause", max_duration=10, wal_rate=1024))["state"] == "PAUSED"
    assert not await tc.fetchval(
        "SELECT subenabled FROM pg_subscription WHERE subname=$1", service.config.subscription
    )
    await sc.execute("INSERT INTO app VALUES(4,40)")
    assert (await recover(service))["state"] == "REPLICATING"
    assert await tc.fetchval("SELECT n FROM app WHERE id=4") == 40


async def test_cutover_checks_roles_and_analyzes(migration):
    service, sc, tc = migration
    service.config.raw["cutover"] = dict(analyze="all")
    service, sc, tc = await started(migration, role=True)
    fence = service.config.path.parent / "fence.json"
    write_private(
        fence,
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=service.store.read()["source_identity"],
                created_at=now(),
                responsible="test",
                external_fence="owned idle test database",
            )
        ),
    )
    role = qi(service.config.migration_id + "_app")
    await tc.execute("ALTER ROLE " + role + " NOLOGIN")
    with pytest.raises(PreconditionError, match="ROLE"):
        await operate(service, "cutover-finalize", write_fence=str(fence))
    await tc.execute("ALTER ROLE " + role + " LOGIN")
    result = await operate(service, "cutover-finalize", write_fence=str(fence))
    assert result["analyze"] == "all"


async def test_offline_abort_and_deferred_cleanup(migration, monkeypatch):
    service, sc, tc = await started(migration)
    with pytest.raises(PreconditionError, match="ordinary"):
        await service.plan("abort", extra=dict(source_unavailable=True, reason="test"))
    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", unavailable())
        plan = await service.plan("abort", extra=dict(source_unavailable=True, reason="source unavailable"))
        result = await service.abort(plan["path"], plan["plan_hash"])
        assert result["state"] == "ABORT_CLEANUP_PENDING"
        assert result["availability"]["source"] is False
        assert not await tc.fetchval(
            "SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", service.config.subscription
        )
        assert await sc.fetchval(
            "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", service.config.subscription
        )
        assert (await service.abort(plan["path"], plan["plan_hash"]))["state"] == "ABORT_CLEANUP_PENDING"
    plan = await service.plan("cleanup", extra=dict(reason="source restored"))
    result = await emergency.cleanup(service, plan["path"], plan["plan_hash"])
    assert result["state"] == "ABORTED"
    assert await tc.fetchval("SELECT count(*) FROM app") == 3
    assert not await sc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", service.config.subscription
    )


async def test_offline_cutover_requires_unchanged_finalized_target(migration, monkeypatch):
    service, sc, tc = await started(migration)
    fence = service.config.path.parent / "fence.json"
    write_private(
        fence,
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=service.store.read()["source_identity"],
                created_at=now(),
                responsible="test",
                external_fence="idle fixture",
            )
        ),
    )
    await operate(service, "cutover-finalize", write_fence=str(fence))
    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", unavailable())
        await tc.execute("UPDATE app SET n=99 WHERE id=1")
        with pytest.raises(PreconditionError, match="data changed"):
            await service.plan("cutover-commit", extra=dict(source_unavailable=True, reason="lost source"))
        await tc.execute("UPDATE app SET n=10 WHERE id=1")
        plan = await service.plan("cutover-commit", extra=dict(source_unavailable=True, reason="lost source"))
        result = await service.cutover_commit(plan["path"], plan["plan_hash"])
        assert result["state"] == "DETACH_CLEANUP_PENDING"
    plan = await service.plan("cleanup", extra=dict(reason="source restored"))
    assert (await emergency.cleanup(service, plan["path"], plan["plan_hash"]))["state"] == "DETACHED"


async def test_deferred_cleanup_rejects_replaced_subscription_receipt(migration, monkeypatch):
    service, sc, tc = await started(migration)
    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", unavailable())
        plan = await service.plan("abort", extra=dict(source_unavailable=True, reason="source lost"))
        await service.abort(plan["path"], plan["plan_hash"])
    await sc.execute("DELETE FROM lc.operations WHERE name='subscription'")
    plan = await service.plan("cleanup", extra=dict(reason="source restored"))
    with pytest.raises(OwnershipError, match="receipt"):
        await emergency.cleanup(service, plan["path"], plan["plan_hash"])
    # Restore the test's own receipt to let the fixture remove its slot safely.
    await sc.execute(
        "INSERT INTO lc.operations(name,details) VALUES('subscription',$1::jsonb)",
        json.dumps(dict(oid=service.store.read()["subscription_oid"])),
    )
    plan = await service.plan("cleanup", extra=dict(reason="receipt restored"))
    await emergency.cleanup(service, plan["path"], plan["plan_hash"])


async def test_resync_restores_foreign_keys_and_data(migration):
    service, sc, tc = migration
    service.config.raw["validation"] = dict(online=False)
    await sc.execute(
        "CREATE TABLE parent(id int PRIMARY KEY,n int); CREATE TABLE child(id int PRIMARY KEY,p int REFERENCES parent(id)); INSERT INTO parent VALUES(1,10),(2,20); INSERT INTO child VALUES(1,1)"
    )
    await sc.execute("COMMENT ON CONSTRAINT child_p_fkey ON child IS 'preserve this comment'")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await tc.execute("UPDATE parent SET n=-1 WHERE id=1")
    await service.block("VALIDATION_MISMATCH")
    plan = await service.plan("resync-table", extra=dict(table="public.parent", reason="repair fixture"))
    result = await resync.execute(service, plan["path"], plan["plan_hash"])
    assert result["passed"] and service.store.read()["state"] == "REPLICATING"
    assert await tc.fetchval("SELECT n FROM parent WHERE id=1") == 10
    assert await tc.fetchval("SELECT convalidated FROM pg_constraint WHERE conname='child_p_fkey'")


async def test_runtime_settings_resource_drift_and_lsn_resume(migration):
    service, sc, tc = await started(migration)
    await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
    await recovery.wait_stopped(service, tc)
    await sc.execute("INSERT INTO app VALUES(4,40)")
    state = service.store.read()
    state["validation_cursor"] = None
    service.store.save(state)
    await monitoring.validation_chunk(service, sc, tc)
    assert service.store.read()["validation_cursor"]["pending"]["lsn"]
    resumed = Service(service.config)
    await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " ENABLE")
    for _ in range(100):
        await monitoring.validation_chunk(resumed, sc, tc)
        if resumed.store.read()["validation_cursor"]["chunks"]:
            break
        await asyncio.sleep(0.05)
    assert resumed.store.read()["validation_cursor"]["chunks"] == 1
    original = await tc.fetchval("SELECT settings FROM lc.control")
    await tc.execute("UPDATE lc.control SET settings=settings||'{\"tampered\":true}'::jsonb")
    result = await service.run_once()
    assert result["state"] == "BLOCKED" and "TARGET_RESOURCE_DRIFT" in result["blocked_details"]
    await tc.execute("UPDATE lc.control SET settings=$1::jsonb", original)
    assert (await recover(service))["state"] == "REPLICATING"


async def test_offline_abort_recovers_lost_drop_reply(migration, monkeypatch):
    service, sc, tc = await started(migration)
    original_connect = service.target.connect
    lost = False

    class LoseDropReply:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        async def execute(self, sql, *args):
            nonlocal lost
            result = await self.connection.execute(sql, *args)
            if sql.startswith("DROP SUBSCRIPTION") and not lost:
                lost = True
                raise TimeoutError("lost committed DROP response")
            return result

    @asynccontextmanager
    async def target(**kwargs):
        async with original_connect(**kwargs) as connection:
            yield LoseDropReply(connection)

    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", unavailable())
        plan = await service.plan("abort", extra=dict(source_unavailable=True, reason="source lost"))
        patch.setattr(service.target, "connect", target)
        with pytest.raises(TimeoutError):
            await service.abort(plan["path"], plan["plan_hash"])
        assert service.store.read()["state"] == "ABORT_CLEANUP_PENDING"
        assert (await service.abort(plan["path"], plan["plan_hash"]))["state"] == "ABORT_CLEANUP_PENDING"
    plan = await service.plan("cleanup", extra=dict(reason="source restored"))
    await emergency.cleanup(service, plan["path"], plan["plan_hash"])


async def test_resync_retry_uses_original_plan_after_lost_truncate_reply(migration, monkeypatch):
    service, sc, tc = await started(migration)
    await tc.execute("UPDATE app SET n=-1 WHERE id=1")
    plan = await service.plan("resync-table", extra=dict(table="public.app", reason="repair"))
    original_connect = service.source.connect
    lost = False

    class LoseAddReply:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        async def execute(self, sql, *args):
            nonlocal lost
            if sql.startswith("ALTER PUBLICATION") and " ADD TABLE " in sql and not lost:
                lost = True
                raise TimeoutError("after committed target truncate")
            return await self.connection.execute(sql, *args)

    @asynccontextmanager
    async def source(**kwargs):
        async with original_connect(**kwargs) as connection:
            yield LoseAddReply(connection)

    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", source)
        with pytest.raises(TimeoutError):
            await resync.execute(service, plan["path"], plan["plan_hash"])
    assert service.store.read()["state"] == "RESYNCING"
    assert await tc.fetchval("SELECT count(*) FROM app") == 0
    another = await service.plan("resync-table", extra=dict(table="public.app", reason="must use old plan"))
    with pytest.raises(PreconditionError, match="original resync"):
        await resync.execute(service, another["path"], another["plan_hash"])
    assert (await resync.execute(service, plan["path"], plan["plan_hash"]))["passed"]
    assert await tc.fetchval("SELECT count(*) FROM app") == 3
