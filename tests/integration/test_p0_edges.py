"""Failure boundaries in real native replication; only owned fixture databases."""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from test_ddl_expanded import command
from test_lifecycle import migration as migration_fixture
from test_lifecycle import operate, ready
from test_p0_recovery import recover, started, unavailable

from pg_logical_repl import ddl, emergency, integrity, monitoring, plans, recovery, resync
from pg_logical_repl.errors import PreconditionError
from pg_logical_repl.security import identifier as qi
from pg_logical_repl.service import Service

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def test_resolve_skip_does_not_hide_missing_transaction_rows(migration):
    service, sc, tc = await started(migration)
    await tc.execute("INSERT INTO app VALUES(4,40)")
    async with sc.transaction():
        xid = int(await sc.fetchval("SELECT txid_current()::text"))
        await sc.execute("INSERT INTO app VALUES(4,40),(5,50)")
    for _ in range(200):
        if await tc.fetchval(
            "SELECT apply_error_count>0 FROM pg_stat_subscription_stats WHERE subid=$1",
            service.store.read()["subscription_oid"],
        ):
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail("Expected a real duplicate-key apply error")
    await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
    await recovery.wait_stopped(service, tc)
    for _ in range(100):
        if not await sc.fetchval(
            "SELECT active FROM pg_replication_slots WHERE slot_name=$1", service.config.subscription
        ):
            break
        await asyncio.sleep(0.05)
    # BEGIN carries the same finish LSN reported by the apply error context.
    # Fetch only BEGIN metadata, never tuple payload, and do not advance the slot.
    records = await sc.fetch(
        """SELECT data FROM pg_logical_slot_peek_binary_changes($1,NULL,NULL,
        'proto_version','1','publication_names',$2) WHERE get_byte(data,0)=66""",
        service.config.subscription,
        service.config.publication,
    )
    begin = next(bytes(r["data"]) for r in records if int.from_bytes(r["data"][17:21], "big") == xid % 2**32)
    final = int.from_bytes(begin[1:9], "big")
    lsn = f"{final >> 32:X}/{final & 0xFFFFFFFF:X}"
    await service.block("PERSISTENT_APPLY_ERROR_COUNT")
    with pytest.raises(PreconditionError, match="validation"):
        await recover(service, "resolve", action="skip", lsn=lsn)
    assert service.store.read()["state"] == "BLOCKED"
    assert service.store.read()["interventions"][0]["lsn"] == lsn
    assert await tc.fetchval(
        "SELECT subskiplsn='0/0'::pg_lsn FROM pg_subscription WHERE subname=$1", service.config.subscription
    )
    await tc.execute("INSERT INTO app VALUES(5,50)")
    assert (await recover(service, "resolve", action="applied-manually"))["state"] == "REPLICATING"


async def test_journal_pruning_recovers_after_committed_source_delete(migration):
    service, sc, tc = migration
    service.config.raw["ddl"]["mode"] = "captured"
    service, sc, tc = await started(migration)
    await command(service, sc, tc, "ALTER TABLE public.app ADD COLUMN note text")
    await sc.execute("UPDATE lc.ddl_log SET captured_at=clock_timestamp()-interval '2 hours'")
    await service.barrier(sc, tc)
    watermark = await sc.fetchval("SELECT max(id) FROM lc.ddl_log")
    assert watermark

    class LoseDeleteReply:
        def __getattr__(self, name):
            return getattr(sc, name)

        async def execute(self, sql, *args):
            result = await sc.execute(sql, *args)
            if sql.startswith("DELETE FROM lc.ddl_log"):
                raise TimeoutError("committed journal prune response was lost")
            return result

    with pytest.raises(TimeoutError):
        await monitoring.prune_journal(service, LoseDeleteReply(), tc)
    assert service.store.read()["journal_prune_pending"] == watermark
    assert await sc.fetchval("SELECT count(*) FROM lc.ddl_log") == 0
    resumed = Service(service.config)
    await monitoring.prune_journal(resumed, sc, tc)
    assert await tc.fetchval("SELECT count(*) FROM lc.ddl_applied") == 0
    assert (await integrity.require(resumed, sc, tc))["passed"]
    await command(resumed, sc, tc, "ALTER TABLE public.app ADD COLUMN more text")
    assert (await integrity.require(resumed, sc, tc))["passed"]


async def test_pause_deadline_and_sequence_schedule(migration):
    service, sc, tc = migration
    await sc.execute("CREATE SEQUENCE public.ticket")
    service, sc, tc = await started(migration)
    await sc.execute("SELECT setval('ticket',12345,false)")
    service.config.raw["sequences"] = dict(presync_interval_seconds=0)
    await service.run_once()
    assert await tc.fetchval("SELECT last_value FROM ticket") != 12345
    service.config.raw["sequences"]["presync_interval_seconds"] = 1
    state = service.store.read()
    state["sequences_presynced_at"] = None
    service.store.save(state)
    await service.run_once()
    assert dict(await tc.fetchrow("SELECT last_value,is_called FROM ticket")) == dict(
        last_value=12345, is_called=False
    )
    await recover(service, "pause", max_duration=10, wal_rate=1024)
    state = service.store.read()
    state["pause"]["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    service.store.save(state)
    status = await service.run_once()
    assert status["state"] == "PAUSED" and "PAUSE_DEADLINE_EXCEEDED" in status["health"]["advisories"]
    assert "lc_pause_remaining_seconds" in (service.config.path.parent / "metrics.prom").read_text()
    await recover(service)


async def test_deferred_cleanup_recovers_lost_slot_drop_reply(migration, monkeypatch):
    service, sc, tc = await started(migration)
    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", unavailable())
        plan = await service.plan("abort", extra=dict(source_unavailable=True, reason="source lost"))
        await service.abort(plan["path"], plan["plan_hash"])
    plan = await service.plan("cleanup", extra=dict(reason="source restored"))
    original_connect = service.source.connect

    class LoseSlotDropReply:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        async def fetchval(self, sql, *args):
            result = await self.connection.fetchval(sql, *args)
            if "pg_drop_replication_slot" in sql:
                raise TimeoutError("lost slot deletion reply")
            return result

    @asynccontextmanager
    async def source(**kwargs):
        async with original_connect(**kwargs) as connection:
            yield LoseSlotDropReply(connection)

    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", source)
        with pytest.raises(TimeoutError):
            await emergency.cleanup(service, plan["path"], plan["plan_hash"])
    assert (await emergency.cleanup(service, plan["path"], plan["plan_hash"]))["state"] == "ABORTED"


async def test_abort_interrupted_resync_preserves_foreign_key(migration, monkeypatch):
    service, sc, tc = migration
    await sc.execute(
        "CREATE TABLE parent(id int PRIMARY KEY); CREATE TABLE child(id int PRIMARY KEY,p int REFERENCES parent(id)); INSERT INTO parent VALUES(1); INSERT INTO child VALUES(1,1)"
    )
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    plan = await service.plan("resync-table", extra=dict(table="public.parent", reason="repair"))
    original_connect = service.source.connect

    class FailAdd:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        async def execute(self, sql, *args):
            if sql.startswith("ALTER PUBLICATION") and " ADD TABLE " in sql:
                raise TimeoutError("interrupted after truncation")
            return await self.connection.execute(sql, *args)

    @asynccontextmanager
    async def source(**kwargs):
        async with original_connect(**kwargs) as connection:
            yield FailAdd(connection)

    with monkeypatch.context() as patch:
        patch.setattr(service.source, "connect", source)
        with pytest.raises(TimeoutError):
            await resync.execute(service, plan["path"], plan["plan_hash"])
    assert await tc.fetchval("SELECT count(*) FROM parent") == 0
    result = await operate(service, "abort")
    assert result["state"] == "ABORTED"
    assert await tc.fetchval("SELECT convalidated=false FROM pg_constraint WHERE conname='child_p_fkey'")
    assert result["resync_abort_constraints"][0]["constraint"] == "child_p_fkey"


async def test_managed_ddl_retry_after_journal_pruning_and_plan_expiration(migration, monkeypatch):
    service, sc, tc = migration
    service.config.raw["ddl"]["mode"] = "managed_only"
    service, sc, tc = await started(migration)
    statement = "ALTER TABLE public.app ADD COLUMN note text"
    sql_file = service.store.directory / "operation.sql"
    sql_file.write_text(statement)
    plan = await service.plan("ddl-exec", extra=ddl.classify(statement))
    original_barrier = service.barrier

    async def interrupted(*args, **kwargs):
        raise TimeoutError("source DDL committed, controller interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(service, "barrier", interrupted)
        with pytest.raises(TimeoutError):
            await ddl.execute(service, plan["path"], plan["plan_hash"], sql_file)
    await original_barrier(sc, tc)
    await sc.execute("UPDATE lc.ddl_log SET captured_at=clock_timestamp()-interval '2 hours'")
    await original_barrier(sc, tc)
    await monitoring.prune_journal(service, sc, tc)
    assert await sc.fetchval("SELECT count(*) FROM lc.ddl_log") == 0

    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + timedelta(days=2)

    with monkeypatch.context() as patch:
        patch.setattr(plans, "datetime", Later)
        result = await ddl.execute(service, plan["path"], plan["plan_hash"], sql_file)
    assert result["applied"] and result["archived"]
    assert (await integrity.require(service, sc, tc))["passed"]


@pytest.mark.parametrize(
    "change,expected",
    [
        ("PASSWORD 'fixture-verifier'", "TARGET_ROLE_DRIFT"),
        ("membership", "TARGET_ROLE_DRIFT"),
        ("schema_acl", "SCHEMA_DRIFT"),
    ],
)
async def test_password_membership_and_schema_acl_drift(migration, change, expected):
    service, sc, tc = await started(migration, role=True)
    role = qi(service.config.migration_id + "_app")
    if change == "membership":
        await tc.execute("GRANT pg_read_all_data TO " + role)
    elif change == "schema_acl":
        await tc.execute("GRANT USAGE ON SCHEMA public TO " + role)
    else:
        await tc.execute("ALTER ROLE " + role + " " + change)
    status = await service.run_once()
    assert status["state"] == "BLOCKED" and expected in status["blocked_details"]
    assert "fixture-verifier" not in (service.store.directory / "reports/integrity.json").read_text()


async def test_missing_service_relation_has_readable_blocked_status(migration):
    service, sc, tc = await started(migration)
    await tc.execute("ALTER TABLE lc.ddl_queue RENAME TO damaged_queue")
    result = await service.run_once()
    assert result["state"] == "BLOCKED" and result["blocked_reason"] == "DATABASE_SCHEMA_ERROR"
    assert result["subscription"]["subenabled"]
    assert (await service.status())["state"] == "BLOCKED"
    await tc.execute("ALTER TABLE lc.damaged_queue RENAME TO ddl_queue")
    assert (await recover(service))["state"] == "REPLICATING"
