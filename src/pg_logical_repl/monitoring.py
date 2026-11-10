"""Bounded daemon work, persisted validation cursors and operator metrics."""

import asyncio
import json
import uuid
from datetime import UTC, datetime

import asyncpg

from . import health, integrity, validation
from .catalog import fingerprint, inventory
from .errors import PreconditionError
from .security import qualified as qn
from .state import now, write_private


async def paused_tick(service, sc, tc):
    observed = await health.observe(service, sc, tc, allow_disabled=True)
    state = service.store.read()
    expires = datetime.fromisoformat(state["pause"]["expires_at"])
    observed["pause_remaining_seconds"] = (expires - datetime.now(UTC)).total_seconds()
    if observed["pause_remaining_seconds"] <= 0:
        observed["advisories"].append("PAUSE_DEADLINE_EXCEEDED")
        if not state["pause"].get("deadline_reported"):
            service.store.event("health_advisory", code="PAUSE_DEADLINE_EXCEEDED")
            state["pause"]["deadline_reported"] = True
    state["health"] = observed
    state["heartbeat"] = now()
    service.store.save(state)
    if observed["blockers"]:
        await service.block("HEALTH_BLOCKED", details=", ".join(observed["blockers"]))


def due(timestamp, interval):
    return (
        not timestamp or (datetime.now(UTC) - datetime.fromisoformat(timestamp)).total_seconds() >= interval
    )


async def ddl_epoch(sc):
    return await sc.fetchval("SELECT COALESCE(max(id),0) FROM lc.ddl_log")


def invalidate_chunk(service):
    state = service.store.read()
    state.pop("validation_cursor", None)
    service.store.save(state)
    service.store.event("validation_schema_changed")


async def validation_chunk(service, sc, tc):
    epoch = await ddl_epoch(sc)
    try:
        await _validation_chunk(service, sc, tc, epoch)
    except (
        asyncpg.UndefinedColumnError,
        asyncpg.UndefinedTableError,
        asyncpg.UndefinedObjectError,
        PreconditionError,
    ) as exc:
        if isinstance(exc, PreconditionError) and exc.code != "VALIDATION_MISMATCH":
            raise
        # A catalog list can become stale before either aggregate query, or
        # between the source digest and target apply. Only a newly committed
        # captured DDL transaction justifies discarding that chunk. Target-only
        # damage and other SQL errors must retain their normal blocking behavior.
        if await ddl_epoch(sc) == epoch:
            raise
        invalidate_chunk(service)
    else:
        if await ddl_epoch(sc) != epoch:
            invalidate_chunk(service)


async def _validation_chunk(service, sc, tc, epoch):
    inv = await inventory(sc)
    signature = fingerprint(inv)
    relations = [
        r
        for r in inv["relations"]
        if r["kind"] in ("r", "p")
        and r["persistence"] == "p"
        and not r["extension"]
        and not r["is_partition"]
    ]
    state = service.store.read()
    cursor = state.get("validation_cursor")
    if (
        not cursor
        or cursor["schema"] != signature
        or cursor.get("ddl_epoch") != epoch
        or cursor["table"] >= len(relations)
    ):
        cursor = dict(
            schema=signature,
            ddl_epoch=epoch,
            table=0,
            lower=None,
            cycle=uuid.uuid4().hex,
            started_at=now(),
            chunks=0,
            unstable=0,
        )
    if not relations:
        state["validation_last_pass"] = now()
        service.store.save(state)
        return
    relation = relations[cursor["table"]]
    name = qn(relation["schema"], relation["name"])
    keys = await validation.primary_key(sc, name)
    columns = [c["attname"] for c in relation["columns"]]
    lower = cursor["lower"]
    pending = cursor.get("pending")
    if pending is None:
        upper = None
        async with validation.snapshot(sc):
            if keys:
                query, params = validation.boundary_query(
                    name, keys, lower, service.config.get("validation", "chunk_rows", 10000)
                )
                upper = await sc.fetchval(query, *params)
            source = await validation.digest_range(sc, name, columns, keys, lower, upper, "checksums")
        pending = dict(
            upper=upper, source=source, lsn=await sc.fetchval("SELECT pg_current_wal_lsn()::text"), attempts=0
        )
        cursor["pending"] = pending
    # Waiting for apply is a persisted phase, not a blocking wait in the daemon.
    state["validation_cursor"] = cursor
    service.store.save(state)
    reached = await sc.fetchval(
        "SELECT confirmed_flush_lsn >= $2::text::pg_lsn FROM pg_replication_slots WHERE slot_name=$1",
        service.config.subscription,
        pending["lsn"],
    )
    if not reached:
        return
    upper, source = pending["upper"], pending["source"]
    async with validation.snapshot(tc):
        target = await validation.digest_range(tc, name, columns, keys, lower, upper, "checksums")
    status = "passed" if source == target else "mismatch"
    if status != "passed":
        async with validation.snapshot(sc):
            after = await validation.digest_range(sc, name, columns, keys, lower, upper, "checksums")
        if after != source:
            status = "unstable"
        if pending["attempts"] < service.config.get("validation", "max_retries", 3):
            pending.update(
                source=after,
                lsn=await sc.fetchval("SELECT pg_current_wal_lsn()::text"),
                attempts=pending["attempts"] + 1,
            )
            service.store.save(state)
            return
    result = dict(
        source=source,
        target=target,
        status=status,
        attempts=pending["attempts"] + 1,
        source_lsn=pending["lsn"],
    )
    cursor.pop("pending", None)
    # Do not commit a cursor across a concurrently committed DDL transaction.
    if fingerprint(await inventory(sc)) != signature or await ddl_epoch(sc) != epoch:
        state.pop("validation_cursor", None)
        service.store.save(state)
        return
    cursor["chunks"] += 1
    cursor["unstable"] += result["status"] == "unstable"
    cursor["lower"] = upper
    if upper is None:
        cursor["table"] += 1
    cursor["checked_at"] = now()
    state["validation_cursor"] = cursor
    state["validation_last_chunk"] = dict(table=name, lower=lower, upper=upper, **result)
    if cursor["table"] == len(relations) and not cursor["unstable"] and result["status"] == "passed":
        state["validation_last_pass"] = now()
    service.store.save(state)
    pause = service.config.get("validation", "pause_between_chunks_ms", 50) / 1000
    if pause:
        await asyncio.sleep(pause)
    if result["status"] == "mismatch":
        raise PreconditionError("VALIDATION_MISMATCH", "Confirmed data mismatch in " + name)


async def tick(service, sc, tc):
    await prune_journal(service, sc, tc)
    state = service.store.read()
    interval = service.config.get("daemon", "drift_interval_seconds", 900)
    if due((state.get("integrity") or {}).get("checked_at"), interval):
        # Drain the native stream before comparing asynchronous catalogs.
        async with integrity.stable_catalogs(service, sc, tc):
            result = await integrity.inspect(service, sc, tc, barrier_passed=True)
        state = service.store.read()
        state["integrity"] = result
        service.store.save(state)
        if result["blockers"]:
            raise PreconditionError("INTEGRITY_BLOCKED", ", ".join(result["blockers"]))
    pending = await tc.fetchval("""SELECT EXISTS(SELECT FROM lc.ddl_queue WHERE status NOT IN ('done','skipped'))
        OR EXISTS(SELECT FROM lc.table_sync WHERE state NOT IN ('ready','removed'))""")
    if pending:
        return
    presync = service.config.get("sequences", "presync_interval_seconds", 300)
    state = service.store.read()
    if presync and due(state.get("sequences_presynced_at"), presync):
        await validation.sequences(sc, tc, apply=True)
        state["sequences_presynced_at"] = now()
        service.store.save(state)
    if service.config.get("validation", "online", True):
        await validation_chunk(service, sc, tc)


async def prune_journal(service, sc, tc):
    state = service.store.read()
    watermark = state.get("journal_prune_pending")
    if watermark is None:
        if not due(state.get("journal_pruned_at"), 3600):
            return
        watermark = await sc.fetchval(
            "SELECT max(id) FROM lc.ddl_log WHERE captured_at<clock_timestamp()-interval '1 hour'"
        )
        if watermark is None:
            return
        async with integrity.stable_catalogs(service, sc, tc):
            checked = await integrity.inspect(service, sc, tc, barrier_passed=True)
            if not checked["passed"] or not checked["settled"]:
                return
        state["journal_prune_pending"] = watermark
        service.store.save(state)
    # The durable intent makes loss of any DELETE/COMMIT response recoverable.
    # Keep compact managed-operation receipts for retries after journal retention.
    await sc.execute(
        """INSERT INTO lc.operations(name,details)
        SELECT 'ddl_archive:'||operation_key,jsonb_build_object('id',id,'statement_sha256',statement_sha256)
        FROM lc.ddl_log WHERE id<=$1 AND operation_key IS NOT NULL ON CONFLICT(name) DO NOTHING""",
        watermark,
    )
    await sc.execute("DELETE FROM lc.ddl_log WHERE id<=$1", watermark)
    await service.barrier(sc, tc)
    async with tc.transaction():
        await tc.execute("DELETE FROM lc.ddl_applied WHERE id<=$1", watermark)
        await tc.execute(
            "DELETE FROM lc.ddl_queue WHERE ddl_log_id<=$1 AND status IN ('done','skipped')", watermark
        )
        await tc.execute(
            "DELETE FROM lc.table_sync WHERE ddl_log_id<=$1 AND state IN ('ready','removed')", watermark
        )
    state = service.store.read()
    await sc.execute("DELETE FROM lc.barrier WHERE created_at<clock_timestamp()-interval '1 hour'")
    state["journal_prune_pending"] = None
    state["journal_pruned_at"] = now()
    service.store.save(state)
    service.store.event("journal_pruned", through_id=watermark)


def metrics(service):
    path = service.config.get("daemon", "metrics_textfile")
    if not path:
        return
    state = service.store.read()
    health = state.get("health", {})
    slot = health.get("slot") or {}
    values = dict(
        lc_up=1,
        lc_slot_retained_bytes=slot.get("retained_bytes"),
        lc_lag_bytes=slot.get("lag_bytes"),
        lc_lag_seconds=health.get("lag_seconds"),
        lc_ddl_queue_oldest_age_seconds=health.get("queue_age_seconds"),
        lc_pending_refresh_age_seconds=health.get("refresh_age_seconds"),
        lc_blocked=int(state["state"] == "BLOCKED"),
        lc_pause_remaining_seconds=health.get("pause_remaining_seconds"),
    )
    content = "".join(
        f"# TYPE {key} gauge\n{key} {value}\n" for key, value in values.items() if value is not None
    )
    content += "lc_state{state=" + json.dumps(state["state"]) + "} 1\n"
    content += f"lc_heartbeat_timestamp_seconds {datetime.now(UTC).timestamp()}\n"
    write_private(service.config.path.parent / path, content)
