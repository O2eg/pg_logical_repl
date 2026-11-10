"""Explicit, planned pause and recovery. Recovery never edits user data implicitly."""

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta

from . import ddl_queue, health, integrity, validation
from .errors import PreconditionError
from .security import identifier as qi
from .security import literal as ql
from .state import now

KINDS = {"pause", "resume", "resolve"}


def extra(kind, value):
    value = dict(value or {})
    if not isinstance(value.get("reason"), str) or not value["reason"].strip():
        raise PreconditionError("REASON_REQUIRED", kind + " requires a reason in its plan")
    if kind == "pause":
        for key in ("max_duration", "wal_rate"):
            if type(value.get(key)) is not int or value[key] <= 0:
                raise PreconditionError(
                    "PAUSE_BUDGET", "Positive max_duration and conservative wal_rate are required"
                )
    if kind == "resolve":
        if value.get("action") not in {"skip", "applied-manually"}:
            raise PreconditionError("RESOLVE_ACTION", "Expected skip or applied-manually")
        if value["action"] == "skip" and not re.fullmatch(r"[0-9A-Fa-f]+/[0-9A-Fa-f]+", value.get("lsn", "")):
            raise PreconditionError(
                "RESOLVE_LSN", "skip requires the failed transaction finish LSN from the server log"
            )
    return value


async def pause_budget(service, sc, details):
    slot = await sc.fetchrow(
        """SELECT wal_status,safe_wal_size,
        to_jsonb(s)->>'inactive_since' AS inactive_since FROM pg_replication_slots s WHERE slot_name=$1""",
        service.config.subscription,
    )
    if not slot or slot["wal_status"] not in {"reserved", "extended"}:
        raise PreconditionError("PAUSE_SLOT", "Pause requires a valid reserved slot")
    try:
        evidence = json.loads(
            (service.config.path.parent / service.config.get("capacity", "evidence_file")).read_text()
        )
        age = (datetime.now(UTC) - datetime.fromisoformat(evidence["measured_at"])).total_seconds()
        source = evidence["source"]
        if (
            not 0 <= age <= service.config.get("capacity", "max_age_seconds", 300)
            or source["system_identifier"] != service.store.read()["source_identity"]["system_identifier"]
        ):
            raise ValueError
        available = int(source["free_bytes"]) - int(source["required_bytes"])
    except (OSError, TypeError, KeyError, ValueError):
        raise PreconditionError(
            "PAUSE_CAPACITY", "Fresh matching source capacity evidence is required"
        ) from None
    if slot["safe_wal_size"] is not None:
        available = min(available, slot["safe_wal_size"])
    safe_seconds = max(0, available // (2 * details["wal_rate"]))
    idle = await sc.fetchval(
        "SELECT setting::bigint FROM pg_settings WHERE name='idle_replication_slot_timeout'"
    )
    if idle:
        inactive = slot["inactive_since"]
        elapsed = (datetime.now(UTC) - datetime.fromisoformat(inactive)).total_seconds() if inactive else 0
        safe_seconds = min(safe_seconds, max(0, int(idle - elapsed - 60)))
    if details["max_duration"] > safe_seconds:
        raise PreconditionError(
            "PAUSE_BUDGET", f"Requested pause exceeds estimated safe limit of {safe_seconds} seconds"
        )
    return safe_seconds


async def wait_stopped(service, tc):
    deadline = asyncio.get_running_loop().time() + service.config.get(
        "limits", "operation_timeout_seconds", 120
    )
    while await tc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_stat_subscription WHERE subname=$1 AND pid IS NOT NULL)",
        service.config.subscription,
    ):
        if asyncio.get_running_loop().time() >= deadline:
            raise PreconditionError("WORKER_STOP_TIMEOUT", "Subscription workers have not stopped")
        await asyncio.sleep(0.1)


async def execute(service, kind, path, plan_hash):
    states = (
        {"SYNCING", "CATCHING_UP", "REPLICATING", "PAUSING"}
        if kind == "pause"
        else {"BLOCKED", "PAUSED", "RESUMING"}
    )
    async with service.mutation(kind, path, plan_hash, states=states) as (sc, tc, plan):
        details = extra(kind, plan["extra"])
        await service.owned(sc)
        await service.owned(tc)
        observed = await health.observe(service, sc, tc, allow_disabled=True, ignore_history=True)
        if observed["blockers"]:
            raise PreconditionError("RECOVERY_HEALTH", ", ".join(observed["blockers"]))
        state = service.store.read()
        if state.get("config_hash") != service.config.control_hash:
            raise PreconditionError("CONFIGURATION_CHANGED", "Restore the approved migration configuration")
        if kind == "pause":
            budget = await pause_budget(service, sc, details)
            previous = (state.get("pause") or {}).get("previous_state", state["state"])
            pause = state.get("pause") or dict(
                previous_state=previous,
                reason=details["reason"],
                safe_seconds=budget,
                expires_at=(datetime.now(UTC) + timedelta(seconds=details["max_duration"])).isoformat(),
            )
            await service.state("PAUSING", sc, tc, pause=pause)
            await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
            await wait_stopped(service, tc)
            await service.state("PAUSED", sc, tc, active_plan=None)
        else:
            checked = await integrity.inspect(service, sc, tc, schema=False)
            if checked["blockers"]:
                raise PreconditionError("RECOVERY_INTEGRITY", ", ".join(checked["blockers"]))
            previous = (state.get("pause") or {}).get(
                "previous_state", state.get("previous_state", "REPLICATING")
            )
            if previous not in {"SYNCING", "CATCHING_UP", "REPLICATING"}:
                raise PreconditionError("RECOVERY_STATE", "Retry the interrupted lifecycle operation instead")
            if kind == "resolve":
                interventions = state.get("interventions", [])
                if not any(i["plan_hash"] == plan_hash for i in interventions):
                    interventions.append(dict(plan_hash=plan_hash, recorded_at=now(), **details))
                    state["interventions"] = interventions
                    service.store.save(state)
                    service.store.event("operator_intervention", **details)
                if details["action"] == "skip":
                    if not await sc.fetchval(
                        "SELECT $1::text::pg_lsn <= pg_current_wal_lsn()", details["lsn"]
                    ):
                        raise PreconditionError("RESOLVE_LSN", "Finish LSN is beyond source WAL")
                    await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
                    await wait_stopped(service, tc)
                    reached = await tc.fetchval(
                        "SELECT remote_lsn >= $1::text::pg_lsn FROM pg_replication_origin_status WHERE external_id=$2",
                        details["lsn"],
                        "pg_" + str(state["subscription_oid"]),
                    )
                    if not reached:
                        await tc.execute(
                            "ALTER SUBSCRIPTION "
                            + qi(service.config.subscription)
                            + " SKIP (lsn="
                            + ql(details["lsn"])
                            + ")"
                        )
            await service.state("RESUMING", sc, tc, previous_state=previous)
            try:
                await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " ENABLE")
                await service.barrier(sc, tc)
                await ddl_queue.require_ready(service, tc)
                if await tc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='post')"):
                    await integrity.require(service, sc, tc)
                    report = await validation.verify(service, sc, tc)
                    if not report["passed"]:
                        raise PreconditionError(
                            "RECOVERY_VALIDATION", "Data validation must pass before recovery"
                        )
                else:
                    previous = "SYNCING"
                observed = await health.observe(service, sc, tc, ignore_history=True)
                if observed["blockers"]:
                    raise PreconditionError("RECOVERY_HEALTH", ", ".join(observed["blockers"]))
            except BaseException:
                await service.block("RECOVERY_INCOMPLETE")
                raise
            await service.state(
                previous,
                sc,
                tc,
                active_plan=None,
                blocked_reason=None,
                blocked_details=None,
                pause=None,
                health=observed,
                integrity=None,
            )
            service.store.event("recovery_completed", reason=details["reason"])
    return await service.status()
