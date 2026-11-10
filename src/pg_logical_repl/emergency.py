"""Explicit source-offline detach and separately planned, ownership-checked cleanup."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import __version__, integrity, plans, recovery, validation
from .catalog import fingerprint, inventory
from .config import digest
from .db import identity
from .errors import ConnectorError, OwnershipError, PreconditionError
from .security import identifier as qi
from .security import qualified as qn
from .state import now, write_private


def read_plan(path, expected_hash, kind, service):
    try:
        plan = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        raise PreconditionError("PLAN_INVALID", "Cannot read emergency plan") from None
    if (
        digest(plan) != expected_hash
        or plan["plan_kind"] != kind
        or plan["migration_id"] != service.config.migration_id
    ):
        raise PreconditionError("PLAN_MISMATCH", "Emergency plan does not match the reviewed operation")
    retry = service.store.read().get("active_plan") == expected_hash
    if plan["config_hash"] != service.config.hash or (
        not retry and datetime.fromisoformat(plan["expires_at"]) < datetime.now(UTC)
    ):
        raise PreconditionError("PLAN_EXPIRED", "Configuration changed or plan expired")
    return plan


def is_offline(path):
    try:
        return json.loads(Path(path).read_text()).get("extra", {}).get("source_unavailable", False)
    except (OSError, ValueError):
        raise PreconditionError("PLAN_INVALID", "Cannot read plan") from None


async def require_unavailable(service):
    try:
        async with service.source.connect(readonly=True):
            pass
    except ConnectorError as exc:
        if exc.code == "DATABASE_UNAVAILABLE":
            return
        raise
    raise PreconditionError(
        "SOURCE_AVAILABLE", "Use the ordinary online operation while source is accessible"
    )


async def slot_snapshot(service, sc):
    row = await sc.fetchrow(
        """SELECT slot_name,slot_type,plugin,database,active,
        restart_lsn::text,confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1""",
        service.config.subscription,
    )
    return dict(row) if row else None


async def assert_endpoint(service, c, side):
    expected = service.store.read().get(side + "_identity")
    if not expected or await identity(c) != expected:
        raise OwnershipError(side + " identity differs from the migration")
    await service.owned(c)


async def create_plan(service, kind, *, out=None, extra=None):
    details = dict(extra or {})
    if kind not in {"abort", "cutover-commit", "cleanup"}:
        raise PreconditionError("PLAN_KIND", "Offline mode supports abort and cutover-commit only")
    if not isinstance(details.get("reason"), str) or not details["reason"].strip():
        raise PreconditionError("REASON_REQUIRED", "Emergency operations require a reason")
    state = service.store.read()
    if not state.get("source_identity") or not state.get("target_identity"):
        raise OwnershipError("Emergency cleanup requires recorded endpoint identities")
    if kind == "cleanup":
        if state["state"] not in {"DETACH_CLEANUP_PENDING", "ABORT_CLEANUP_PENDING"}:
            raise PreconditionError("CLEANUP_STATE", "No deferred cleanup is pending")
        details["source_unavailable"] = False
    else:
        await require_unavailable(service)
        details["source_unavailable"] = True
        allowed = (
            {"CUTOVER_READY", "COMMITTING", "DETACH_CLEANUP_PENDING"}
            if kind == "cutover-commit"
            else {
                "PREPARED",
                "STARTING",
                "SYNCING",
                "CATCHING_UP",
                "REPLICATING",
                "BLOCKED",
                "PAUSED",
                "PAUSING",
                "RESUMING",
                "RESYNCING",
                "ABORTING",
                "ABORT_CLEANUP_PENDING",
            }
        )
        if state["state"] not in allowed:
            raise PreconditionError("EMERGENCY_STATE", "Offline operation is not valid in " + state["state"])
    async with service.target.connect(readonly=True) as tc:
        await assert_endpoint(service, tc, "target")
        target = plans.snapshot(await inventory(tc, include_passwords=True), state["ownership_token"])
        if kind == "cutover-commit":
            await verify_final_target(service, tc)
    source = dict(identity=state["source_identity"])
    if kind == "cleanup":
        async with service.source.connect(readonly=True) as sc:
            await assert_endpoint(service, sc, "source")
            source = plans.snapshot(await inventory(sc, include_passwords=True), state["ownership_token"])
            details["slot"] = await slot_snapshot(service, sc)
            if details["slot"] and details["slot"]["active"]:
                raise PreconditionError("SLOT_ACTIVE", "Deferred slot is still active")
    plan = dict(
        schema_version="pg_logical_connector/plan-v2",
        plan_kind=kind,
        migration_id=service.config.migration_id,
        tool_version=__version__,
        config_hash=service.config.hash,
        source=source,
        target=target,
        state=state["state"],
        created_at=now(),
        expires_at=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        extra=details,
        actions=["Detach only the recorded target subscription; retain source cleanup obligations"]
        if kind != "cleanup"
        else [
            "Recheck both identities, ownership receipts and the reviewed inactive slot; remove migration resources"
        ],
        source_revalidated=kind == "cleanup",
    )
    value = digest(plan)
    dest = Path(out) if out else service.store.directory / "plans" / f"{kind}-{value[:16]}.json"
    write_private(dest, json.dumps(plan, indent=2))
    write_private(
        dest.with_suffix(".md"), f"# {kind}\n\nHash: `{value}`\n\n" + "\n".join(plan["actions"]) + "\n"
    )
    return dict(plan=plan, plan_hash=value, path=str(dest))


async def verify_final_target(service, tc):
    state = service.store.read()
    try:
        report = json.loads((service.store.directory / "reports/cutover.json").read_text())
        evidence = state["write_fence"]
        age = (datetime.now(UTC) - datetime.fromisoformat(evidence["created_at"])).total_seconds()
        if not report["authoritative"] or not 0 <= age <= service.config.get(
            "cutover", "max_validation_age_seconds", 21600
        ):
            raise ValueError
        expected_schema = report["target_schema"]
    except (OSError, KeyError, TypeError, ValueError):
        raise PreconditionError(
            "FINAL_EVIDENCE_REQUIRED", "Offline detach requires fresh finalized data and write-fence evidence"
        ) from None
    inv = await inventory(tc, include_passwords=True)
    if fingerprint(inv) != expected_schema:
        raise PreconditionError("TARGET_CHANGED", "Target schema changed after finalize")
    executor = "lc_ddl_executor_" + service.config.migration_id
    if (
        integrity.role_view(inv, state["ownership_token"], executor) != state["role_baseline"]["target"]
        or await integrity.resources(tc, executor) != state["resource_baseline"]["target"]
    ):
        raise PreconditionError("TARGET_CHANGED", "Target roles or installed resources changed")
    relations = {qn(r["schema"], r["name"]): r for r in inv["relations"]}
    for row in report["tables"]:
        relation = relations[row["table"]]
        async with validation.snapshot(tc):
            actual = await validation.digest_range(
                tc, row["table"], [c["attname"] for c in relation["columns"]], [], None, None, report["level"]
            )
        if actual != row["target"]:
            raise PreconditionError("TARGET_CHANGED", "Target data changed after finalize")
    for row in report["sequences"]:
        if dict(await tc.fetchrow("SELECT last_value,is_called FROM " + row["sequence"])) != row["source"]:
            raise PreconditionError("TARGET_CHANGED", "Target sequence changed after finalize")
    if await validation.large_objects(tc) != report["large_objects"]:
        raise PreconditionError("TARGET_CHANGED", "Target large objects changed after finalize")


async def execute(service, kind, path, plan_hash):
    plan = read_plan(path, plan_hash, kind, service)
    if not plan["extra"].get("source_unavailable"):
        raise PreconditionError("PLAN_MISMATCH", "Explicit offline plan required")
    await require_unavailable(service)
    with service.store.lock():
        state = service.store.read()
        retry = state.get("active_plan") == plan_hash
        async with service.target.connect() as tc:
            await service.controller_lock(None, tc)
            await assert_endpoint(service, tc, "target")
            observed = plans.snapshot(await inventory(tc, include_passwords=True), state["ownership_token"])
            if not retry and (observed != plan["target"] or state["state"] != plan["state"]):
                raise PreconditionError("PLAN_DRIFT", "Target or migration changed after the emergency plan")
            if kind == "cutover-commit":
                await verify_final_target(service, tc)
            sub = await tc.fetchrow(
                "SELECT oid::int,subslotname,subpublications FROM pg_subscription WHERE subname=$1",
                service.config.subscription,
            )
            if sub and (
                sub["oid"] != state.get("subscription_oid")
                or sub["subslotname"] not in {service.config.subscription, None}
                or sub["subpublications"] != [service.config.publication]
            ):
                raise OwnershipError("Subscription differs from its creation receipt")
            if sub and sub["subslotname"] is None and not retry:
                raise OwnershipError("Subscription slot was detached outside this operation")
            pending = "DETACH_CLEANUP_PENDING" if kind == "cutover-commit" else "ABORT_CLEANUP_PENDING"
            await service.state(
                pending,
                tc=tc,
                active_plan=plan_hash,
                cleanup_pending=dict(
                    outcome="DETACHED" if kind == "cutover-commit" else "ABORTED",
                    reason=plan["extra"]["reason"],
                    source_revalidated=False,
                ),
            )
            if sub:
                await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
                await recovery.wait_stopped(service, tc)
                await tc.execute(
                    "ALTER SUBSCRIPTION " + qi(service.config.subscription) + " SET (slot_name=NONE)"
                )
                await tc.execute("DROP SUBSCRIPTION " + qi(service.config.subscription))
            if kind == "abort" and state.get("resync"):
                from .resync import restore_for_abort

                await restore_for_abort(service, tc)
            service.store.event(
                "offline_detached", source_cleanup_pending=True, reason=plan["extra"]["reason"]
            )
    return await service.status()


async def cleanup(service, path, plan_hash):
    plan = read_plan(path, plan_hash, "cleanup", service)
    with service.store.lock():
        state = service.store.read()
        if state["state"] not in {"DETACH_CLEANUP_PENDING", "ABORT_CLEANUP_PENDING"}:
            raise PreconditionError("CLEANUP_STATE", "No deferred cleanup is pending")
        async with service.source.connect() as sc, service.target.connect() as tc:
            await service.controller_lock(sc, tc)
            retry = state.get("active_plan") == plan_hash
            for side, c in [("source", sc), ("target", tc)]:
                if await identity(c) != plan[side]["identity"]:
                    raise OwnershipError(side + " identity changed")
                if not retry:
                    await service.owned(c)
                    if (
                        plans.snapshot(await inventory(c, include_passwords=True), state["ownership_token"])
                        != plan[side]
                    ):
                        raise PreconditionError("PLAN_DRIFT", side + " changed after the cleanup plan")
                await c.fetchval("SELECT set_config('lc.ownership_token',$1,false)", state["ownership_token"])
            if await tc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", service.config.subscription
            ):
                raise OwnershipError("Subscription must be detached before deferred cleanup")
            slot = await slot_snapshot(service, sc)
            if slot:
                await service.owned(sc)
                receipt = await sc.fetchval("SELECT details FROM lc.operations WHERE name='subscription'")
                if not receipt or json.loads(receipt).get("oid") != state.get("subscription_oid"):
                    raise OwnershipError("Missing source receipt for the detached subscription")
                if (
                    slot != plan["extra"]["slot"]
                    or slot["active"]
                    or slot["plugin"] != "pgoutput"
                    or slot["database"] != state["source_identity"]["database"]
                ):
                    raise OwnershipError("Slot differs from the reviewed inactive slot")
            # Never guess ownership of leftover tablesync or unrelated slots.
            other = await sc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE database=current_database() AND slot_name<>$1 AND NOT slot_name=ANY($2::text[]))",
                service.config.subscription,
                state.get("source_slots_before", []),
            )
            if other:
                raise OwnershipError("Additional slots require ownership reconciliation before cleanup")
            state["active_plan"] = plan_hash
            service.store.save(state)
            if slot:
                await sc.fetchval("SELECT pg_drop_replication_slot($1)", service.config.subscription)
            await service.cleanup(sc, tc)
            await service.state(state["cleanup_pending"]["outcome"], active_plan=None, cleanup_pending=None)
    return await service.status()
