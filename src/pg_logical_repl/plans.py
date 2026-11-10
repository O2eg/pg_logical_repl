"""Plans bind reviewed intent to identities and catalog fingerprints."""

import hmac
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import __version__, recovery
from .catalog import fingerprint, inventory, role_fingerprint
from .config import canonical, digest
from .errors import PreconditionError
from .state import now, write_private

KINDS = (
    {"prepare", "start", "abort", "cutover-finalize", "cutover-commit", "ddl-exec", "ddl-queue-skip"}
    | recovery.KINDS
    | {"cleanup", "resync-table"}
)


def snapshot(inv, token):
    verifiers = [(r["rolname"], r.get("rolpassword", "UNREADABLE")) for r in inv["roles"]]
    return dict(
        identity=inv["identity"],
        schema=fingerprint(inv),
        roles=role_fingerprint(inv),
        role_verifiers=hmac.digest(token.encode(), canonical(verifiers), "sha256").hex(),
    )


async def create(service, kind, *, out=None, extra=None):
    if kind == "cleanup" or (extra or {}).get("source_unavailable"):
        from . import emergency

        return await emergency.create_plan(service, kind, out=out, extra=extra)
    if kind in recovery.KINDS:
        extra = recovery.extra(kind, extra)
    if kind not in KINDS:
        raise PreconditionError("PLAN_KIND", "Unsupported plan kind")
    async with service.source.connect(readonly=True) as sc, service.target.connect(readonly=True) as tc:
        si, ti = await inventory(sc, include_passwords=True), await inventory(tc, include_passwords=True)
        if kind == "pause":
            extra["estimated_safe_seconds"] = await recovery.pause_budget(service, sc, extra)
        if kind == "resync-table":
            from .resync import prepare_plan

            extra = await prepare_plan(service, sc, tc, extra)
        if kind == "ddl-queue-skip":
            from .ddl_queue import job_fingerprint

            if not extra or not extra.get("evidence_hash"):
                raise PreconditionError("QUEUE_EVIDENCE", "Queue skip plan requires evidence")
            await service.owned(tc)
            job = await tc.fetchrow("SELECT * FROM lc.ddl_queue WHERE id=$1", extra.get("job_id"))
            if not job or job["status"] != "failed":
                raise PreconditionError("QUEUE_SKIP_STATE", "Plan requires a failed queue job")
            extra = dict(extra, job_hash=job_fingerprint(job))
    with service.store.lock():
        state = service.store.initialize()
    plan = dict(
        schema_version="pg_logical_connector/plan-v2",
        plan_kind=kind,
        migration_id=service.config.migration_id,
        tool_version=__version__,
        config_hash=service.config.hash,
        source=snapshot(si, state["ownership_token"]),
        target=snapshot(ti, state["ownership_token"]),
        state=state["state"],
        created_at=now(),
        expires_at=(datetime.now(UTC) + timedelta(hours=24)).isoformat(),
        extra=extra or {},
    )
    actions = {
        "prepare": [
            "Validate source and empty target, capacity, roles, extensions and locale",
            "Create owned lc resources on both endpoints; freeze source DDL",
            "Copy missing global roles and exact memberships; restore schema and identity indexes",
        ],
        "start": [
            "Create explicit publication of logged tables, ddl_log and barrier",
            "Create subscription and source slot; PostgreSQL performs initial data copy",
        ],
        "abort": [
            "Disable and drop owned subscription and slot",
            "If table resync was interrupted, retain referencing foreign keys as NOT VALID and report incomplete target data",
            "Drop owned publication and source guard",
            "Restore changed replica identities; remove owned lc schemas; preserve target application objects and roles",
        ],
        "cutover-finalize": [
            "Validate external write fence and drain existing writers",
            "Lock source tables; wait for replication barrier",
            "Synchronize sequences and deferred data",
            "Validate schema and data; leave target closed",
        ],
        "cutover-commit": [
            "Revalidate write fence, schema and data",
            "Irreversibly remove subscription and slot",
            "Remove publication, guard and lc resources; leave independent target",
        ],
        "ddl-exec": [
            "Validate single DDL statement and lock affected relation",
            "Execute source DDL and journal row in one transaction",
        ],
        "ddl-queue-skip": [
            "Record the exact job and operator justification as a permanent intervention",
            "Skip a failed index/validation job; schema drift still blocks cutover",
        ],
    }
    actions.update(
        pause=["Check WAL budget and pause the owned subscription"],
        resume=["Recheck identities, roles, resources, schema and data before recovery"],
        resolve=["Record the reviewed intervention and validate before recovery"],
    )
    actions["resync-table"] = [
        "Remove the reviewed table from publication, truncate only it, run native tablesync, restore referencing foreign keys and validate"
    ]
    plan["actions"] = actions[kind]
    plan["global_roles_to_create"] = [
        r["rolname"] for r in si["roles"] if r["rolname"] not in {x["rolname"] for x in ti["roles"]}
    ]
    plan["source_replica_identity_changes"] = [
        r["schema"] + "." + r["name"]
        for r in si["relations"]
        if r["kind"] in ("r", "p")
        and r["persistence"] == "p"
        and not r["extension"]
        and r["replica_identity"] == "d"
        and not r["has_pk"]
    ]
    h = digest(plan)
    path = Path(out) if out else service.store.directory / "plans" / f"{kind}-{h[:16]}.json"
    write_private(path, json.dumps(plan, indent=2, ensure_ascii=False) + "\n")
    write_private(
        path.with_suffix(".md"),
        f"# {kind}: {service.config.migration_id}\n\nHash: `{h}`\n\n"
        + "\n".join("- " + a for a in plan["actions"])
        + "\n",
    )
    return dict(plan=plan, plan_hash=h, path=str(path))


async def validate(service, kind, path, expected_hash, sc, tc):
    try:
        plan = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        raise PreconditionError("PLAN_INVALID", "Cannot read plan") from None
    if digest(plan) != expected_hash:
        raise PreconditionError("PLAN_HASH", "Plan hash does not match")
    if (
        plan["plan_kind"] != kind
        or plan["migration_id"] != service.config.migration_id
        or plan["config_hash"] != service.config.hash
    ):
        raise PreconditionError("PLAN_MISMATCH", "Plan kind, migration or configuration changed")
    state = service.store.read()
    retry = state.get("active_plan") == expected_hash
    if not retry and datetime.fromisoformat(plan["expires_at"]) < datetime.now(UTC):
        raise PreconditionError("PLAN_EXPIRED", "Generate a fresh plan")
    for side, c in [("source", sc), ("target", tc)]:
        observed = snapshot(await inventory(c, include_passwords=True), state["ownership_token"])
        if observed["identity"] != plan[side]["identity"]:
            raise PreconditionError("IDENTITY_CHANGED", side + " identity changed")
        if not retry and observed != plan[side]:
            raise PreconditionError("PLAN_DRIFT", side + " catalog changed; create a new plan")
    if not retry and state["state"] != plan["state"]:
        raise PreconditionError("STATE_CHANGED", "Migration state changed; create a new plan")
    return plan
