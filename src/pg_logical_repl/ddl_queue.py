"""Durable, ordered out-of-stream DDL. The caller holds both controller locks.

No DML is applied here. REFRESH always delegates copy and catchup to tablesync.
Every job has a durable intent before an autocommit command can be issued.
"""

import json
from pathlib import Path

import asyncpg

from .config import digest
from .errors import PreconditionError
from .security import identifier as qi
from .security import qualified as qn


async def listing(service):
    async with service.target.connect(readonly=True) as tc:
        await service.owned(tc)
        return [
            dict(r)
            for r in await tc.fetch(
                "SELECT id,ddl_log_id,seq,kind,status,attempts,last_error,created_at,started_at,finished_at "
                "FROM lc.ddl_queue ORDER BY ddl_log_id,seq"
            )
        ]


async def sync_status(service, sc, tc):
    # Names can change in the same transaction as CREATE, before REFRESH. Track
    # target OIDs so RENAME/SET SCHEMA cannot turn an unfinished copy into REMOVED.
    tables = await tc.fetch("""SELECT t.relation_oid,t.state,t.schema_name AS old_schema,t.table_name AS old_name,n.nspname AS schema,c.relname AS name
        FROM lc.table_sync t LEFT JOIN pg_class c ON c.oid=t.relation_oid
        LEFT JOIN pg_namespace n ON n.oid=c.relnamespace WHERE t.state<>'removed'""")
    for row in tables:
        oid = row["relation_oid"]
        if row["name"] is None:
            await tc.execute(
                "UPDATE lc.table_sync SET state='removed',ready_at=clock_timestamp() WHERE relation_oid=$1",
                oid,
            )
            continue
        if (row["schema"], row["name"]) != (row["old_schema"], row["old_name"]):
            await tc.execute(
                "UPDATE lc.table_sync SET schema_name=$2,table_name=$3 WHERE relation_oid=$1",
                oid,
                row["schema"],
                row["name"],
            )
        if row["state"] == "ready":
            continue
        name = qn(row["schema"], row["name"])
        source_kind = await sc.fetchval("SELECT relkind::text FROM pg_class WHERE oid=to_regclass($1)", name)
        if not source_kind:
            # Source may have committed a rename/drop that apply has not reached.
            continue
        if source_kind == "p":
            ready = await tc.fetchval(
                """SELECT NOT EXISTS(SELECT FROM pg_partition_tree($1::oid::regclass) t
                LEFT JOIN pg_subscription_rel r ON r.srrelid=t.relid AND r.srsubid=(SELECT oid FROM pg_subscription WHERE subname=$2)
                WHERE t.isleaf AND r.srsubstate IS DISTINCT FROM 'r')""",
                oid,
                service.config.subscription,
            )
        else:
            ready = await tc.fetchval(
                """SELECT srsubstate='r' FROM pg_subscription_rel
                WHERE srrelid=$1 AND srsubid=(SELECT oid FROM pg_subscription WHERE subname=$2)""",
                oid,
                service.config.subscription,
            )
        if ready:
            await tc.execute(
                "UPDATE lc.table_sync SET state='ready',ready_at=COALESCE(ready_at,clock_timestamp()) WHERE relation_oid=$1",
                oid,
            )


async def refresh(service, sc, tc, job):
    # Never REFRESH while a published table's CREATE has not reached target yet.
    relations = await sc.fetch(
        """SELECT n.nspname,c.relname FROM pg_get_publication_tables($1) p
        JOIN pg_class c ON c.oid=p.relid JOIN pg_namespace n ON n.oid=c.relnamespace""",
        service.config.publication,
    )
    for relation in relations:
        if not await tc.fetchval("SELECT to_regclass($1) IS NOT NULL", qn(*relation.values())):
            return False
    details = json.loads(job["details"])
    if not details.get("refreshed"):
        await tc.execute(
            "ALTER SUBSCRIPTION "
            + qi(service.config.subscription)
            + " REFRESH PUBLICATION WITH (copy_data=true)"
        )
        # Losing the reply is safe: REFRESH does not restart already enrolled tables.
        await tc.execute(
            "UPDATE lc.ddl_queue SET details=details||'{\"refreshed\":true}'::jsonb WHERE id=$1", job["id"]
        )
        await tc.execute(
            "UPDATE lc.table_sync SET state='copying' WHERE state='pending_refresh' AND ddl_log_id<=$1",
            job["ddl_log_id"],
        )
    await sync_status(service, sc, tc)
    return not await tc.fetchval(
        "SELECT EXISTS(SELECT FROM lc.table_sync WHERE state NOT IN ('ready','removed') AND ddl_log_id<=$1)",
        job["ddl_log_id"],
    )


async def execute_statement(tc, statement, details):
    context = details.get("context", {})
    previous = {}
    try:
        for key, value in context.items():
            if key not in {
                "TimeZone",
                "DateStyle",
                "IntervalStyle",
                "standard_conforming_strings",
                "search_path",
                "lock_timeout",
            }:
                raise PreconditionError("QUEUE_CONTEXT", "Unexpected journal setting")
            previous[key] = await tc.fetchval("SELECT pg_catalog.current_setting($1)", key)
            await tc.fetchval("SELECT pg_catalog.set_config($1,$2,false)", key, value)
        await tc.execute(statement)
    finally:
        for key, value in previous.items():
            await tc.fetchval("SELECT pg_catalog.set_config($1,$2,false)", key, value)


async def partitioned_index(service, tc, job, details):
    from pglast import parse_sql
    from pglast.stream import RawStream

    nodes = details["nodes"]
    receipts = details.get("nodes_intent", {})
    for node in nodes:
        name = qn(node["schema"], node["name"])
        stmt = parse_sql(node["definition"])[0].stmt
        stmt.concurrent = node["kind"] == "i"
        if node["kind"] == "I":
            stmt.relation.inh = False  # ON ONLY: metadata, never a recursive blocking build.
        statement = RawStream()(stmt)
        existing = await tc.fetchrow(
            "SELECT c.oid::int,pg_get_indexdef(c.oid) AS definition,i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)",
            name,
        )
        intent = receipts.get(name)
        if existing:
            if existing["definition"] != node["definition"] or (
                intent and intent.get("oid") and intent["oid"] != existing["oid"]
            ):
                raise PreconditionError("QUEUE_INDEX_DRIFT", "Partition index identity/definition changed")
            if not intent and node["created"]:
                raise PreconditionError(
                    "QUEUE_OBJECT_OWNERSHIP", "New partition index exists without creation intent"
                )
            if node["kind"] == "I" or existing["indisvalid"]:
                continue
            if not node["created"]:
                raise PreconditionError(
                    "QUEUE_OBJECT_OWNERSHIP", "Cannot delete a pre-existing invalid partition index"
                )
            await tc.execute("DROP INDEX CONCURRENTLY " + name)
        receipts[name] = dict(creating=True)
        await tc.execute(
            "UPDATE lc.ddl_queue SET details=jsonb_set(details,'{nodes_intent}',$2::jsonb) WHERE id=$1",
            job["id"],
            json.dumps(receipts),
        )
        try:
            await execute_statement(tc, statement, details)
        finally:
            oid = await tc.fetchval("SELECT to_regclass($1)::oid::int", name)
            if oid:
                receipts[name]["oid"] = oid
                await tc.execute(
                    "UPDATE lc.ddl_queue SET details=jsonb_set(details,'{nodes_intent}',$2::jsonb) WHERE id=$1",
                    job["id"],
                    json.dumps(receipts),
                )
    # Attach bottom-up, including multilevel partition trees. Each edge is its
    # own idempotent metadata operation; the parent becomes valid automatically.
    for node in sorted(nodes, key=lambda n: -n["depth"]):
        if not node["parent_name"]:
            continue
        name = qn(node["schema"], node["name"])
        parent = qn(node["parent_schema"], node["parent_name"])
        attached = await tc.fetchval(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=to_regclass($1) AND inhparent=to_regclass($2))",
            name,
            parent,
        )
        if not attached:
            await tc.execute("ALTER INDEX " + parent + " ATTACH PARTITION " + name)
    for node in nodes:
        valid = await tc.fetchval(
            "SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)",
            qn(node["schema"], node["name"]),
        )
        if valid != node["valid"]:
            raise PreconditionError("QUEUE_INDEX_VALIDITY", "Partition index validity differs from source")
    return True


async def execute_job(service, sc, tc, job):
    details = json.loads(job["details"])
    if job["kind"] == "refresh_publication":
        return await refresh(service, sc, tc, job)
    if job["kind"] == "create_partitioned_index":
        return await partitioned_index(service, tc, job, details)
    if job["kind"] in {"drop_index", "drop_partitioned_index"}:
        name = qn(details["schema"], details["name"])
        existing = await tc.fetchrow(
            "SELECT c.oid::int,pg_get_indexdef(c.oid) AS definition FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)",
            name,
        )
        if not existing:
            if not details.get("drop_oid"):
                raise PreconditionError(
                    "QUEUE_OBJECT_OWNERSHIP", "Target index disappeared before the drop intent"
                )
            return True
        if existing["definition"] != details["definition"] or (
            details.get("drop_oid") and details["drop_oid"] != existing["oid"]
        ):
            raise PreconditionError("QUEUE_OBJECT_OWNERSHIP", "Target index changed after the drop intent")
        await tc.execute(
            "UPDATE lc.ddl_queue SET details=details||jsonb_build_object('drop_oid',$2::int) WHERE id=$1",
            job["id"],
            existing["oid"],
        )
        await tc.execute("DROP INDEX " + ("CONCURRENTLY " if job["kind"] == "drop_index" else "") + name)
        return True
    if job["kind"] == "create_index":
        name = qn(details["schema"], details["name"])
        index = await tc.fetchrow(
            """SELECT c.oid::int,i.indisvalid,pg_get_indexdef(c.oid) AS definition
            FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)""",
            name,
        )
        if index:
            # The intent records that this name was absent before our first attempt.
            # Existing objects are never adopted merely because their names match.
            if not details.get("create_intent") or (details.get("oid") and details["oid"] != index["oid"]):
                raise PreconditionError(
                    "QUEUE_OBJECT_OWNERSHIP", "Index exists without this job's creation intent/OID"
                )
            if index["definition"] != details["definition"]:
                raise PreconditionError(
                    "QUEUE_INDEX_DRIFT", "Existing index definition differs from the journal"
                )
            if index["indisvalid"]:
                return True
            await tc.execute("DROP INDEX CONCURRENTLY " + name)
            await tc.execute("UPDATE lc.ddl_queue SET details=details-'oid' WHERE id=$1", job["id"])
        else:
            await tc.execute(
                "UPDATE lc.ddl_queue SET details=details||'{\"create_intent\":true}'::jsonb WHERE id=$1",
                job["id"],
            )
        try:
            await execute_statement(tc, job["statement"], details)
        finally:
            oid = await tc.fetchval("SELECT to_regclass($1)::oid::int", name)
            if oid:
                await tc.execute(
                    "UPDATE lc.ddl_queue SET details=details||jsonb_build_object('oid',$2::int) WHERE id=$1",
                    job["id"],
                    oid,
                )
        return True
    if job["kind"] in {"validate_constraint", "attach_constraint"}:
        if job["kind"] == "attach_constraint":
            existing = await tc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname=$2)",
                details["table"],
                details["name"],
            )
            if existing:
                if not details.get("attach_intent"):
                    raise PreconditionError(
                        "QUEUE_OBJECT_OWNERSHIP", "Constraint exists without this job's intent"
                    )
                return True
            await tc.execute(
                "UPDATE lc.ddl_queue SET details=details||'{\"attach_intent\":true}'::jsonb WHERE id=$1",
                job["id"],
            )
        await execute_statement(tc, job["statement"], details)
        return True
    raise PreconditionError("QUEUE_KIND", "Unknown job kind: " + job["kind"])


async def process(service, sc, tc):
    if service.config.ddl_mode == "frozen":
        return
    await sync_status(service, sc, tc)
    # A connection belongs to this controller only; leave its session settings intact.
    previous = await tc.fetchval("SHOW search_path")
    await tc.execute("SET search_path=pg_catalog")
    try:
        while True:
            job = await tc.fetchrow(
                "SELECT * FROM lc.ddl_queue WHERE status NOT IN ('done','skipped') ORDER BY ddl_log_id,seq LIMIT 1"
            )
            if not job:
                break
            if job["status"] == "failed":
                raise PreconditionError(
                    "DDL_QUEUE_FAILED", f"DDL queue job {job['id']} exhausted retries; inspect ddl queue"
                )
            if job["status"] != "running" or job["kind"] != "refresh_publication":
                await tc.execute(
                    "UPDATE lc.ddl_queue SET status='running',attempts=attempts+1,started_at=clock_timestamp() WHERE id=$1",
                    job["id"],
                )
            try:
                complete = await execute_job(service, sc, tc, job)
            except (asyncpg.PostgresError, PreconditionError, TimeoutError) as exc:
                attempts = await tc.fetchval("SELECT attempts FROM lc.ddl_queue WHERE id=$1", job["id"])
                failed = attempts >= service.config.get("daemon", "queue_max_attempts", 3) or isinstance(
                    exc, PreconditionError
                )
                await tc.execute(
                    "UPDATE lc.ddl_queue SET status=$2,last_error=$3 WHERE id=$1",
                    job["id"],
                    "failed" if failed else "pending",
                    service.redactor.text(exc)[:2000],
                )
                service.store.event(
                    "ddl_queue_error",
                    job_id=job["id"],
                    attempts=attempts,
                    failed=failed,
                    sqlstate=getattr(exc, "sqlstate", None),
                )
                if failed:
                    raise PreconditionError(
                        "DDL_QUEUE_FAILED", f"DDL queue job {job['id']} failed; inspect ddl queue"
                    ) from None
                break
            if not complete:
                break
            await tc.execute(
                "UPDATE lc.ddl_queue SET status='done',last_error=NULL,finished_at=clock_timestamp() WHERE id=$1",
                job["id"],
            )
            service.store.event("ddl_queue_done", job_id=job["id"], kind=job["kind"])
        # Release source DDL backpressure only after ALL jobs of a journal entry complete.
        completed = await tc.fetch(
            "SELECT ddl_log_id FROM lc.ddl_queue GROUP BY ddl_log_id HAVING bool_and(status IN ('done','skipped'))"
        )
        for row in completed:
            await sc.execute(
                "INSERT INTO lc.operations(name) VALUES($1) ON CONFLICT DO NOTHING",
                "ddl_done:" + str(row["ddl_log_id"]),
            )
    finally:
        await tc.fetchval("SELECT set_config('search_path',$1,false)", previous)


async def require_ready(service, tc):
    if await tc.fetchval(
        "SELECT EXISTS(SELECT FROM lc.ddl_queue WHERE status NOT IN ('done','skipped')) OR EXISTS(SELECT FROM lc.table_sync WHERE state NOT IN ('ready','removed'))"
    ):
        raise PreconditionError("DDL_PENDING", "DDL queue or new table synchronization is incomplete")


async def retry(service, job_id):
    with service.store.lock():
        async with service.source.connect() as sc, service.target.connect() as tc:
            await service.controller_lock(sc, tc)
            await service.owned(sc)
            await service.owned(tc)
            job = await tc.fetchrow("SELECT status FROM lc.ddl_queue WHERE id=$1", job_id)
            if not job or job["status"] != "failed":
                raise PreconditionError("QUEUE_RETRY_STATE", "Only a failed job can be retried")
            await tc.execute(
                "UPDATE lc.ddl_queue SET status='pending',attempts=0,last_error=NULL WHERE id=$1", job_id
            )
            await resume(service, sc, tc)
            service.store.event("ddl_queue_retry", job_id=job_id)
    return dict(id=job_id, status="pending")


async def resume(service, sc, tc):
    state = service.store.read()
    if state["state"] == "BLOCKED" and state.get("blocked_reason") == "DDL_QUEUE_FAILED":
        await service.state(state.get("previous_state", "REPLICATING"), sc, tc, blocked_reason=None)


def read_evidence(path):
    try:
        evidence = json.loads(Path(path).read_text())
    except (OSError, ValueError, TypeError):
        raise PreconditionError("QUEUE_EVIDENCE", "Cannot read queue skip evidence JSON") from None
    if not isinstance(evidence, dict) or any(
        not isinstance(evidence.get(key), str) or not evidence[key].strip()
        for key in ("reason", "approved_by", "verification")
    ):
        raise PreconditionError("QUEUE_EVIDENCE", "Evidence requires reason, approved_by and verification")
    return dict(evidence=evidence, evidence_hash=digest(evidence), reason=evidence["reason"])


def job_fingerprint(job):
    return digest({key: job[key] for key in ("id", "ddl_log_id", "seq", "kind", "statement", "details")})


async def skip(service, path, plan_hash, job_id, reason, *, evidence_file):
    evidence = read_evidence(evidence_file)
    if not reason.strip():
        raise PreconditionError("QUEUE_SKIP_REASON", "An operator justification is required")
    async with service.mutation("ddl-queue-skip", path, plan_hash, states={"REPLICATING", "BLOCKED"}) as (
        sc,
        tc,
        plan,
    ):
        await service.owned(sc)
        await service.owned(tc)
        if plan["extra"].get("job_id") != job_id or plan["extra"].get("reason") != reason:
            raise PreconditionError("QUEUE_SKIP_PLAN", "Plan must bind the job and exact justification")
        job = await tc.fetchrow("SELECT * FROM lc.ddl_queue WHERE id=$1", job_id)
        if evidence["reason"] != reason or plan["extra"].get("evidence_hash") != evidence["evidence_hash"]:
            raise PreconditionError("QUEUE_SKIP_EVIDENCE", "Evidence differs from the approved plan")
        if (
            not job
            or job["status"] != "failed"
            or job["kind"] not in {"create_index", "drop_index", "validate_constraint"}
        ):
            raise PreconditionError(
                "QUEUE_SKIP_STATE",
                "Only a failed index/validation job may be skipped; refresh and constraint attachment are mandatory",
            )
        if plan["extra"].get("job_hash") != job_fingerprint(job):
            raise PreconditionError("QUEUE_SKIP_DRIFT", "Queue job changed after planning")
        if await tc.fetchval(
            "SELECT EXISTS(SELECT FROM lc.ddl_queue WHERE ddl_log_id=$1 AND seq>$2 AND kind='attach_constraint')",
            job["ddl_log_id"],
            job["seq"],
        ):
            raise PreconditionError(
                "QUEUE_SKIP_DEPENDENCY", "This index is required by a constraint attachment"
            )
        await tc.execute(
            "UPDATE lc.ddl_queue SET status='skipped',finished_at=clock_timestamp(),details=details||jsonb_build_object('skip_reason',$2::text,'skip_plan',$3::text) WHERE id=$1",
            job_id,
            reason,
            plan_hash,
        )
        service.store.event("ddl_queue_skipped", job_id=job_id, reason=reason, plan_hash=plan_hash)
        await resume(service, sc, tc)
        state = service.store.read()
        state["active_plan"] = None
        state.setdefault("interventions", []).append(dict(job_id=job_id, reason=reason, plan_hash=plan_hash))
        state["interventions"][-1].update(evidence)
        service.store.save(state)
    return dict(id=job_id, status="skipped")
