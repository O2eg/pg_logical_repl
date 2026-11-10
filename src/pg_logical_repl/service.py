"""Transport-independent lifecycle service. PostgreSQL owns row replication."""

import asyncio
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from importlib.resources import files

import asyncpg

from . import ddl_queue, health, integrity, monitoring, plans, schema
from .catalog import fingerprint, inventory
from .coverage import manifest
from .db import Database
from .errors import ConnectorError, OwnershipError, PreconditionError
from .preflight import check
from .roles import copy_roles
from .security import Redactor, conninfo
from .security import identifier as qi
from .security import literal as ql
from .security import qualified as qn
from .state import Store, now, write_private


def resource(name):
    return files(__package__).joinpath("sql", name + ".sql").read_text()


class Service:
    def __init__(self, config, redactor=None):
        self.config = config
        self.redactor = redactor or Redactor()
        self.store = Store(config.state_dir, config.migration_id, self.redactor)
        self.source = Database(config, "source", self.redactor)
        self.target = Database(config, "target", self.redactor)

    async def check(self):
        result, source, _ = await check(
            self.config, self.redactor, prepared=self.store.read()["state"] != "NEW"
        )
        coverage = manifest(source)
        write_private(self.store.directory / "coverage/latest.json", json.dumps(coverage, indent=2))
        result["coverage"] = coverage
        write_private(self.store.directory / "checks/latest.json", json.dumps(result, indent=2, default=str))
        return result

    async def plan(self, kind, **kwargs):
        return await plans.create(self, kind, **kwargs)

    async def owned(self, c, *, optional=False):
        if not await c.fetchval("SELECT to_regnamespace('lc') IS NOT NULL"):
            if optional:
                return False
            raise OwnershipError("Owned lc schema is missing")
        try:
            r = await c.fetchrow("SELECT migration_id,ownership_token,resource_version FROM lc.control")
        except Exception:
            raise OwnershipError("Existing lc schema has no recognized control record") from None
        s = self.store.read()
        if (
            not r
            or r["migration_id"] != self.config.migration_id
            or r["ownership_token"] != s["ownership_token"]
            or r["resource_version"] != 2
        ):
            raise OwnershipError("Database ownership does not match local state")
        return True

    async def controller_lock(self, sc, tc):
        # Keep the existing lock namespace so old and new controllers still exclude each other.
        key = int.from_bytes(
            hashlib.sha256(("logical_connector/" + self.config.migration_id).encode()).digest()[:8],
            signed=True,
        )
        for c in (sc, tc):
            if c is None:
                continue
            if not await c.fetchval("SELECT pg_try_advisory_lock($1)", key):
                raise OwnershipError("Another controller holds the migration lock")

    @asynccontextmanager
    async def mutation(self, kind, path, plan_hash, *, states):
        with self.store.lock():
            s = self.store.read()
            if (
                s["state"] == "RESYNCING"
                and kind != "abort"
                and (kind != "resync-table" or (s.get("resync") or {}).get("plan_hash") != plan_hash)
            ):
                raise PreconditionError("RESYNC_PLAN_REQUIRED", "Retry the original resync plan or abort")
            if s["state"] not in states:
                raise PreconditionError("INVALID_STATE", f"{kind} is unavailable in {s['state']}")
            async with self.source.connect() as sc, self.target.connect() as tc:
                await self.controller_lock(sc, tc)
                plan = await plans.validate(self, kind, path, plan_hash, sc, tc)
                s = self.store.initialize()
                s["active_plan"] = plan_hash
                self.store.save(s)
                for c in (sc, tc):
                    await c.fetchval("SELECT set_config('lc.ownership_token',$1,false)", s["ownership_token"])
                self.store.event("operation_started", kind=kind, plan_hash=plan_hash)
                try:
                    yield sc, tc, plan
                except BaseException as exc:
                    self.store.event("operation_interrupted", kind=kind, error=type(exc).__name__)
                    raise
                else:
                    self.store.event(
                        "operation_completed",
                        kind=kind,
                        plan_hash=plan_hash,
                        state=self.store.read()["state"],
                    )

    async def state(self, name, sc=None, tc=None, **extra):
        s = self.store.read()
        s.update(state=name, **extra)
        self.store.save(s)
        for c in (sc, tc):
            if c and await self.owned(c, optional=True):
                await c.execute("UPDATE lc.control SET state=$1,updated_at=clock_timestamp()", name)
        self.store.event("state_changed", state=name)

    async def install(self, c):
        if await self.owned(c, optional=True):
            return
        self.store.event("resource_install_intent")
        async with c.transaction():
            await c.execute(resource("base"))
            await c.execute(
                "INSERT INTO lc.control(migration_id,ownership_token,state) VALUES($1,$2,$3)",
                self.config.migration_id,
                self.store.read()["ownership_token"],
                "PREPARING",
            )
            await c.execute(
                "UPDATE lc.control SET ddl_mode=$1,settings=$2::jsonb",
                self.config.ddl_mode,
                json.dumps(
                    dict(
                        lock_timeout_ms=1000 * self.config.get("limits", "lock_timeout_seconds", 5),
                        new_tables_without_pk=self.config.get("ddl", "new_tables_without_pk", "reject"),
                        rewrite_max_table_bytes=self.config.get("ddl", "rewrite_max_table_bytes", 67108864),
                        set_not_null_max_table_bytes=self.config.get(
                            "ddl", "set_not_null_max_table_bytes", 67108864
                        ),
                    )
                ),
            )

    async def prepare(self, path, plan_hash):
        async with self.mutation("prepare", path, plan_hash, states={"NEW", "PREPARING"}) as (sc, tc, plan):
            if self.store.read()["state"] == "NEW":
                report, si, _ = await check(self.config, self.redactor)
                if report["blockers"]:
                    raise PreconditionError("PREFLIGHT_BLOCKED", json.dumps(report["blockers"]))
                for c in (sc, tc):
                    if await c.fetchval("SELECT to_regnamespace('lc') IS NOT NULL"):
                        raise OwnershipError("Pre-existing lc schema is not adoptable")
                await self.state("PREPARING")
            await self.install(tc)
            await self.install(sc)
            if not await sc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='guard')"):
                async with sc.transaction():
                    if await sc.fetchval(
                        "SELECT EXISTS(SELECT FROM pg_event_trigger WHERE evtname IN ('lc_guard','lc_capture','lc_rewrite_guard'))"
                    ):
                        raise OwnershipError("Event trigger exists without an install receipt")
                    await sc.execute(resource("ddl_support"))
                    await sc.execute(resource("freeze"))
                    if self.config.ddl_mode != "frozen":
                        await sc.execute(resource("capture"))
                    await sc.execute("INSERT INTO lc.operations(name) VALUES('guard')")
            si = await inventory(sc, include_passwords=True)
            ri_receipt = await sc.fetchval("SELECT details FROM lc.operations WHERE name='replica_identity'")
            expected_source = json.loads(ri_receipt)["schema"] if ri_receipt else plan["source"]["schema"]
            observed = plans.snapshot(si, self.store.read()["ownership_token"])
            if observed["schema"] != expected_source or any(
                observed[k] != plan["source"][k] for k in ("roles", "role_verifiers")
            ):
                raise PreconditionError(
                    "PREPARE_SOURCE_DRIFT", "Source changed outside the approved preparation steps"
                )
            major = si["identity"]["major"]
            if not await tc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='roles')"):
                self.store.event("step_intent", step="roles")
                async with tc.transaction():
                    await copy_roles(sc, tc, si, self.redactor)
                    await tc.execute("INSERT INTO lc.operations(name) VALUES('roles')")
            if not await sc.fetchval(
                "SELECT EXISTS(SELECT FROM lc.operations WHERE name='replica_identity')"
            ):
                changed = []
                async with sc.transaction():
                    for r in si["relations"]:
                        if (
                            r["kind"] in ("r", "p")
                            and r["persistence"] == "p"
                            and not r["extension"]
                            and r["replica_identity"] == "d"
                            and not r["has_pk"]
                        ):
                            if self.config.get("policies", "replica_identity", "fail") != "full_if_needed":
                                raise PreconditionError("REPLICA_IDENTITY", "Missing identity")
                            await sc.execute(
                                "ALTER TABLE " + qn(r["schema"], r["name"]) + " REPLICA IDENTITY FULL"
                            )
                            changed.append([r["schema"], r["name"]])
                    await sc.execute(
                        "INSERT INTO lc.operations(name,details) VALUES('replica_identity',$1::jsonb)",
                        json.dumps(dict(changed=changed, schema=fingerprint(await inventory(sc)))),
                    )
            if not await tc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='schema')"):
                self.store.event("step_intent", step="schema")
                await schema.dump_schema(self, major)
                await schema.restore(self, major, "schema")
            if self.config.ddl_mode != "frozen" and not await tc.fetchval(
                "SELECT EXISTS(SELECT FROM lc.operations WHERE name='replay')"
            ):
                executor = "lc_ddl_executor_" + self.config.migration_id
                async with tc.transaction():
                    if await tc.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", executor):
                        raise OwnershipError("DDL executor role exists without receipt")
                    await tc.execute(
                        "CREATE ROLE " + qi(executor) + " NOLOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB INHERIT"
                    )
                    owners = sorted(
                        {r["rolname"] for r in si["roles"] if not r["rolname"].startswith("lc_ddl_executor_")}
                    )
                    for owner in owners:
                        await tc.execute(
                            "GRANT "
                            + qi(owner)
                            + " TO "
                            + qi(executor)
                            + (" WITH INHERIT TRUE, SET TRUE" if major >= 16 else "")
                        )
                    await tc.execute("GRANT USAGE,CREATE ON SCHEMA lc TO " + qi(executor))
                    await tc.execute("GRANT SELECT ON lc.control TO " + qi(executor))
                    await tc.execute(
                        "GRANT SELECT,INSERT,UPDATE ON lc.ddl_applied,lc.ddl_queue,lc.table_sync TO "
                        + qi(executor)
                    )
                    await tc.execute("GRANT USAGE ON ALL SEQUENCES IN SCHEMA lc TO " + qi(executor))
                    await tc.execute(resource("replay"))
                    await tc.execute("ALTER FUNCTION lc.replay() OWNER TO " + qi(executor))
                    oid = await tc.fetchval("SELECT oid::int FROM pg_roles WHERE rolname=$1", executor)
                    await tc.execute(
                        "INSERT INTO lc.operations(name,details) VALUES('replay',$1::jsonb)",
                        json.dumps(dict(role=executor, oid=oid, schemas=[])),
                    )
            si = await inventory(sc)
            if not await tc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='route_probe')"):
                probe = "lc_probe_" + self.config.migration_id
                if await tc.fetchval("SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", probe):
                    raise OwnershipError("Route probe name already exists")
                values, _ = self.source.libpq(replication=True)
                async with tc.transaction():
                    await tc.execute(
                        "CREATE SUBSCRIPTION "
                        + qi(probe)
                        + " CONNECTION "
                        + ql(conninfo(values))
                        + " PUBLICATION "
                        + qi(self.config.publication)
                        + " WITH (enabled=false,create_slot=false,slot_name=NONE,copy_data=false)"
                    )
                    await tc.execute("DROP SUBSCRIPTION " + qi(probe))
                    await tc.execute("INSERT INTO lc.operations(name) VALUES('route_probe')")
            s = self.store.read()
            s["source_slots_before"] = [r["slot_name"] for r in si["slots"]]
            s["source_schema"] = fingerprint(si)
            s["source_identity"] = si["identity"]
            s["target_identity"] = (await inventory(tc))["identity"]
            s["relations"] = [
                dict(
                    schema=r["schema"],
                    name=r["name"],
                    kind=r["kind"],
                    persistence=r["persistence"],
                    populated=r["populated"],
                )
                for r in si["relations"]
                if not r["extension"]
            ]
            s["active_plan"] = None
            self.store.save(s)
            await integrity.establish(self, sc, tc)
            await self.state("PREPARED", sc, tc)
        return await self.status()

    async def start(self, path, plan_hash):
        async with self.mutation("start", path, plan_hash, states={"PREPARED", "STARTING"}) as (sc, tc, plan):
            await self.owned(sc)
            await self.owned(tc)
            if fingerprint(await inventory(sc)) != self.store.read()["source_schema"]:
                raise PreconditionError("SCHEMA_DRIFT", "Source schema changed after prepare")
            await self.state("STARTING", sc, tc)
            if not await sc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='publication')"):
                if await sc.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_publication WHERE pubname=$1)", self.config.publication
                ):
                    raise OwnershipError("Publication exists without an ownership receipt")
                relations = [
                    r
                    for r in (await inventory(sc))["relations"]
                    if r["kind"] in ("r", "p")
                    and r["persistence"] == "p"
                    and not r["is_partition"]
                    and not r["extension"]
                ]
                names = [qn(r["schema"], r["name"]) for r in relations] + ["lc.ddl_log", "lc.barrier"]
                async with sc.transaction():
                    await sc.execute(
                        "CREATE PUBLICATION "
                        + qi(self.config.publication)
                        + " FOR TABLE "
                        + ",".join(names)
                        + " WITH (publish_via_partition_root=false)"
                    )
                    oid = await sc.fetchval(
                        "SELECT oid::int FROM pg_publication WHERE pubname=$1", self.config.publication
                    )
                    await sc.execute(
                        "INSERT INTO lc.operations(name,details) VALUES('publication',$1::jsonb)",
                        json.dumps(
                            dict(
                                oid=oid,
                                tables=sorted(
                                    [[r["schema"], r["name"]] for r in relations]
                                    + [["lc", "ddl_log"], ["lc", "barrier"]]
                                ),
                            )
                        ),
                    )
            else:
                expected = json.loads(
                    await sc.fetchval("SELECT details FROM lc.operations WHERE name='publication'")
                )["oid"]
                if (
                    await sc.fetchval(
                        "SELECT oid::int FROM pg_publication WHERE pubname=$1", self.config.publication
                    )
                    != expected
                ):
                    raise OwnershipError("Publication identity changed")
            existing = await tc.fetchrow(
                "SELECT oid::int,subslotname,subenabled,subpublications FROM pg_subscription WHERE subname=$1",
                self.config.subscription,
            )
            if not existing:
                if await sc.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)",
                    self.config.subscription,
                ):
                    raise OwnershipError("Slot already exists without subscription; inspect before recovery")
                s = self.store.read()
                s["subscription_intent"] = True
                self.store.save(s)
                values, _ = self.source.libpq(replication=True)
                await tc.execute(
                    "CREATE SUBSCRIPTION "
                    + qi(self.config.subscription)
                    + " CONNECTION "
                    + ql(conninfo(values))
                    + " PUBLICATION "
                    + qi(self.config.publication)
                    + " WITH (streaming=on, binary=false, synchronous_commit=on, enabled=false"
                    + (", run_as_owner=true" if self.store.read()["source_identity"]["major"] >= 16 else "")
                    + ")"
                )
                existing = await tc.fetchrow(
                    "SELECT oid::int,subslotname,subenabled,subpublications FROM pg_subscription WHERE subname=$1",
                    self.config.subscription,
                )
            if (
                not self.store.read().get("subscription_intent")
                or existing["subslotname"] != self.config.subscription
                or existing["subpublications"] != [self.config.publication]
            ):
                raise OwnershipError("Subscription does not match creation intent")
            s = self.store.read()
            await sc.execute(
                "INSERT INTO lc.operations(name,details) VALUES('subscription',$1::jsonb) ON CONFLICT(name) DO NOTHING",
                json.dumps(dict(oid=existing["oid"], slot=self.config.subscription)),
            )
            s["subscription_oid"] = existing["oid"]
            s["active_plan"] = None
            self.store.save(s)
            await tc.execute("ALTER SUBSCRIPTION " + qi(self.config.subscription) + " ENABLE")
            await self.state("SYNCING", sc, tc)
        return await self.status()

    async def status(self):
        state = self.store.read()
        try:
            result = await self._live_status()
        except (ConnectorError, asyncpg.PostgresError) as exc:
            if isinstance(exc, ConnectorError) and exc.code != "DATABASE_UNAVAILABLE":
                raise
            if isinstance(exc, asyncpg.PostgresError) and not (exc.sqlstate or "").startswith("42"):
                raise
            result = dict(
                schema_version="pg_logical_connector/status-v2",
                state=state["state"],
                migration_id=self.config.migration_id,
                health=state.get("health"),
                availability={},
                inspection_error=getattr(exc, "code", None) or "SQLSTATE " + exc.sqlstate,
            )
            for side, database in [("source", self.source), ("target", self.target)]:
                try:
                    async with database.connect(readonly=True) as c:
                        result["availability"][side] = True
                        if side == "target":
                            sub = await c.fetchrow(
                                "SELECT oid::int,subenabled,subslotname FROM pg_subscription WHERE subname=$1",
                                self.config.subscription,
                            )
                            result["subscription"] = dict(sub) if sub else None
                except ConnectorError as unavailable:
                    if unavailable.code != "DATABASE_UNAVAILABLE":
                        raise
                    result["availability"][side] = False
        for key in (
            "blocked_reason",
            "blocked_details",
            "integrity",
            "validation_last_pass",
            "validation_last_chunk",
            "pause",
            "cleanup_pending",
            "interventions",
            "resync",
            "resync_abort_constraints",
        ):
            if state.get(key) is not None:
                result[key] = state[key]
        return result

    async def _live_status(self):
        s = self.store.read()
        result = dict(
            schema_version="pg_logical_connector/status-v2",
            state=s["state"],
            migration_id=self.config.migration_id,
            health=s.get("health"),
        )
        if s["state"] in {"NEW", "DETACHED", "ABORTED"}:
            return result
        async with self.source.connect(readonly=True) as sc, self.target.connect(readonly=True) as tc:
            sub = await tc.fetchrow(
                "SELECT oid::int,subenabled,subslotname FROM pg_subscription WHERE subname=$1",
                self.config.subscription,
            )
            result["subscription"] = dict(sub) if sub else None
            if sub:
                result["tables"] = [
                    dict(r)
                    for r in await tc.fetch(
                        """SELECT n.nspname AS schema,c.relname AS name,r.srsubstate::text AS state
                    FROM pg_subscription_rel r JOIN pg_class c ON c.oid=r.srrelid JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE srsubid=$1 ORDER BY 1,2""",
                        sub["oid"],
                    )
                ]
                result["errors"] = [
                    dict(r)
                    for r in await tc.fetch(
                        "SELECT apply_error_count,sync_error_count FROM pg_stat_subscription_stats WHERE subid=$1",
                        sub["oid"],
                    )
                ]
            result["slot"] = [
                dict(r)
                for r in await sc.fetch(
                    """SELECT slot_name,active,wal_status,
                pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint AS retained_bytes,
                pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint AS lag_bytes
                FROM pg_replication_slots WHERE slot_name=$1""",
                    self.config.subscription,
                )
            ]
            if await tc.fetchval("SELECT to_regclass('lc.ddl_queue') IS NOT NULL"):
                result["ddl_queue"] = [
                    dict(r)
                    for r in await tc.fetch(
                        "SELECT id,ddl_log_id,seq,kind,status,attempts,last_error FROM lc.ddl_queue ORDER BY ddl_log_id,seq"
                    )
                ]
                result["new_tables"] = [
                    dict(r)
                    for r in await tc.fetch("SELECT * FROM lc.table_sync ORDER BY schema_name,table_name")
                ]
                result["pending_refresh_age_seconds"] = await tc.fetchval(
                    "SELECT COALESCE(max(extract(epoch FROM clock_timestamp()-created_at)),0)::float8 FROM lc.table_sync WHERE state NOT IN ('ready','removed')"
                )
        return result

    async def drain_ddl(self, sc, tc):
        deadline = asyncio.get_running_loop().time() + self.config.get(
            "limits", "operation_timeout_seconds", 120
        )
        while asyncio.get_running_loop().time() < deadline:
            await ddl_queue.process(self, sc, tc)
            if not await tc.fetchval(
                "SELECT EXISTS(SELECT FROM lc.ddl_queue WHERE status NOT IN ('done','skipped'))"
            ):
                await ddl_queue.require_ready(self, tc)
                return
            await asyncio.sleep(0.1)
        raise PreconditionError("DDL_QUEUE_TIMEOUT", "DDL queue/tablesync did not become ready")

    async def barrier(self, sc, tc, kind="checkpoint"):
        token = uuid.uuid4()
        await sc.execute("INSERT INTO lc.barrier(token,kind) VALUES($1,$2)", token, kind)
        deadline = asyncio.get_running_loop().time() + self.config.get(
            "limits", "operation_timeout_seconds", 120
        )
        while asyncio.get_running_loop().time() < deadline:
            await ddl_queue.process(self, sc, tc)
            if await tc.fetchval("SELECT EXISTS(SELECT FROM lc.barrier WHERE token=$1)", token):
                return str(token)
            await asyncio.sleep(0.1)
        raise PreconditionError("CATCHUP_TIMEOUT", "Replication barrier did not arrive")

    async def block(self, reason, *, details=None):
        state = self.store.read()
        previous = (
            state.get("previous_state", "REPLICATING")
            if state["state"] in {"BLOCKED", "RESUMING"}
            else state["state"]
        )
        await self.state("BLOCKED", previous_state=previous, blocked_reason=reason, blocked_details=details)
        self.store.event("blocked", reason=reason, details=details)

    async def run_once(self):
        with self.store.lock():
            s = self.store.read()
            if s["state"] == "PAUSED":
                async with self.source.connect() as sc, self.target.connect() as tc:
                    await self.controller_lock(sc, tc)
                    await self.owned(sc)
                    await self.owned(tc)
                    await monitoring.paused_tick(self, sc, tc)
            if s["state"] not in {"SYNCING", "CATCHING_UP", "REPLICATING"}:
                monitoring.metrics(self)
                return await self.status()
            async with self.source.connect() as sc, self.target.connect() as tc:
                await self.controller_lock(sc, tc)
                try:
                    await self.owned(sc)
                    await self.owned(tc)
                except OwnershipError as exc:
                    await self.block("OWNERSHIP_ERROR", details=str(exc))
                    monitoring.metrics(self)
                    return await self.status()
                try:
                    if s.get("config_hash") != self.config.control_hash:
                        raise PreconditionError(
                            "CONFIGURATION_CHANGED", "Restore the migration configuration before resuming"
                        )
                    observed = await health.observe(self, sc, tc)
                    current = self.store.read()
                    current["health"] = observed
                    current["heartbeat"] = now()
                    self.store.save(current)
                    for advisory in observed["advisories"]:
                        self.store.event("health_advisory", code=advisory)
                    if observed["blockers"]:
                        raise PreconditionError("HEALTH_BLOCKED", ", ".join(observed["blockers"]))
                    await ddl_queue.process(self, sc, tc)
                    ready = await tc.fetchval(
                        """SELECT count(*)>0 AND bool_and(srsubstate='r') FROM pg_subscription_rel
                        WHERE srsubid=(SELECT oid FROM pg_subscription WHERE subname=$1)""",
                        self.config.subscription,
                    )
                    if ready and s["state"] in {"SYNCING", "CATCHING_UP"}:
                        await self.barrier(sc, tc)
                        if not await tc.fetchval(
                            "SELECT EXISTS(SELECT FROM lc.operations WHERE name='post')"
                        ):
                            await schema.restore(self, s["source_identity"]["major"], "post")
                        await self.state("CATCHING_UP", sc, tc)
                        await self.barrier(sc, tc)
                        await integrity.require(self, sc, tc)
                        current = self.store.read()
                        # Two acknowledged barriers establish a measured upper bound on lag.
                        observed = await health.observe(self, sc, tc)
                        lag = observed["lag_seconds"]
                        if lag is None:
                            lag = 0.0 if not (observed.get("slot") or {}).get("lag_bytes") else None
                        within = lag is not None and lag <= self.config.get(
                            "limits", "catchup_lag_seconds", 10
                        )
                        since = (current.get("catchup_since") or now()) if within else None
                        current["catchup_since"] = since
                        self.store.save(current)
                        if since and (
                            datetime.now(UTC) - datetime.fromisoformat(since)
                        ).total_seconds() >= self.config.get("limits", "catchup_stable_seconds", 0):
                            await self.state("REPLICATING", sc, tc)
                    new_errors = any(
                        observed["errors"].get(k, 0) > (s.get("health") or {}).get("errors", {}).get(k, 0)
                        for k in ("apply_error_count", "sync_error_count")
                    )
                    if self.store.read()["state"] == "REPLICATING" and not new_errors:
                        await monitoring.tick(self, sc, tc)
                    self.store.event("daemon_tick", tables_ready=bool(ready))
                except PreconditionError as exc:
                    if s["state"] == "REPLICATING" and exc.code in {
                        "CATCHUP_TIMEOUT",
                        "VALIDATION_CATCHUP_TIMEOUT",
                    }:
                        self.store.event("background_deferred", code=exc.code)
                    else:
                        await self.block(exc.code, details=str(exc))
                        # Do not write to an endpoint whose identity has changed.
                        if "IDENTITY_CHANGED" not in str(exc):
                            await self.state("BLOCKED", sc, tc)
                except asyncpg.PostgresError as exc:
                    if not (exc.sqlstate or "").startswith("42"):
                        raise
                    await self.block(
                        "DATABASE_SCHEMA_ERROR",
                        details="Cannot inspect database resources; SQLSTATE " + exc.sqlstate,
                    )
                monitoring.metrics(self)
        return await self.status()

    async def cleanup(self, sc, tc):
        s = self.store.read()
        has_source = await self.owned(sc, optional=True)
        has_target = await self.owned(tc, optional=True)
        sub = await tc.fetchrow(
            "SELECT oid::int,subslotname FROM pg_subscription WHERE subname=$1", self.config.subscription
        )
        if sub:
            if not has_source or not has_target or not s.get("subscription_intent"):
                raise OwnershipError("Cannot prove ownership of subscription")
            if s.get("subscription_oid") and sub["oid"] != s["subscription_oid"]:
                raise OwnershipError("Subscription OID changed")
            if sub["subslotname"] != self.config.subscription:
                raise OwnershipError("Subscription slot changed")
            self.store.event("step_intent", step="drop_subscription")
            await tc.execute("ALTER SUBSCRIPTION " + qi(self.config.subscription) + " DISABLE")
            await tc.execute("DROP SUBSCRIPTION " + qi(self.config.subscription))
        if s.get("resync"):
            from .resync import restore_for_abort

            await restore_for_abort(self, tc)
        if has_source:
            if await sc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", self.config.subscription
            ):
                raise OwnershipError("Unattached slot remains; automatic deletion is unsafe")
            pub = await sc.fetchval(
                "SELECT oid::int FROM pg_publication WHERE pubname=$1", self.config.publication
            )
            if pub:
                receipt = await sc.fetchval("SELECT details FROM lc.operations WHERE name='publication'")
                if not receipt or json.loads(receipt).get("oid") != pub:
                    raise OwnershipError("Publication lacks receipt")
                await sc.execute("DROP PUBLICATION " + qi(self.config.publication))
            changed = await sc.fetchval("SELECT details FROM lc.operations WHERE name='replica_identity'")
            if changed:
                for ns, name in json.loads(changed)["changed"]:
                    await sc.execute("ALTER TABLE " + qn(ns, name) + " REPLICA IDENTITY DEFAULT")
            if await sc.fetchval("SELECT EXISTS(SELECT FROM pg_event_trigger WHERE evtname='lc_guard')"):
                await sc.execute("DROP EVENT TRIGGER lc_guard")
                await sc.execute("DROP FUNCTION lc.guard()")
                await sc.execute("DROP FUNCTION lc.guard_check(text,text,text,text)")
            if await sc.fetchval("SELECT EXISTS(SELECT FROM pg_event_trigger WHERE evtname='lc_capture')"):
                await sc.execute("DROP EVENT TRIGGER lc_capture")
                await sc.execute("DROP FUNCTION lc.capture()")
            await sc.execute("DROP EVENT TRIGGER IF EXISTS lc_rewrite_guard")
            await sc.execute("DROP FUNCTION IF EXISTS lc.rewrite_guard()")
            await sc.execute("DROP FUNCTION IF EXISTS lc.record_ddl(text,text,jsonb,jsonb)")
            await sc.execute("DROP FUNCTION IF EXISTS lc.clean_sql(text)")
            await sc.execute("DROP FUNCTION IF EXISTS lc.top_comma(text)")
            await sc.execute("DROP FUNCTION IF EXISTS lc.resolve_relation(text,text)")
        replay = (
            await tc.fetchval("SELECT details FROM lc.operations WHERE name='replay'") if has_target else None
        )
        if replay:
            replay = json.loads(replay)
            if (
                await tc.fetchval("SELECT oid::int FROM pg_roles WHERE rolname=$1", replay["role"])
                != replay["oid"]
            ):
                raise OwnershipError("DDL executor role identity changed")
        for c, owned in [(sc, has_source), (tc, has_target)]:
            if owned:
                async with c.transaction():
                    await c.execute(
                        "DROP TABLE lc.ddl_log,lc.ddl_applied,lc.ddl_queue,lc.table_sync,lc.barrier,lc.operations,lc.control"
                    )
                    if c is tc and replay:
                        await c.execute("DROP FUNCTION lc.replay()")
                    if c is tc and replay:
                        for ns in replay["schemas"]:
                            await c.execute("REVOKE ALL ON SCHEMA " + qi(ns) + " FROM " + qi(replay["role"]))
                        await c.execute("REVOKE ALL ON SCHEMA lc FROM " + qi(replay["role"]))
                        await c.execute("DROP ROLE " + qi(replay["role"]))
                    await c.execute("DROP SCHEMA lc")

    async def abort(self, path, plan_hash):
        from . import emergency

        if emergency.is_offline(path):
            return await emergency.execute(self, "abort", path, plan_hash)
        allowed = {
            "NEW",
            "PREPARING",
            "PREPARED",
            "STARTING",
            "SYNCING",
            "CATCHING_UP",
            "REPLICATING",
            "PAUSED",
            "BLOCKED",
            "ABORTING",
            "PAUSING",
            "RESUMING",
            "RESYNCING",
        }
        async with self.mutation("abort", path, plan_hash, states=allowed) as (sc, tc, plan):
            await self.state("ABORTING")
            await self.cleanup(sc, tc)
            await self.state("ABORTED", active_plan=None)
        return await self.status()

    async def verify(self, *, level=None, table=None):
        from .validation import verify

        async with self.source.connect(readonly=True) as sc, self.target.connect(readonly=True) as tc:
            report = await verify(self, sc, tc, level=level, table=table)
            if self.store.read().get("role_baseline"):
                report["integrity"] = await integrity.inspect(self, sc, tc)
                report["passed"] = (
                    report["passed"] and report["integrity"]["passed"] and report["integrity"]["settled"]
                )
            return report

    async def cutover_finalize(self, path, plan_hash, write_fence):
        from .cutover import finalize

        return await finalize(self, path, plan_hash, write_fence)

    async def cutover_commit(self, path, plan_hash):
        from . import emergency
        from .cutover import commit

        if emergency.is_offline(path):
            return await emergency.execute(self, "cutover-commit", path, plan_hash)

        return await commit(self, path, plan_hash)

    async def cutover_abort(self, reason):
        if not reason or not reason.strip():
            raise PreconditionError("REASON_REQUIRED", "A reason is required")
        with self.store.lock():
            if self.store.read()["state"] not in {"FINALIZING", "CUTOVER_READY"}:
                raise PreconditionError("INVALID_STATE", "Cutover abort requires FINALIZING or CUTOVER_READY")
            async with self.source.connect() as sc, self.target.connect() as tc:
                await self.controller_lock(sc, tc)
                await self.owned(sc)
                await self.owned(tc)
                self.store.event("cutover_abort", reason=reason)
                await self.state("REPLICATING", sc, tc, active_plan=None, write_fence=None)
        return await self.status()
