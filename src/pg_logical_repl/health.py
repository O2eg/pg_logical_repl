"""Native replication health checks; never repair or discard WAL implicitly."""

import json
from datetime import UTC, datetime

from .db import identity
from .errors import PreconditionError
from .security import conninfo
from .state import now

ERROR_WINDOW_SECONDS = 300


async def require_healthy(service, sc, tc):
    result = await observe(service, sc, tc)
    if result["blockers"]:
        raise PreconditionError("HEALTH_BLOCKED", ", ".join(result["blockers"]))
    return result


async def observe(service, sc, tc, *, allow_disabled=False, ignore_history=False):
    state = service.store.read()
    blockers, advisories = [], []

    def require(condition, code):
        if not condition:
            blockers.append(code)

    for side, c in [("source", sc), ("target", tc)]:
        require(await identity(c) == state[side + "_identity"], side.upper() + "_IDENTITY_CHANGED")
    sub = await tc.fetchrow(
        """SELECT oid::int,subenabled,subslotname,subpublications,subbinary,
        substream::text,subsynccommit,subconninfo,subdisableonerr,
        to_jsonb(pg_subscription)->>'subrunasowner' AS run_as_owner
        FROM pg_subscription WHERE subname=$1""",
        service.config.subscription,
    )
    require(sub is not None, "SUBSCRIPTION_MISSING")
    errors = None
    if sub:
        require(sub["oid"] == state.get("subscription_oid"), "SUBSCRIPTION_IDENTITY_CHANGED")
        values, _ = service.source.libpq(replication=True)
        require(
            (sub["subenabled"] or allow_disabled)
            and sub["subslotname"] == service.config.subscription
            and sub["subpublications"] == [service.config.publication]
            and not sub["subbinary"]
            # PG15 stores boolean; PG16+ stores a char to include parallel mode.
            and sub["substream"] in {"t", "true"}
            and sub["subsynccommit"] == "on"
            and not sub["subdisableonerr"]
            and (state["source_identity"]["major"] < 16 or sub["run_as_owner"] == "true")
            and sub["subconninfo"] == conninfo(values),
            "SUBSCRIPTION_CONFIGURATION_CHANGED",
        )
        errors = await tc.fetchrow(
            "SELECT apply_error_count,sync_error_count,to_jsonb(s) AS all_stats FROM pg_stat_subscription_stats s WHERE subid=$1",
            sub["oid"],
        )
    pub = await sc.fetchrow(
        """SELECT oid::int,puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot
        FROM pg_publication WHERE pubname=$1""",
        service.config.publication,
    )
    receipt_text = await sc.fetchval("SELECT details FROM lc.operations WHERE name='publication'")
    receipt = json.loads(receipt_text) if receipt_text else {}
    require(pub is not None, "PUBLICATION_MISSING")
    if pub:
        require(pub["oid"] == receipt.get("oid"), "PUBLICATION_IDENTITY_CHANGED")
        require(
            not pub["puballtables"]
            and not pub["pubviaroot"]
            and all(pub[k] for k in ("pubinsert", "pubupdate", "pubdelete", "pubtruncate")),
            "PUBLICATION_CONFIGURATION_CHANGED",
        )
        tables = [
            [r["schema"], r["name"]]
            for r in await sc.fetch(
                """SELECT n.nspname AS schema,c.relname AS name FROM pg_publication_rel r
                JOIN pg_class c ON c.oid=r.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE r.prpubid=$1 ORDER BY 1,2""",
                pub["oid"],
            )
        ]
        require(tables == receipt.get("tables"), "PUBLICATION_MEMBERSHIP_CHANGED")
        require(
            not await sc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_publication_namespace WHERE pnpubid=$1)", pub["oid"]
            ),
            "PUBLICATION_SCHEMAS_CHANGED",
        )
        require(
            not await sc.fetchval(
                """SELECT EXISTS(SELECT FROM pg_publication_rel
                WHERE prpubid=$1 AND (prattrs IS NOT NULL OR prqual IS NOT NULL))""",
                pub["oid"],
            ),
            "PUBLICATION_FILTER_CHANGED",
        )
    slot = await sc.fetchrow(
        """SELECT slot_name,slot_type,plugin,database,active,wal_status,
        pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint AS retained_bytes,
        pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint AS lag_bytes,
        confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1""",
        service.config.subscription,
    )
    require(slot is not None, "SLOT_MISSING_RESYNC_REQUIRED")
    if slot:
        require(slot["wal_status"] != "lost", "SLOT_LOST_RESYNC_REQUIRED")
        require(
            slot["slot_type"] == "logical"
            and slot["plugin"] == "pgoutput"
            and slot["database"] == state["source_identity"]["database"],
            "SLOT_CONFIGURATION_CHANGED",
        )
        threshold = service.config.get("limits", "alert_slot_retained_bytes", 8 * 1024**3)
        if slot["retained_bytes"] is not None and slot["retained_bytes"] >= threshold:
            advisories.append("SLOT_WAL_RETENTION_HIGH")
        if slot["wal_status"] == "unreserved":
            advisories.append("SLOT_WAL_AT_RISK")
    health = dict(
        checked_at=now(),
        blockers=blockers,
        advisories=advisories,
        errors=dict(errors) if errors else {},
        slot=dict(slot) if slot else None,
    )
    lag = await sc.fetchval(
        """SELECT max(extract(epoch FROM replay_lag))::float8
        FROM pg_stat_replication WHERE application_name=$1""",
        service.config.subscription,
    )
    health["lag_seconds"] = lag
    if slot and slot["lag_bytes"] == 0:
        health["lag_seconds"] = 0.0
    if health["lag_seconds"] is not None and health["lag_seconds"] > service.config.get(
        "limits", "alert_lag_seconds", 60
    ):
        advisories.append("REPLICATION_LAG_HIGH")
    for key, table, predicate, option in [
        ("queue_age_seconds", "ddl_queue", "status NOT IN ('done','skipped')", "alert_queue_age_seconds"),
        (
            "refresh_age_seconds",
            "table_sync",
            "state NOT IN ('ready','removed')",
            "alert_refresh_age_seconds",
        ),
    ]:
        age = await tc.fetchval(
            "SELECT COALESCE(max(extract(epoch FROM clock_timestamp()-created_at)),0)::float8 FROM lc."
            + table
            + " WHERE "
            + predicate
        )
        health[key] = age
        if age > service.config.get("limits", option, 300):
            advisories.append(option.upper())
    previous = {} if ignore_history else state.get("health", {})
    application_sessions = await tc.fetchval("""SELECT count(*) FROM pg_stat_activity a JOIN pg_roles r ON r.oid=a.usesysid
        WHERE a.datname=current_database() AND a.backend_type='client backend' AND NOT r.rolsuper
        AND a.application_name NOT LIKE 'pg-logical-repl/%'""")
    if application_sessions:
        blockers.append("TARGET_APPLICATION_SESSIONS")
    stats = json.loads(health["errors"].pop("all_stats", "{}"))
    health["conflicts"] = {k: v for k, v in stats.items() if k.startswith("confl_")}
    for key in ("confl_update_missing", "confl_delete_missing"):
        if not ignore_history and health["conflicts"].get(key, 0) > previous.get("conflicts", {}).get(key, 0):
            blockers.append(key.upper())
    timestamp = datetime.now(UTC)
    # Historical nonzero counters alone are not a failure. Block only on recurring
    # errors spanning the window; a gap of a full window starts a new observation.
    for counter in ("apply_error_count", "sync_error_count"):
        value = health["errors"].get(counter, 0)
        old_value = previous.get("errors", {}).get(counter, 0)
        tracking = previous.get(counter + "_window")
        if (
            tracking
            and (timestamp - datetime.fromisoformat(tracking["last"])).total_seconds() > ERROR_WINDOW_SECONDS
        ):
            tracking = None
        if value < old_value:
            tracking = None
        if value > old_value:
            tracking = tracking or dict(first=health["checked_at"], increments=0)
            tracking.update(last=health["checked_at"], increments=tracking["increments"] + 1)
            if (
                tracking["increments"] > 1
                and (timestamp - datetime.fromisoformat(tracking["first"])).total_seconds()
                >= ERROR_WINDOW_SECONDS
            ):
                blockers.append("PERSISTENT_" + counter.upper())
        if tracking:
            health[counter + "_window"] = tracking
    return health
