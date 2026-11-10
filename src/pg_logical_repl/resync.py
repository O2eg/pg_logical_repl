"""Resynchronize one ordinary table using native tablesync and durable step receipts."""

import asyncio
import json

from . import ddl_queue, health, integrity, validation
from .errors import OwnershipError, PreconditionError
from .security import identifier as qi
from .security import literal as ql
from .security import qualified as qn


async def prepare_plan(service, sc, tc, details):
    details = dict(details or {})
    if not details.get("reason") or not details.get("table"):
        raise PreconditionError("RESYNC_ARGUMENT", "resync-table requires a qualified table and reason")
    query = """SELECT c.oid::int,n.nspname AS schema,c.relname AS name,c.relkind::text,c.relispartition,
        c.relpersistence::text,EXISTS(SELECT FROM pg_depend WHERE classid='pg_class'::regclass
        AND objid=c.oid AND deptype='e') AS extension FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid=to_regclass($1)"""
    source, target = await sc.fetchrow(query, details["table"]), await tc.fetchrow(query, details["table"])
    if (
        not source
        or not target
        or any(
            r["relkind"] != "r"
            or r["relispartition"]
            or r["relpersistence"] != "p"
            or r["extension"]
            or r["schema"] == "lc"
            or r["schema"].startswith("pg_")
            for r in (source, target)
        )
    ):
        raise PreconditionError(
            "RESYNC_PROFILE",
            "Only ordinary logged application tables are supported; partition trees require a fresh migration",
        )
    name = qn(source["schema"], source["name"])
    if qn(target["schema"], target["name"]) != name:
        raise PreconditionError("RESYNC_TABLE", "Qualified source and target names differ")
    fks = [
        dict(r)
        for r in await tc.fetch(
            """SELECT c.oid::int,c.conrelid::int AS relation_oid,c.conname,c.convalidated,
        pg_get_constraintdef(c.oid) AS definition,obj_description(c.oid,'pg_constraint') AS comment,n.nspname AS schema,t.relname AS name
        FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE c.contype='f' AND c.confrelid=$1 ORDER BY n.nspname,t.relname,c.conname""",
            target["oid"],
        )
    ]
    if any("PARTITION" in r["definition"] for r in fks) or await tc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_constraint WHERE contype='f' AND confrelid=$1 AND conparentid<>0)",
        target["oid"],
    ):
        raise PreconditionError("RESYNC_PROFILE", "Partitioned foreign keys require a fresh migration")
    return dict(details, table=name, source_oid=source["oid"], target_oid=target["oid"], foreign_keys=fks)


async def publication_receipt(service, sc):
    tables = [
        [r["schema"], r["name"]]
        for r in await sc.fetch(
            """SELECT n.nspname AS schema,c.relname AS name
        FROM pg_publication_rel p JOIN pg_class c ON c.oid=p.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE p.prpubid=(SELECT oid FROM pg_publication WHERE pubname=$1) ORDER BY 1,2""",
            service.config.publication,
        )
    ]
    await sc.execute(
        "UPDATE lc.operations SET details=jsonb_set(details,'{tables}',$1::jsonb) WHERE name='publication'",
        json.dumps(tables),
    )


async def execute(service, path, plan_hash):
    async with service.mutation(
        "resync-table", path, plan_hash, states={"REPLICATING", "BLOCKED", "RESYNCING"}
    ) as (sc, tc, plan):
        details = plan["extra"]
        await service.owned(sc)
        await service.owned(tc)
        state = service.store.read()
        for side, c in [("source", sc), ("target", tc)]:
            if (
                await c.fetchval("SELECT to_regclass($1)::oid::int", details["table"])
                != details[side + "_oid"]
            ):
                raise OwnershipError("Resync relation was replaced on " + side)
        prefix = "resync:" + plan_hash + ":"
        if state["state"] != "RESYNCING":
            for fk in details["foreign_keys"]:
                if (
                    await tc.fetchval(
                        "SELECT oid::int FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname=$2",
                        qn(fk["schema"], fk["name"]),
                        fk["conname"],
                    )
                    != fk["oid"]
                ):
                    raise OwnershipError("Referencing foreign key was replaced after the resync plan")
            observed = await health.observe(service, sc, tc, ignore_history=True)
            if observed["blockers"]:
                raise PreconditionError("RESYNC_HEALTH", ", ".join(observed["blockers"]))
            await ddl_queue.require_ready(service, tc)
            await integrity.require(service, sc, tc)
            await service.state("RESYNCING", sc, tc, resync=dict(details, plan_hash=plan_hash))
        # The source guard freezes application DDL for the entire interrupted operation.
        added = await sc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name=$1)", prefix + "added")
        if not added:
            if not await sc.fetchval(
                "SELECT EXISTS(SELECT FROM lc.operations WHERE name=$1)", prefix + "removed"
            ):
                async with sc.transaction():
                    await sc.execute(
                        "ALTER PUBLICATION "
                        + qi(service.config.publication)
                        + " DROP TABLE "
                        + details["table"]
                    )
                    await publication_receipt(service, sc)
                    await sc.execute("INSERT INTO lc.operations(name) VALUES($1)", prefix + "removed")
            await tc.execute(
                "ALTER SUBSCRIPTION "
                + qi(service.config.subscription)
                + " REFRESH PUBLICATION WITH (copy_data=false)"
            )
            if not await tc.fetchval(
                "SELECT EXISTS(SELECT FROM lc.operations WHERE name=$1)", prefix + "truncated"
            ):
                async with tc.transaction():
                    for fk in details["foreign_keys"]:
                        await tc.execute(
                            "ALTER TABLE "
                            + qn(fk["schema"], fk["name"])
                            + " DROP CONSTRAINT "
                            + qi(fk["conname"])
                        )
                    await tc.execute("TRUNCATE ONLY " + details["table"])
                    await tc.execute("INSERT INTO lc.operations(name) VALUES($1)", prefix + "truncated")
            async with sc.transaction():
                await sc.execute(
                    "ALTER PUBLICATION " + qi(service.config.publication) + " ADD TABLE " + details["table"]
                )
                await publication_receipt(service, sc)
                await sc.execute("INSERT INTO lc.operations(name) VALUES($1)", prefix + "added")
        await tc.execute(
            "ALTER SUBSCRIPTION "
            + qi(service.config.subscription)
            + " REFRESH PUBLICATION WITH (copy_data=true)"
        )
        deadline = asyncio.get_running_loop().time() + service.config.get(
            "limits", "operation_timeout_seconds", 120
        )
        while not await tc.fetchval(
            """SELECT srsubstate='r' FROM pg_subscription_rel
            WHERE srsubid=$1 AND srrelid=$2""",
            state["subscription_oid"],
            details["target_oid"],
        ):
            if asyncio.get_running_loop().time() >= deadline:
                raise PreconditionError("RESYNC_TIMEOUT", "Retry the same resync plan to continue tablesync")
            await asyncio.sleep(0.1)
        if not await tc.fetchval(
            "SELECT EXISTS(SELECT FROM lc.operations WHERE name=$1)", prefix + "constraints"
        ):
            async with tc.transaction():
                for fk in details["foreign_keys"]:
                    if (
                        await tc.fetchval("SELECT to_regclass($1)::oid::int", qn(fk["schema"], fk["name"]))
                        != fk["relation_oid"]
                    ):
                        raise OwnershipError("Referencing table was replaced during resync")
                    definition = fk["definition"].removesuffix(" NOT VALID")
                    await tc.execute(
                        "ALTER TABLE "
                        + qn(fk["schema"], fk["name"])
                        + " ADD CONSTRAINT "
                        + qi(fk["conname"])
                        + " "
                        + definition
                        + " NOT VALID"
                    )
                    if fk["comment"] is not None:
                        await tc.execute(
                            "COMMENT ON CONSTRAINT "
                            + qi(fk["conname"])
                            + " ON "
                            + qn(fk["schema"], fk["name"])
                            + " IS "
                            + ql(fk["comment"])
                        )
                await tc.execute("INSERT INTO lc.operations(name) VALUES($1)", prefix + "constraints")
        for fk in details["foreign_keys"]:
            if fk["convalidated"]:
                await tc.execute(
                    "ALTER TABLE "
                    + qn(fk["schema"], fk["name"])
                    + " VALIDATE CONSTRAINT "
                    + qi(fk["conname"])
                )
        await service.barrier(sc, tc)
        await integrity.require(service, sc, tc)
        report = await validation.verify(service, sc, tc)
        if not report["passed"]:
            raise PreconditionError(
                "RESYNC_VALIDATION", "Data still differs; retry or repair before resuming"
            )
        observed = await health.observe(service, sc, tc, ignore_history=True)
        if observed["blockers"]:
            raise PreconditionError("RESYNC_HEALTH", ", ".join(observed["blockers"]))
        await service.state(
            "REPLICATING",
            sc,
            tc,
            active_plan=None,
            resync=None,
            blocked_reason=None,
            blocked_details=None,
            health=observed,
            integrity=None,
            validation_cursor=None,
        )
        service.store.event("table_resynchronized", table=details["table"], reason=details["reason"])
    return report


async def restore_for_abort(service, tc):
    """Retain FK definitions even when an interrupted copy leaves incomplete data."""
    state = service.store.read()
    details = state.get("resync")
    if not details:
        return
    if await tc.fetchval("SELECT to_regclass($1)::oid::int", details["table"]) != details["target_oid"]:
        raise OwnershipError("Resync table was replaced before abort")
    restored = []
    async with tc.transaction():
        for fk in details["foreign_keys"]:
            table = qn(fk["schema"], fk["name"])
            if await tc.fetchval("SELECT to_regclass($1)::oid::int", table) != fk["relation_oid"]:
                raise OwnershipError("Referencing table was replaced before abort")
            actual = await tc.fetchval(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname=$2",
                table,
                fk["conname"],
            )
            definition = fk["definition"].removesuffix(" NOT VALID")
            if actual is not None:
                if actual.removesuffix(" NOT VALID") != definition:
                    raise OwnershipError("Foreign key changed before abort")
                continue
            await tc.execute(
                "ALTER TABLE "
                + table
                + " ADD CONSTRAINT "
                + qi(fk["conname"])
                + " "
                + definition
                + " NOT VALID"
            )
            if fk["comment"] is not None:
                await tc.execute(
                    "COMMENT ON CONSTRAINT " + qi(fk["conname"]) + " ON " + table + " IS " + ql(fk["comment"])
                )
            restored.append(dict(table=table, constraint=fk["conname"], validated=False))
    state["resync_abort_constraints"] = state.get("resync_abort_constraints", []) + restored
    service.store.save(state)
