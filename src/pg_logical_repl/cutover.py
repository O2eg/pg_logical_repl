"""Two-phase detachment with a required external write fence."""

import asyncio
import json
import re
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from . import integrity
from .catalog import fingerprint, inventory
from .ddl_queue import require_ready
from .errors import PreconditionError
from .health import require_healthy
from .process import execute, pg_environment, tool
from .security import qualified as qn
from .state import write_private
from .validation import large_objects, sequences, verify


def fence(service, path):
    try:
        data = json.loads(Path(path).read_text())
        age = (datetime.now(UTC) - datetime.fromisoformat(data["created_at"])).total_seconds()
        valid = (
            data["migration_id"] == service.config.migration_id
            and data["source_identity"] == service.store.read()["source_identity"]
            and 0 <= age <= service.config.get("cutover", "max_validation_age_seconds", 21600)
            and bool(data["responsible"])
            and bool(data["external_fence"])
        )
    except (OSError, ValueError, KeyError, TypeError):
        valid = False
    if not valid:
        raise PreconditionError(
            "WRITE_FENCE_INVALID", "A fresh external write-fence record matching source identity is required"
        )
    return data


@asynccontextmanager
async def locked_source(service, sc):
    prepared = await sc.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()")
    writers = await sc.fetchval("""SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
        AND pid<>pg_backend_pid() AND backend_type='client backend' AND xact_start IS NOT NULL
        AND application_name NOT LIKE 'pg-logical-repl/%' """)
    if prepared or writers:
        raise PreconditionError("WRITERS_NOT_DRAINED", "Prepared or open application transactions remain")
    async with sc.transaction():
        si = await inventory(sc)
        if service.config.get("cutover", "share_locks", True):
            for r in si["relations"]:
                if r["kind"] in ("r", "p") and not r["extension"]:
                    await sc.execute("LOCK TABLE ONLY " + qn(r["schema"], r["name"]) + " IN SHARE MODE")
        yield


async def deferred_copy(service, sc, tc):
    si = await inventory(sc)
    major = si["identity"]["major"]
    unlogged = [
        r for r in si["relations"] if r["kind"] == "r" and r["persistence"] == "u" and not r["extension"]
    ]
    # Include all LO metadata/ACL and unlogged data in one archive. Logged rows remain native tablesync's job.
    archive = service.store.directory / "deferred.dump"
    args = [
        tool(service.config, major, "pg_dump"),
        "--format=custom",
        "--data-only",
        "-b",
        "--file=" + str(archive),
    ]
    if unlogged:
        args += ["--table=" + qn(r["schema"], r["name"]) for r in unlogged]
    else:
        args += ["--exclude-table-data=*.*"]
    with pg_environment(service.source, service.store.directory) as env:
        await execute(args, env=env, redactor=service.redactor)
    archive.chmod(0o600)
    toc = await execute(
        [tool(service.config, major, "pg_restore"), "--list", str(archive)], redactor=service.redactor
    )
    keep = [
        line
        for line in toc.splitlines()
        if re.search(r"; \d+ \d+ (TABLE DATA|BLOB|BLOBS|BLOB COMMENTS|LARGE OBJECT)", line)
        or re.search(r"; \d+ \d+ (ACL|COMMENT|SECURITY LABEL) - (LARGE OBJECT|BLOB)", line)
    ]
    if not unlogged:
        keep = [line for line in keep if not re.search(r"; \d+ \d+ TABLE DATA ", line)]
    listing = service.store.directory / "deferred.list"
    write_private(listing, "\n".join(keep) + "\n")
    output = service.store.directory / "deferred.sql"
    await execute(
        [tool(service.config, major, "pg_restore"), "--file=-", "--use-list=" + str(listing), str(archive)],
        stdout_path=output,
        redactor=service.redactor,
    )
    cleanup = []
    if unlogged:
        cleanup.append("TRUNCATE " + ",".join(qn(r["schema"], r["name"]) for r in unlogged) + ";")
    # Empty target was verified before bootstrap; its LO set is migration-owned until detachment.
    cleanup.append("SELECT pg_catalog.lo_unlink(oid) FROM pg_catalog.pg_largeobject_metadata;")
    before = service.store.directory / "deferred-before.sql"
    write_private(before, "SET LOCAL session_replication_role=replica;\n" + "\n".join(cleanup))
    with pg_environment(service.target, service.store.directory) as env:
        await execute(
            [
                tool(service.config, major, "psql"),
                "-X",
                "-w",
                "--quiet",
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                "--file",
                str(before),
                "--file",
                str(output),
            ],
            env=env,
            redactor=service.redactor,
        )
    lo_source, lo_target = await large_objects(sc), await large_objects(tc)
    if lo_source != lo_target:
        raise PreconditionError("LARGE_OBJECT_MISMATCH", "Large object content, OID or privileges differ")
    return dict(unlogged_tables=len(unlogged), large_objects=len(lo_source))


async def refresh_matviews(service, sc, tc):
    rows = await sc.fetch("""SELECT c.oid::int,n.nspname AS schema,c.relname AS name,c.relispopulated
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='m'
        AND n.nspname !~ '^pg_' AND n.nspname<>'lc' ORDER BY 2,3""")
    dependencies = await sc.fetch("""SELECT DISTINCT r.ev_class::int AS view,d.refobjid::int AS dependency
        FROM pg_rewrite r JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid
        JOIN pg_class c ON c.oid=d.refobjid WHERE c.relkind='m' AND d.refobjid<>r.ev_class""")
    remaining = {r["oid"]: r for r in rows}
    done = []
    while remaining:
        ready = [
            r
            for oid, r in remaining.items()
            if not any(d["view"] == oid and d["dependency"] in remaining for d in dependencies)
        ]
        if not ready:
            raise PreconditionError("MATVIEW_DEPENDENCY_CYCLE", "Cannot determine refresh order")
        for r in ready:
            if r["relispopulated"]:
                await tc.execute("REFRESH MATERIALIZED VIEW " + qn(r["schema"], r["name"]))
                done.append(qn(r["schema"], r["name"]))
            remaining.pop(r["oid"])
    return done


async def finalize(service, path, plan_hash, write_fence):
    async with service.mutation(
        "cutover-finalize", path, plan_hash, states={"REPLICATING", "FINALIZING"}
    ) as (sc, tc, plan):
        await service.owned(sc)
        await service.owned(tc)
        await require_healthy(service, sc, tc)
        await require_ready(service, tc)
        await service.barrier(sc, tc)
        await integrity.require(service, sc, tc)
        evidence = fence(service, write_fence)
        await service.state("FINALIZING", sc, tc, write_fence=evidence)
        async with locked_source(service, sc):
            async with service.source.connect() as marker:
                barrier = await service.barrier(marker, tc, "cutover")
            seq_before = await sequences(sc, tc)
            await asyncio.sleep(2)
            await deferred_copy(service, sc, tc)
            seq = await sequences(sc, tc, apply=True)
            matviews = await refresh_matviews(service, sc, tc)
            report = await verify(service, sc, tc, include_deferred=True)
            await integrity.require(service, sc, tc)
            seq_after = await sequences(sc, tc)
            if any(a["source"] != b["source"] for a, b in zip(seq_before, seq_after)):
                raise PreconditionError(
                    "SEQUENCE_WRITER", "Source sequences changed under the external fence"
                )
            if not report["passed"] or not all(x["passed"] for x in seq_after):
                raise PreconditionError("FINAL_VALIDATION_FAILED", "Source and target do not match")
            report.update(
                authoritative=True,
                write_fence=evidence,
                barrier=barrier,
                sequences=seq,
                matviews_refreshed=matviews,
            )
            report["large_objects"] = await large_objects(sc)
            report["target_schema"] = fingerprint(await inventory(tc))
            report["interventions"] = service.store.read().get("interventions", [])
            if service.config.get("cutover", "analyze", "never") == "all":
                for row in (await inventory(tc))["relations"]:
                    if row["kind"] in ("r", "p", "m") and not row["extension"]:
                        await tc.execute("ANALYZE " + qn(row["schema"], row["name"]))
            report["analyze"] = service.config.get("cutover", "analyze", "never")
            write_private(service.store.directory / "reports/cutover.json", json.dumps(report, indent=2))
        await service.state("CUTOVER_READY", sc, tc, active_plan=None)
    return report


async def commit(service, path, plan_hash):
    async with service.mutation(
        "cutover-commit", path, plan_hash, states={"CUTOVER_READY", "COMMITTING"}
    ) as (sc, tc, plan):
        state = service.store.read()
        if state["state"] == "CUTOVER_READY":
            await service.owned(sc)
            await service.owned(tc)
            await require_healthy(service, sc, tc)
            await require_ready(service, tc)
            saved = service.store.directory / "write-fence.json"
            write_private(saved, json.dumps(state["write_fence"]))
            fence(service, saved)
            async with locked_source(service, sc):
                async with service.source.connect() as marker:
                    await service.barrier(marker, tc, "pre-detach")
                report = await verify(service, sc, tc, include_deferred=True)
                await integrity.require(service, sc, tc)
                baseline = json.loads((service.store.directory / "reports/cutover.json").read_text())
                if [(r["table"], r["source"]) for r in report["tables"]] != [
                    (r["table"], r["source"]) for r in baseline["tables"]
                ]:
                    raise PreconditionError("FENCE_VIOLATED", "Source table contents changed after finalize")
                if not report["passed"] or not all(x["passed"] for x in await sequences(sc, tc)):
                    raise PreconditionError("CUTOVER_CHANGED", "Data or sequences changed after finalize")
                if await large_objects(sc) != baseline["large_objects"] or await large_objects(
                    sc
                ) != await large_objects(tc):
                    raise PreconditionError("CUTOVER_CHANGED", "Large objects changed after finalize")
                await service.state("COMMITTING", tc=tc)
                # Source lock transaction cannot contain DROP SUBSCRIPTION (which is on target).
                await service.cleanup(sc, tc)
        else:
            await service.cleanup(sc, tc)
        await service.state("DETACHED", active_plan=None)
    return await service.status()
