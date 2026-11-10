"""Server-side, order-independent checksums; Python receives aggregates and PK bounds only."""

import asyncio
import json
import uuid
from contextlib import asynccontextmanager

from .catalog import difference, fingerprint, inventory, schema_manifest
from .errors import PreconditionError
from .security import identifier as qi
from .security import qualified as qn
from .state import now, write_private

# MD5 is a comparison checksum, not a cryptographic proof. Convert the full
# unsigned 128-bit digest to numeric before summing (no bigint overflow).
MD5_NUMBER = " + ".join(
    f"(('x'||substr(h.digest,{start},8))::bit(32)::bigint::numeric * {factor})"
    for start, factor in [(1, 2**96), (9, 2**64), (17, 2**32), (25, 1)]
)
FIXED_GUCS = """SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,YMD';
SET LOCAL IntervalStyle='postgres'; SET LOCAL extra_float_digits=3;
SET LOCAL bytea_output='hex'; SET LOCAL lc_monetary='C'; SET LOCAL search_path=pg_catalog;
SET LOCAL max_parallel_workers_per_gather=0;"""


@asynccontextmanager
async def snapshot(c):
    outer = c.is_in_transaction()
    options = {} if outer else dict(isolation="repeatable_read", readonly=True)
    async with c.transaction(**options):
        previous = None
        if outer:
            previous = await c.fetch(
                "SELECT name,current_setting(name) AS value FROM unnest($1::text[]) name",
                [
                    "TimeZone",
                    "DateStyle",
                    "IntervalStyle",
                    "extra_float_digits",
                    "bytea_output",
                    "lc_monetary",
                    "search_path",
                    "max_parallel_workers_per_gather",
                ],
            )
        await c.execute(FIXED_GUCS)
        yield
        if previous is not None:
            # Releasing a savepoint does NOT undo SET LOCAL. Preserve the outer
            # cutover transaction's catalog/deparser context. On error the nested
            # transaction rollback already restores these settings.
            await c.execute(
                "SELECT set_config(name,value,true) FROM unnest($1::text[],$2::text[]) v(name,value)",
                [r["name"] for r in previous],
                [r["value"] for r in previous],
            )


async def primary_key(c, name):
    return [
        r["attname"]
        for r in await c.fetch(
            """SELECT a.attname FROM pg_index i
            CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
            JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
            WHERE i.indrelid=$1::regclass AND i.indisprimary AND i.indisvalid
              AND k.ord<=i.indnkeyatts ORDER BY k.ord""",
            name,
        )
    ]


def range_clause(name, keys, lower, upper):
    clauses, params = [], []
    lhs = "(" + ",".join("t." + qi(k) for k in keys) + ")"
    rhs = ",".join("b." + qi(k) for k in keys)
    for boundary, operator in [(lower, ">"), (upper, "<=")]:
        if boundary is not None:
            params.append(boundary)
            clauses.append(
                lhs
                + operator
                + f"(SELECT {rhs} FROM jsonb_populate_record(NULL::{name},${len(params)}::jsonb) b)"
            )
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), params


def boundary_query(name, keys, lower, chunk_rows):
    where, params = range_clause(name, keys, lower, None)
    ordering = ",".join("t." + qi(k) for k in keys)
    params.append(chunk_rows - 1)
    # Offset is bounded by one chunk and applied after the previous PK bound.
    # The PK index supplies the order; payload columns are never sorted.
    return (
        f"SELECT to_jsonb(pk)::text FROM (SELECT {ordering} FROM {name} t{where} "
        f"ORDER BY {ordering} OFFSET ${len(params)} LIMIT 1) pk",
        params,
    )


def digest_query(name, columns, keys=(), lower=None, upper=None, *, level="checksums"):
    where, params = range_clause(name, keys, lower, upper)
    if level == "counts":
        return f"SELECT count(*)::bigint AS count FROM {name} t{where}", params
    fields = ",".join("t." + qi(column) for column in columns)
    # OFFSET 0 is an optimization fence, not materialization: one md5 per row,
    # no whole-table sort and no hash string/row array accumulated in memory.
    return (
        f"SELECT count(*)::bigint AS count, COALESCE(sum({MD5_NUMBER}),0)::text AS md5_sum "
        f"FROM (SELECT md5(ROW({fields})::text) AS digest FROM {name} t{where} OFFSET 0) h",
        params,
    )


async def digest_range(c, name, columns, keys, lower, upper, level):
    query, params = digest_query(name, columns, keys, lower, upper, level=level)
    return dict(await c.fetchrow(query, *params))


async def wait_for_lsn(service, sc, lsn):
    deadline = asyncio.get_running_loop().time() + service.config.get(
        "limits", "operation_timeout_seconds", 120
    )
    while True:
        slot = await sc.fetchrow(
            """SELECT wal_status,confirmed_flush_lsn >= $2::text::pg_lsn AS reached
            FROM pg_replication_slots WHERE slot_name=$1 AND database=current_database()
            AND slot_type='logical' AND plugin='pgoutput'""",
            service.config.subscription,
            lsn,
        )
        if not slot or slot["wal_status"] == "lost":
            raise PreconditionError(
                "VALIDATION_SLOT_UNAVAILABLE", "Validation needs a valid owned replication slot"
            )
        if slot["reached"]:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise PreconditionError(
                "VALIDATION_CATCHUP_TIMEOUT", "Replica has not acknowledged the validation LSN"
            )
        await asyncio.sleep(0.1)


async def compare_chunk(service, sc, tc, name, columns, keys, lower, upper, source, level, online):
    retries = service.config.get("validation", "max_retries", 3) if online else 0
    pause = service.config.get("validation", "pause_between_chunks_ms", 50) / 1000
    for attempt in range(retries + 1):
        if attempt:
            if pause:
                await asyncio.sleep(pause)
            async with snapshot(sc):
                source = await digest_range(sc, name, columns, keys, lower, upper, level)
        lsn = None
        if online:
            # The source REPEATABLE READ snapshot has ended before taking Ls.
            lsn = await sc.fetchval("SELECT pg_current_wal_lsn()::text")
            await wait_for_lsn(service, sc, lsn)
        async with snapshot(tc):
            target = await digest_range(tc, name, columns, keys, lower, upper, level)
        status = "passed" if source == target else "mismatch"
        if status == "passed":
            break
        if online:
            async with snapshot(sc):
                after = await digest_range(sc, name, columns, keys, lower, upper, level)
            if after != source:
                status = "unstable"
    return dict(source=source, target=target, status=status, attempts=attempt + 1, source_lsn=lsn)


def add_digest(total, value):
    total["count"] += value["count"]
    if "md5_sum" in total:
        total["md5_sum"] = str(int(total["md5_sum"]) + int(value["md5_sum"]))


async def verify_table(service, sc, tc, relation, level, online, emit):
    if online and (sc.is_in_transaction() or tc.is_in_transaction()):
        raise PreconditionError(
            "VALIDATION_SNAPSHOT", "Online validation needs connections without an outer transaction"
        )
    name = qn(relation["schema"], relation["name"])
    columns = [c["attname"] for c in relation["columns"]]
    keys = await primary_key(sc, name) if level != "counts" else []
    result = dict(
        table=name,
        source=dict(count=0),
        target=dict(count=0),
        passed=True,
        chunks_checked=0,
        mismatched_chunks=0,
        unstable_chunks=0,
        attempts=0,
        strategy="pk_ranges" if keys else "whole_table_aggregate",
        advisories=[] if keys or level == "counts" else ["NO_PRIMARY_KEY_FULL_SCAN"],
    )
    if level != "counts":
        result["source"]["md5_sum"] = result["target"]["md5_sum"] = "0"
    lower = None
    while True:
        async with snapshot(sc):
            upper = None
            if keys:
                query, params = boundary_query(
                    name, keys, lower, service.config.get("validation", "chunk_rows", 10000)
                )
                upper = await sc.fetchval(query, *params)
            source = await digest_range(sc, name, columns, keys, lower, upper, level)
        compared = await compare_chunk(
            service, sc, tc, name, columns, keys, lower, upper, source, level, online
        )
        result["chunks_checked"] += 1
        result["attempts"] += compared["attempts"]
        result["mismatched_chunks"] += compared["status"] == "mismatch"
        result["unstable_chunks"] += compared["status"] == "unstable"
        result["passed"] = result["passed"] and compared["status"] == "passed"
        for side in ("source", "target"):
            add_digest(result[side], compared[side])
        # Bounds are serialized JSON, preserving large numeric keys without a
        # Python float roundtrip. Reports contain bounds, never payload rows.
        emit(
            dict(
                table=name,
                chunk=result["chunks_checked"],
                lower_exclusive=lower,
                upper_inclusive=upper,
                **compared,
            )
        )
        if upper is None:
            return result
        lower = upper
        pause = service.config.get("validation", "pause_between_chunks_ms", 50) / 1000
        if pause:
            await asyncio.sleep(pause)


async def verify(service, sc, tc, *, include_deferred=False, level=None, table=None):
    level = level or (
        service.config.get("validation", "cutover_level", "checksums") if include_deferred else "checksums"
    )
    if level not in {"counts", "checksums", "full"}:
        raise PreconditionError("VALIDATION_LEVEL", "Expected counts, checksums or full")
    si, ti = await inventory(sc), await inventory(tc)
    schema_ok = fingerprint(si) == fingerprint(ti)
    relations = [
        r
        for r in si["relations"]
        if not r["extension"]
        and not r["is_partition"]
        and r["kind"] in ("r", "p")
        and (r["persistence"] == "p" or include_deferred)
    ]
    if table:
        relations = [
            r
            for r in relations
            if table in {r["name"], r["schema"] + "." + r["name"], qn(r["schema"], r["name"])}
        ]
        if len(relations) != 1:
            raise PreconditionError(
                "VALIDATION_TABLE", "Table selection is missing or ambiguous; use its qualified name"
            )
    results = []
    online = not include_deferred
    report_path = service.store.directory / "reports" / ("validation-chunks-" + uuid.uuid4().hex + ".jsonl")
    write_private(report_path, "")
    with report_path.open("a") as stream:

        def emit(chunk):
            stream.write(json.dumps(chunk, ensure_ascii=False) + "\n")
            stream.flush()

        # With a catalog mismatch, do not run payload queries against unknown target types.
        if schema_ok:
            parallel = service.config.get("validation", "max_parallel", 1) if online else 1
            if parallel > 1:
                queue = asyncio.Queue()
                for r in relations:
                    queue.put_nowait(r)

                async def worker():
                    async with service.source.connect() as source, service.target.connect() as target:
                        while not queue.empty():
                            r = queue.get_nowait()
                            results.append(
                                await verify_table(service, source, target, r, level, online, emit)
                            )

                try:
                    async with asyncio.TaskGroup() as group:
                        for _ in range(min(parallel, len(relations))):
                            group.create_task(worker())
                except ExceptionGroup as exc:
                    raise exc.exceptions[0] from None
            else:
                for r in relations:
                    results.append(await verify_table(service, sc, tc, r, level, online, emit))
    result = dict(
        checked_at=now(),
        schema_equal=schema_ok,
        schema_differences=difference(schema_manifest(si), schema_manifest(ti)),
        tables=sorted(results, key=lambda r: r["table"]),
        passed=schema_ok and all(r["passed"] for r in results),
        level=level,
        checksum_algorithm=None if level == "counts" else "sum_unsigned_md5_record_text_128",
        execution="postgresql_aggregates",
        authoritative=False,
        online=online,
        coverage="full_scan_selected_tables",
        chunk_report=str(report_path),
        note="Online comparisons use different snapshots; ongoing writes may require another pass. Incremental cold-chunk caching is not implemented.",
    )
    write_private(service.store.directory / "reports/validation.json", json.dumps(result, indent=2))
    return result


async def sequences(sc, tc, *, apply=False):
    rows = await sc.fetch("""SELECT n.nspname AS schema,c.relname AS name FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S'
        AND n.nspname !~ '^pg_' AND n.nspname NOT IN ('lc','information_schema') ORDER BY 1,2""")
    result = []
    for row in rows:
        name = qn(row["schema"], row["name"])
        source = dict(await sc.fetchrow("SELECT last_value,is_called FROM " + name))
        if apply:
            await tc.execute(
                "SELECT pg_catalog.setval($1::regclass,$2,$3)",
                name,
                source["last_value"],
                source["is_called"],
            )
        target = dict(await tc.fetchrow("SELECT last_value,is_called FROM " + name))
        result.append(dict(sequence=name, source=source, target=target, passed=source == target))
    return result


async def large_objects(c):
    async with snapshot(c):
        return [
            dict(r)
            for r in await c.fetch(
                f"""SELECT m.oid::bigint,pg_get_userbyid(m.lomowner) AS owner,m.lomacl::text AS acl,
                s.pages,s.bytes,s.md5_sum
            FROM pg_largeobject_metadata m LEFT JOIN LATERAL (
                SELECT count(*)::bigint AS pages, COALESCE(sum(h.bytes),0)::bigint AS bytes,
                    COALESCE(sum({MD5_NUMBER}),0)::text AS md5_sum
                FROM (SELECT md5(ROW(l.pageno,l.data)::text) AS digest,octet_length(l.data) AS bytes
                    FROM pg_largeobject l WHERE l.loid=m.oid OFFSET 0) h
            ) s ON true ORDER BY m.oid"""
            )
        ]
