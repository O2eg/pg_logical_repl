"""Opt-in integration against owned pg-stand databases, never application DBs."""

import asyncio
import json
import os
import secrets
import subprocess
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import yaml

from pg_logical_repl import ddl, schema, validation
from pg_logical_repl.config import load
from pg_logical_repl.db import identity
from pg_logical_repl.errors import OwnershipError, PreconditionError
from pg_logical_repl.security import identifier as qi
from pg_logical_repl.security import literal as ql
from pg_logical_repl.service import Service
from pg_logical_repl.state import now, write_private

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Opt-in owned stand required"),
]
ROOT = Path(__file__).resolve().parents[2]
MAJOR = int(os.getenv("LC_TEST_MAJOR", "18"))
SOURCE_PORT = 55300 + MAJOR * 10 + 1
TARGET_PORT = 55300 + MAJOR * 10 + 3


@pytest_asyncio.fixture
async def migration(tmp_path):
    creds = json.loads((ROOT / ".pg_stand/credentials/database/passwords.json").read_text())["superuser"]
    name = "lc_test_" + secrets.token_hex(5)
    connections = []
    for port in (SOURCE_PORT, TARGET_PORT):
        c = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user=creds["username"],
            password=creds["password"],
            database="postgres",
        )
        await c.execute("CREATE DATABASE " + qi(name))
        await c.close()
        connections.append(
            await asyncpg.connect(
                host="127.0.0.1", port=port, user=creds["username"], password=creds["password"], database=name
            )
        )
    sc, tc = connections
    baseline_source_roles = {r["rolname"] for r in await sc.fetch("SELECT rolname FROM pg_roles")}
    baseline_target_roles = {r["rolname"] for r in await tc.fetch("SELECT rolname FROM pg_roles")}
    # Owned pg-stand creates the same plaintext password with independently salted verifiers.
    # Normalize only its documented bootstrap accounts before testing exact role preservation.
    bootstrap = {"postgres", *(f"user_app_{i:02}" for i in range(1, 11))}
    for r in await sc.fetch(
        "SELECT rolname,rolpassword FROM pg_authid WHERE rolname=ANY($1)", list(bootstrap)
    ):
        await tc.execute("ALTER ROLE " + qi(r["rolname"]) + " PASSWORD " + ql(r["rolpassword"]))
    write_private(tmp_path / "password", creds["password"])
    source = dict(
        host="127.0.0.1",
        port=SOURCE_PORT,
        database=name,
        user=creds["username"],
        password_file="password",
        sslmode="disable",
    )
    target = dict(source, port=TARGET_PORT)
    cfg = dict(
        schema_version="pg_logical_connector/config-v2",
        migration_id=name,
        state_dir="state",
        source=dict(
            admin=source, replication=dict(host=f"lc-development{MAJOR}-primary-pg-stand-managed", port=5432)
        ),
        target=dict(admin=target),
        ddl=dict(mode="frozen"),
        capacity=dict(evidence_file="capacity.json", max_age_seconds=3600),
        limits=dict(operation_timeout_seconds=45, catchup_stable_seconds=0),
    )
    if MAJOR != 18:
        cfg["client_tools"] = {
            "bin_dirs": {str(MAJOR): str(ROOT / f".runtime/clients/usr/lib/postgresql/{MAJOR}/bin")}
        }
    write_private(tmp_path / "config.yaml", yaml.safe_dump(cfg))
    capacity = dict(measured_at=now())
    for side, c, node in [("source", sc, "primary"), ("target", tc, "logical")]:
        free = int(
            subprocess.check_output(
                [
                    "docker",
                    "exec",
                    f"lc-development{MAJOR}-{node}-pg-stand-managed",
                    "df",
                    "-B1",
                    "--output=avail",
                    "/var/lib/postgresql",
                ],
                text=True,
            )
            .splitlines()[-1]
            .strip()
        )
        capacity[side] = dict(
            system_identifier=(await identity(c))["system_identifier"],
            free_bytes=free,
            required_bytes=268435456,
        )
    write_private(tmp_path / "capacity.json", json.dumps(capacity))
    service = Service(load(tmp_path / "config.yaml"))
    try:
        yield service, sc, tc
    finally:
        # Every object belongs to these random databases. Do not terminate unrelated backends.
        for connection in connections:
            await connection.close()
        connections = [
            await asyncpg.connect(
                host="127.0.0.1", port=port, user=creds["username"], password=creds["password"], database=name
            )
            for port in (SOURCE_PORT, TARGET_PORT)
        ]
        sc, tc = connections
        exists = await tc.fetchval(
            "SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", service.config.subscription
        )
        if exists:
            await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
            # A network fault may have removed the test proxy. Teardown uses the
            # direct admin connections and this fixture's newly created database.
            for _ in range(450):
                if not await tc.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_stat_subscription WHERE subname=$1 AND pid IS NOT NULL)",
                    service.config.subscription,
                ):
                    break
                await asyncio.sleep(0.1)
            await tc.execute(
                "ALTER SUBSCRIPTION " + qi(service.config.subscription) + " SET (slot_name=NONE)"
            )
            await tc.execute("DROP SUBSCRIPTION " + qi(service.config.subscription))
        for _ in range(450):
            if not await sc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE database=$1 AND active)", name
            ):
                break
            await asyncio.sleep(0.1)
        for slot in await sc.fetch(
            "SELECT slot_name,plugin,active FROM pg_replication_slots WHERE database=$1", name
        ):
            assert slot["plugin"] == "pgoutput" and not slot["active"]
            await sc.fetchval("SELECT pg_drop_replication_slot($1)", slot["slot_name"])
        created_roles = {
            r["rolname"] for r in await tc.fetch("SELECT rolname FROM pg_roles")
        } - baseline_target_roles
        created_source_roles = {
            r["rolname"] for r in await sc.fetch("SELECT rolname FROM pg_roles")
        } - baseline_source_roles
        for c in connections:
            await c.close()
        for port in (SOURCE_PORT, TARGET_PORT):
            c = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user=creds["username"],
                password=creds["password"],
                database="postgres",
            )
            await c.execute("DROP DATABASE " + qi(name))
            await c.close()
        c = await asyncpg.connect(
            host="127.0.0.1",
            port=TARGET_PORT,
            user=creds["username"],
            password=creds["password"],
            database="postgres",
        )
        for role in sorted(created_roles):
            await c.execute("DROP ROLE " + qi(role))
        await c.close()
        c = await asyncpg.connect(
            host="127.0.0.1",
            port=SOURCE_PORT,
            user=creds["username"],
            password=creds["password"],
            database="postgres",
        )
        for role in sorted(created_source_roles):
            assert role.startswith(name)
            await c.execute("DROP ROLE " + qi(role))
        await c.close()


async def operate(service, kind, **kwargs):
    plan = await service.plan(kind)
    return await getattr(service, kind.replace("-", "_"))(plan["path"], plan["plan_hash"], **kwargs)


async def ready(service):
    for _ in range(150):
        status = await service.run_once()
        if status["state"] == "REPLICATING":
            return status
        if status["state"] == "BLOCKED":
            raise AssertionError(status.get("health"))
        await asyncio.sleep(0.1)
    raise AssertionError("Tablesync did not become ready")


class AggregateOnlyConnection:
    """Assert that payload data never reaches the validation client."""

    def __init__(self, connection, forbidden):
        self.connection, self.forbidden = connection, forbidden
        self.max_response_bytes = 0
        self.total_response_bytes = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def inspect(self, result):
        text = json.dumps(result, default=str)
        assert self.forbidden not in text
        self.max_response_bytes = max(self.max_response_bytes, len(text.encode()))
        self.total_response_bytes += len(text.encode())
        return result

    async def fetch(self, query, *args):
        return self.inspect(await self.connection.fetch(query, *args))

    async def fetchrow(self, query, *args):
        return self.inspect(await self.connection.fetchrow(query, *args))

    async def fetchval(self, query, *args):
        return self.inspect(await self.connection.fetchval(query, *args))

    def cursor(self, *args, **kwargs):
        raise AssertionError("Validation must not stream table rows or LO pages")


async def test_validation_server_chunks_composite_pk_and_extra_rows(migration):
    service, sc, tc = migration
    marker = "PAYLOAD_MUST_STAY_IN_POSTGRESQL"
    await sc.execute("""CREATE TABLE "odd.name" (
        tenant text, id numeric, payload text, raw bytea, amount money, moment timestamptz,
        PRIMARY KEY(tenant,id));""")
    await sc.execute(
        """INSERT INTO "odd.name"
        SELECT CASE WHEN n%2=0 THEN 'a' ELSE 'b' END,
            1000000000000000000000000000000::numeric+n*11,
            CASE WHEN n%3=0 THEN NULL ELSE repeat($1,1000) END,
            decode('00ffab','hex'),(n::numeric/100)::money,'2026-01-01'::timestamptz
        FROM generate_series(1,80) n""",
        marker,
    )
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    service.config.raw["validation"] = dict(chunk_rows=7, max_retries=0, pause_between_chunks_ms=0)
    inv = await validation.inventory(sc)
    relation = next(r for r in inv["relations"] if r["name"] == "odd.name")
    source, target = AggregateOnlyConnection(sc, marker), AggregateOnlyConnection(tc, marker)
    chunks = []
    result = await validation.verify_table(
        service, source, target, relation, "checksums", True, chunks.append
    )
    assert result["passed"] and result["source"]["count"] == 80
    assert result["chunks_checked"] == 12
    assert all(x["source"]["count"] <= 7 for x in chunks)
    assert chunks[0]["lower_exclusive"] is None and chunks[-1]["upper_inclusive"] is None
    assert max(source.max_response_bytes, target.max_response_bytes) < 1024
    assert source.total_response_bytes + target.total_response_bytes < 20000

    # Extra keys before, between and after source keys must all be included.
    await tc.execute("""INSERT INTO "odd.name"(tenant,id,payload)
        VALUES('0',1,'extra'),('aa',1,'extra'),('z',1,'extra')""")
    result = await validation.verify_table(
        service, source, target, relation, "checksums", False, lambda _: None
    )
    assert not result["passed"]
    assert result["source"]["count"] == 80 and result["target"]["count"] == 83
    await tc.execute("""DELETE FROM "odd.name" WHERE tenant IN ('0','aa','z');
        UPDATE "odd.name" SET raw=decode('00ffac','hex') WHERE id=1000000000000000000000000000011""")
    result = await validation.verify_table(
        service, source, target, relation, "checksums", False, lambda _: None
    )
    assert not result["passed"] and result["source"]["count"] == result["target"]["count"]

    # Indexable key ranges; no sorting of payload JSON/text to calculate checksums.
    name = 'public."odd.name"'
    keys = ["tenant", "id"]
    query, params = validation.digest_query(
        name,
        [c["attname"] for c in relation["columns"]],
        keys,
        chunks[1]["lower_exclusive"],
        chunks[1]["upper_inclusive"],
    )
    plan = json.loads(await sc.fetchval("EXPLAIN (FORMAT JSON) " + query, *params))

    def nodes(node):
        yield node["Node Type"]
        for child in node.get("Plans", []):
            yield from nodes(child)

    assert "Sort" not in list(nodes(plan[0]["Plan"]))
    await sc.execute("SET enable_seqscan=off")
    query, params = validation.boundary_query(name, keys, chunks[1]["lower_exclusive"], 7)
    plan = json.loads(await sc.fetchval("EXPLAIN (FORMAT JSON) " + query, *params))
    assert any("Index" in n for n in nodes(plan[0]["Plan"]))
    await sc.execute("RESET enable_seqscan")
    await operate(service, "abort")


async def test_validation_waits_for_lsn_before_reading_target(migration, monkeypatch):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY,value text); INSERT INTO app VALUES(1,'before')")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " DISABLE")
    # Wait until the worker has actually stopped before generating new WAL.
    for _ in range(100):
        if not await sc.fetchval(
            "SELECT active FROM pg_replication_slots WHERE slot_name=$1", service.config.subscription
        ):
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("Subscription worker did not stop")
    await sc.execute("UPDATE app SET value='after'")
    service.config.raw["limits"]["operation_timeout_seconds"] = 1
    original = validation.digest_range
    target_reads = []

    async def counted(c, *args, **kwargs):
        if c is tc:
            target_reads.append(True)
        return await original(c, *args, **kwargs)

    monkeypatch.setattr(validation, "digest_range", counted)
    with pytest.raises(PreconditionError) as error:
        await validation.verify(service, sc, tc)
    assert error.value.code == "VALIDATION_CATCHUP_TIMEOUT" and not target_reads
    await tc.execute("ALTER SUBSCRIPTION " + qi(service.config.subscription) + " ENABLE")
    service.config.raw["limits"]["operation_timeout_seconds"] = 45
    await service.barrier(sc, tc)
    result = await service.verify(level="counts", table="public.app")
    assert result["passed"] and result["checksum_algorithm"] is None
    await operate(service, "abort")


async def test_validation_large_objects_aggregate_only(migration):
    _, sc, tc = migration
    for c in (sc, tc):
        await c.execute("""SELECT lo_create(481230);
            SELECT lo_from_bytea(481231,decode(repeat('abcdef',4000),'hex'));
            SELECT lo_put(481231,65536,decode('00ff','hex'));""")
    source, target = AggregateOnlyConnection(sc, "abcdefabcdef"), AggregateOnlyConnection(tc, "abcdefabcdef")
    left, right = await validation.large_objects(source), await validation.large_objects(target)
    assert left == right and left[0]["pages"] == 0 and left[1]["bytes"] == 12002
    assert source.max_response_bytes < 2048 and target.max_response_bytes < 2048
    await tc.execute("UPDATE pg_largeobject SET pageno=pageno+1 WHERE loid=481231 AND pageno=32")
    assert await validation.large_objects(source) != await validation.large_objects(target)


async def test_validation_parallel_no_pk_duplicates_and_counts(migration):
    service, sc, tc = migration
    await sc.execute("""CREATE TABLE keyed(id int PRIMARY KEY,value text);
        INSERT INTO keyed VALUES(1,'a'),(2,'b');
        CREATE TABLE duplicates(value text,extra text); ALTER TABLE duplicates REPLICA IDENTITY FULL;
        INSERT INTO duplicates VALUES('same',NULL),('same',NULL),('other','');""")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    service.config.raw["validation"] = dict(
        max_parallel=2, chunk_rows=2, max_retries=0, pause_between_chunks_ms=0
    )
    report = await service.verify()
    assert report["passed"]
    table = next(r for r in report["tables"] if r["table"] == '"public"."duplicates"')
    assert table["strategy"] == "whole_table_aggregate" and table["advisories"] == [
        "NO_PRIMARY_KEY_FULL_SCAN"
    ]
    await tc.execute("""DELETE FROM duplicates WHERE ctid=(SELECT ctid FROM duplicates WHERE value='same' LIMIT 1);
        INSERT INTO duplicates VALUES('different',NULL);""")
    report = await service.verify(level="full")
    assert not report["passed"]
    table = next(r for r in report["tables"] if r["table"] == '"public"."duplicates"')
    assert table["source"]["count"] == table["target"]["count"] == 3
    assert table["mismatched_chunks"] == 1
    assert (await service.verify(level="counts"))["passed"]  # Explicitly weaker comparison.
    await operate(service, "abort")


async def test_validation_retries_chunk_changed_by_real_writer(migration, monkeypatch):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE changing(id int PRIMARY KEY,value int); INSERT INTO changing VALUES(1,10)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await tc.execute("UPDATE changing SET value=11")
    original = validation.digest_range
    changed = False

    async def with_writer(c, *args, **kwargs):
        nonlocal changed
        if c is tc and not changed:
            changed = True
            await sc.execute("UPDATE changing SET value=30")
        return await original(c, *args, **kwargs)

    monkeypatch.setattr(validation, "digest_range", with_writer)
    report = await validation.verify(service, sc, tc)
    assert report["passed"] and report["tables"][0]["attempts"] == 2
    assert await tc.fetchval("SELECT value FROM changing") == 30
    await operate(service, "abort")


async def test_validation_quoted_key_with_nonstandard_string_setting(migration):
    service, sc, tc = migration
    key = "quoted\\'key"
    for c in (sc, tc):
        await c.execute("CREATE TABLE unusual(" + qi(key) + " bigint PRIMARY KEY,payload text)")
        await c.execute("INSERT INTO unusual VALUES(1,'payload'),(2,'payload2')")
        await c.execute("SET standard_conforming_strings=off")
    relation = next(r for r in (await validation.inventory(sc))["relations"] if r["name"] == "unusual")
    service.config.raw["validation"] = dict(chunk_rows=1, pause_between_chunks_ms=0)
    result = await validation.verify_table(service, sc, tc, relation, "checksums", False, lambda _: None)
    assert result["passed"] and result["source"]["count"] == 2 and result["chunks_checked"] == 3


@pytest.mark.parametrize("side", ["source", "target"])
async def test_unsupported_objects_stop_before_mutation(migration, side):
    service, sc, tc = migration
    c = sc if side == "source" else tc
    await c.execute("""CREATE TABLE guarded(id int PRIMARY KEY, a int, b int);
        ALTER TABLE guarded ENABLE ROW LEVEL SECURITY;
        CREATE POLICY only_own ON guarded USING (id=1);
        CREATE COLLATION private_collation FROM pg_catalog."C";
        CREATE TYPE private_pair AS (a int,b text);
        CREATE STATISTICS private_statistics ON a,b FROM guarded;
        CREATE TEXT SEARCH CONFIGURATION private_search (COPY=pg_catalog.simple);
        CREATE FOREIGN DATA WRAPPER private_fdw;
        CREATE SERVER private_server FOREIGN DATA WRAPPER private_fdw OPTIONS (secret 'never-export-this');
        CREATE USER MAPPING FOR CURRENT_USER SERVER private_server OPTIONS (password 'never-export-this');
        CREATE RULE guarded_rule AS ON INSERT TO guarded DO ALSO SELECT 1;
        CREATE TABLE legacy_parent(id int PRIMARY KEY);
        CREATE TABLE legacy_child(extra text) INHERITS(legacy_parent);
        CREATE AGGREGATE private_total(integer) (SFUNC=int4pl, STYPE=integer, INITCOND='0');
    """)
    result = await service.check()
    assert not result["passed"]
    categories = {r["category"] for r in result[side]["unsupported_objects"]}
    assert {
        "row_security",
        "pg_policy",
        "pg_collation",
        "pg_type",
        "pg_statistic_ext",
        "pg_ts_config",
        "pg_foreign_data_wrapper",
        "pg_foreign_server",
        "pg_user_mapping",
        "pg_rewrite",
        "inheritance",
        "pg_proc",
    } <= categories
    assert "never-export-this" not in json.dumps(result)
    if side == "target":
        assert any(r["code"] == "TARGET_EMPTY" for r in result["blockers"])
    with pytest.raises(PreconditionError) as error:
        await operate(service, "prepare")
    assert error.value.code == "PREFLIGHT_BLOCKED"
    for connection in (sc, tc):
        assert await connection.fetchval("SELECT to_regnamespace('lc')") is None
        assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_publication)")


async def test_plan_detects_partition_boundary_and_sequence_definition_drift(migration):
    service, sc, tc = migration
    await sc.execute("""CREATE TABLE events(id int PRIMARY KEY) PARTITION BY RANGE(id);
        CREATE TABLE events_0 PARTITION OF events FOR VALUES FROM (0) TO (10);
        CREATE SEQUENCE numbering INCREMENT 3 CACHE 4;""")
    plan = await service.plan("prepare")
    await sc.execute("ALTER SEQUENCE numbering INCREMENT 5")
    with pytest.raises(PreconditionError) as error:
        await service.prepare(plan["path"], plan["plan_hash"])
    assert error.value.code == "PLAN_DRIFT"
    await sc.execute("ALTER SEQUENCE numbering INCREMENT 3")
    await sc.execute("""ALTER TABLE events DETACH PARTITION events_0;
        ALTER TABLE events ATTACH PARTITION events_0 FOR VALUES FROM (0) TO (20);""")
    with pytest.raises(PreconditionError) as error:
        await service.prepare(plan["path"], plan["plan_hash"])
    assert error.value.code == "PLAN_DRIFT"
    assert await tc.fetchval("SELECT to_regnamespace('lc')") is None


async def test_health_blocks_removed_publication_member(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE protected(id int PRIMARY KEY)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await sc.execute(
        "SELECT set_config('lc.ownership_token',$1,false)", service.store.read()["ownership_token"]
    )
    await sc.execute("ALTER PUBLICATION " + qi(service.config.publication) + " DROP TABLE protected")
    result = await service.run_once()
    assert result["state"] == "BLOCKED"
    assert "PUBLICATION_MEMBERSHIP_CHANGED" in result["health"]["blockers"]
    assert await sc.fetchval("SELECT state FROM lc.control") == "BLOCKED"
    await operate(service, "abort")


async def test_health_detects_repeating_real_apply_errors(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE conflict_test(id int PRIMARY KEY, value text)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await tc.execute("INSERT INTO conflict_test VALUES(1,'target conflict')")
    await sc.execute("INSERT INTO conflict_test VALUES(1,'source value')")

    async def wait_errors(greater_than):
        for _ in range(200):
            count = await tc.fetchval(
                """SELECT apply_error_count FROM pg_stat_subscription_stats
                WHERE subid=(SELECT oid FROM pg_subscription WHERE subname=$1)""",
                service.config.subscription,
            )
            if count > greater_than:
                return count
            await asyncio.sleep(0.1)
        raise AssertionError("Expected native apply error was not observed")

    count = await wait_errors(0)
    result = await service.run_once()
    assert result["state"] == "REPLICATING"  # A single new error alone is not persistent.
    state = service.store.read()
    # Advance the observation window in durable test state; the errors themselves
    # come from the actual PostgreSQL worker, not mocks or catalog edits.
    state["health"]["apply_error_count_window"]["first"] = (
        datetime.now(UTC) - timedelta(seconds=600)
    ).isoformat()
    service.store.save(state)
    await wait_errors(count)
    result = await service.run_once()
    assert result["state"] == "BLOCKED"
    assert "PERSISTENT_APPLY_ERROR_COUNT" in result["health"]["blockers"]
    assert await tc.fetchval("SELECT value FROM conflict_test WHERE id=1") == "target conflict"
    assert await sc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", service.config.subscription
    )
    await operate(service, "abort")


async def test_bootstrap_dml_abort(migration):
    service, sc, tc = migration
    await sc.execute("""CREATE TABLE parent(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text UNIQUE);
        CREATE TABLE child(id int PRIMARY KEY, parent_id bigint REFERENCES parent(id), value text);
        CREATE INDEX child_value ON child(value);
        INSERT INTO parent(name) SELECT 'row '||n FROM generate_series(1,100) n;
        INSERT INTO child SELECT n,n,'initial' FROM generate_series(1,100) n;
        CREATE VIEW names AS SELECT id,name FROM parent;
        COMMENT ON TABLE parent IS 'migration fixture';
        REVOKE ALL ON parent FROM PUBLIC;""")
    check = await service.check()
    assert check["passed"], check["blockers"]
    await operate(service, "prepare")
    with pytest.raises(asyncpg.PostgresError, match="frozen"):
        await sc.execute("ALTER TABLE parent ADD COLUMN rejected int")
    await operate(service, "start")
    await ready(service)
    await sc.execute("UPDATE child SET value='updated' WHERE id<=25; DELETE FROM child WHERE id>90")
    await service.barrier(sc, tc)
    assert await sc.fetch("SELECT * FROM child ORDER BY id") == await tc.fetch(
        "SELECT * FROM child ORDER BY id"
    )
    assert await tc.fetchval("SELECT count(*) FROM names") == 100
    await operate(service, "abort")
    assert not await sc.fetchval("SELECT to_regnamespace('lc') IS NOT NULL")
    assert not await tc.fetchval("SELECT to_regnamespace('lc') IS NOT NULL")
    assert await tc.fetchval("SELECT count(*) FROM child") == 90
    await sc.execute("ALTER TABLE parent ADD COLUMN restored_ddl int")


async def test_cutover_deferred_objects(migration):
    service, sc, tc = migration
    await sc.execute("""CREATE TABLE app(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, amount numeric);
        INSERT INTO app(amount) SELECT n FROM generate_series(1,80) n;
        CREATE UNLOGGED TABLE cache(id int PRIMARY KEY,value text);
        INSERT INTO cache SELECT n,'cached '||n FROM generate_series(1,25) n;
        CREATE SEQUENCE backwards INCREMENT -3 START -10;
        SELECT nextval('backwards'); SELECT nextval('backwards');
        CREATE SEQUENCE unused START 100;
        CREATE MATERIALIZED VIEW totals AS SELECT sum(amount) AS total FROM app;
        CREATE MATERIALIZED VIEW doubled AS SELECT total*2 AS total FROM totals;
        CREATE MATERIALIZED VIEW not_ready AS SELECT * FROM app WITH NO DATA;
        SELECT lo_from_bytea(481234,decode('0011ffab','hex'));
        COMMENT ON LARGE OBJECT 481234 IS 'owned fixture';
        GRANT SELECT ON LARGE OBJECT 481234 TO PUBLIC;""")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await sc.execute("UPDATE app SET amount=amount+1; INSERT INTO cache VALUES(99,'late')")
    await service.barrier(sc, tc)
    assert await tc.fetchval("SELECT count(*) FROM cache") == 0
    document = dict(
        migration_id=service.config.migration_id,
        source_identity=service.store.read()["source_identity"],
        created_at=now(),
        responsible="integration harness",
        external_fence="All fixture writers joined; no application sessions",
    )
    path = service.store.directory / "fence.json"
    write_private(path, json.dumps(document))
    result = await operate(service, "cutover-finalize", write_fence=path)
    assert result["passed"]
    assert await tc.fetchval("SELECT count(*) FROM cache") == 26
    assert await tc.fetchval("SELECT total FROM doubled") == await sc.fetchval(
        "SELECT sum(amount)*2 FROM app"
    )
    assert await tc.fetchval("SELECT lo_get(481234)") == bytes.fromhex("0011ffab")
    assert await tc.fetchrow("SELECT last_value,is_called FROM backwards") == await sc.fetchrow(
        "SELECT last_value,is_called FROM backwards"
    )
    assert await tc.fetchrow("SELECT last_value,is_called FROM unused") == await sc.fetchrow(
        "SELECT last_value,is_called FROM unused"
    )
    await operate(service, "cutover-commit")
    assert service.store.read()["state"] == "DETACHED"
    assert await tc.fetchval("INSERT INTO app(amount) VALUES(9) RETURNING id") == 81
    assert not await sc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", service.config.subscription
    )


@pytest.mark.parametrize("mode", ["managed_only", "captured"])
async def test_transactional_ddl(migration, mode):
    service, sc, tc = migration
    service.config.raw["ddl"]["mode"] = mode
    await sc.execute("CREATE TABLE public.app(id int PRIMARY KEY,n int); INSERT INTO public.app VALUES(1,10)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    if mode == "managed_only":
        statement = "ALTER TABLE public.app ADD COLUMN extra text"
        sql_file = service.store.directory / "change.sql"
        write_private(sql_file, statement)
        plan = await service.plan("ddl-exec", extra=ddl.classify(statement))
        result = await ddl.execute(service, plan["path"], plan["plan_hash"], sql_file)
        assert result["applied"]
        await sc.execute("INSERT INTO public.app VALUES(2,20,'after ddl')")
    else:
        async with sc.transaction():
            await sc.execute("ALTER TABLE public.app ADD COLUMN extra text")
            await sc.execute("INSERT INTO public.app VALUES(2,20,'after ddl')")
        async with sc.transaction():
            await sc.execute("UPDATE public.app SET n=n+1 WHERE id=1")
            await sc.execute("ALTER TABLE public.app RENAME COLUMN n TO value")
            await sc.execute("UPDATE public.app SET value=value+2 WHERE id=1")
        tx = sc.transaction()
        await tx.start()
        await sc.execute("ALTER TABLE public.app ADD COLUMN undone integer")
        await tx.rollback()
        with pytest.raises(asyncpg.PostgresError, match="deterministic|frozen"):
            await sc.execute("ALTER TABLE public.app ADD COLUMN danger integer DEFAULT random()::integer")
        with pytest.raises(asyncpg.PostgresError, match="replica identity"):
            await sc.execute("ALTER TABLE public.app DROP COLUMN id")
        await sc.execute("ALTER TABLE public.app DROP COLUMN extra")
        await sc.execute("INSERT INTO public.app VALUES(3,30)")
    await service.barrier(sc, tc)
    assert await sc.fetch("SELECT * FROM public.app ORDER BY id") == await tc.fetch(
        "SELECT * FROM public.app ORDER BY id"
    )
    assert await tc.fetchval("SELECT count(*) FROM lc.ddl_applied") == await sc.fetchval(
        "SELECT count(*) FROM lc.ddl_log"
    )
    if mode == "captured":
        assert await tc.fetchval("SELECT value FROM public.app WHERE id=1") == 13
    await operate(service, "abort")


async def test_stale_plan_never_mutates(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE public.app(id int PRIMARY KEY)")
    plan = await service.plan("prepare")
    await sc.execute("ALTER TABLE public.app ADD COLUMN changed text")
    with pytest.raises(PreconditionError, match="catalog changed"):
        await service.prepare(plan["path"], plan["plan_hash"])
    assert not await sc.fetchval("SELECT to_regnamespace('lc') IS NOT NULL")
    assert not await tc.fetchval("SELECT to_regnamespace('lc') IS NOT NULL")


async def test_roles_verifiers_acl_membership(migration):
    service, sc, tc = migration
    role = service.config.migration_id + "_login"
    group = service.config.migration_id + "_group"
    await sc.execute("CREATE ROLE " + qi(group) + " NOLOGIN")
    await sc.execute("CREATE ROLE " + qi(role) + " LOGIN PASSWORD " + ql(secrets.token_urlsafe(32)))
    await sc.execute("GRANT " + qi(group) + " TO " + qi(role) + " WITH ADMIN OPTION")
    await sc.execute("ALTER ROLE " + qi(role) + " SET work_mem='32MB'")
    db = await sc.fetchval("SELECT current_database()")
    await sc.execute("ALTER DATABASE " + qi(db) + " OWNER TO " + qi(group))
    await sc.execute("ALTER DATABASE " + qi(db) + " SET work_mem='16MB'")
    await sc.execute("ALTER ROLE " + qi(role) + " IN DATABASE " + qi(db) + " SET statement_timeout='12s'")
    await sc.execute("COMMENT ON DATABASE " + qi(db) + " IS 'owned database fixture'")
    await sc.execute("ALTER SCHEMA public OWNER TO " + qi(group))
    await sc.execute("CREATE SCHEMA app AUTHORIZATION " + qi(group))
    await sc.execute("CREATE TABLE app.items(id int PRIMARY KEY,value text)")
    await sc.execute("ALTER TABLE app.items OWNER TO " + qi(group))
    await sc.execute("GRANT SELECT ON app.items TO " + qi(role))
    await sc.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE "
        + qi(group)
        + " IN SCHEMA app GRANT SELECT ON TABLES TO "
        + qi(role)
    )
    await sc.execute("INSERT INTO app.items VALUES(1,'owned')")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    for name in (role, group):
        a = await sc.fetchval("SELECT rolpassword FROM pg_authid WHERE rolname=$1", name)
        b = await tc.fetchval("SELECT rolpassword FROM pg_authid WHERE rolname=$1", name)
        assert a == b, "Password verifier was changed"
    assert await tc.fetchval("SELECT has_table_privilege($1,'app.items','SELECT')", role)
    assert (
        await tc.fetchval("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()")
        == group
    )
    assert (
        await tc.fetchval(
            "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()"
        )
        == "owned database fixture"
    )
    await operate(service, "abort")


async def test_recover_lost_schema_reply(migration, monkeypatch):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY)")
    plan = await service.plan("prepare")
    original = schema.restore

    async def failed(*args):
        await original(*args)
        raise TimeoutError("injected lost response after committed restore")

    monkeypatch.setattr(schema, "restore", failed)
    with pytest.raises(TimeoutError):
        await service.prepare(plan["path"], plan["plan_hash"])
    assert await tc.fetchval("SELECT EXISTS(SELECT FROM lc.operations WHERE name='schema')")
    monkeypatch.setattr(schema, "restore", original)
    await service.prepare(plan["path"], plan["plan_hash"])
    assert service.store.read()["state"] == "PREPARED"
    await operate(service, "abort")


async def test_recover_lost_subscription_reply(migration, monkeypatch):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
    await operate(service, "prepare")
    plan = await service.plan("start")
    original = service.target.connect

    class Proxy:
        def __init__(self, c):
            self.c = c

        def __getattr__(self, name):
            return getattr(self.c, name)

        async def execute(self, sql, *args, **kw):
            result = await self.c.execute(sql, *args, **kw)
            if sql.startswith("CREATE SUBSCRIPTION"):
                raise TimeoutError("injected lost CREATE SUBSCRIPTION reply")
            return result

    @asynccontextmanager
    async def broken(**kwargs):
        async with original(**kwargs) as c:
            yield Proxy(c)

    monkeypatch.setattr(service.target, "connect", broken)
    with pytest.raises(TimeoutError):
        await service.start(plan["path"], plan["plan_hash"])
    monkeypatch.setattr(service.target, "connect", original)
    await service.start(plan["path"], plan["plan_hash"])
    await ready(service)
    assert await tc.fetchval("SELECT count(*) FROM app") == 1
    await operate(service, "abort")


async def test_ddl_retry_not_reexecuted(migration, monkeypatch):
    service, sc, tc = migration
    service.config.raw["ddl"]["mode"] = "managed_only"
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    statement = "ALTER TABLE public.app ADD COLUMN value text"
    sql_file = service.store.directory / "change.sql"
    write_private(sql_file, statement)
    plan = await service.plan("ddl-exec", extra=ddl.classify(statement))
    original = service.barrier

    async def fail(*args, **kw):
        raise TimeoutError("injected acknowledgement loss")

    monkeypatch.setattr(service, "barrier", fail)
    with pytest.raises(TimeoutError):
        await ddl.execute(service, plan["path"], plan["plan_hash"], sql_file)
    monkeypatch.setattr(service, "barrier", original)
    await ddl.execute(service, plan["path"], plan["plan_hash"], sql_file)
    assert await sc.fetchval("SELECT count(*) FROM lc.ddl_log") == 1
    assert await tc.fetchval("SELECT count(*) FROM lc.ddl_applied") == 1
    await operate(service, "abort")


async def test_ownership_blocks_cleanup(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    await tc.execute("UPDATE lc.control SET ownership_token='not-ours'")
    with pytest.raises(OwnershipError):
        await operate(service, "abort")
    assert await sc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_publication WHERE pubname=$1)", service.config.publication
    )
    await tc.execute("UPDATE lc.control SET ownership_token=$1", service.store.read()["ownership_token"])
    await operate(service, "abort")


async def test_write_after_finalize_is_detected(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY,value int); INSERT INTO app VALUES(1,10)")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    document = dict(
        migration_id=service.config.migration_id,
        source_identity=service.store.read()["source_identity"],
        created_at=now(),
        responsible="test harness",
        external_fence="test writer stopped",
    )
    path = service.store.directory / "fence.json"
    write_private(path, json.dumps(document))
    await operate(service, "cutover-finalize", write_fence=path)
    await sc.execute("UPDATE app SET value=11")
    await service.barrier(sc, tc)
    with pytest.raises(PreconditionError, match="changed after finalize"):
        await operate(service, "cutover-commit")
    assert await tc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", service.config.subscription
    )
    await service.cutover_abort("Test cancellation after a detected external writer")
    assert service.store.read()["state"] == "REPLICATING"
    await sc.execute("UPDATE app SET value=12")
    await service.barrier(sc, tc)
    assert await tc.fetchval("SELECT value FROM app WHERE id=1") == 12
    await operate(service, "abort")


async def test_native_copy_under_concurrent_load(migration):
    service, sc, tc = migration
    await sc.execute("""CREATE TYPE public.status AS ENUM ('new','updated');
      CREATE TABLE public.app(id int PRIMARY KEY,n int NOT NULL,payload text,status public.status DEFAULT 'new');
      INSERT INTO app SELECT n,0,repeat(md5(n::text),128),'new' FROM generate_series(1,6000) n;
      CREATE TABLE events(id int PRIMARY KEY,value int) PARTITION BY RANGE(id);
      CREATE TABLE events_p1 PARTITION OF events FOR VALUES FROM (0) TO (10000);
      CREATE TABLE events_p2 PARTITION OF events FOR VALUES FROM (10000) TO (20000);
      INSERT INTO events VALUES(1,10),(10001,20);
      CREATE TABLE full_rows(key text,value text); ALTER TABLE full_rows REPLICA IDENTITY FULL;
      INSERT INTO full_rows VALUES('same','duplicate'),('same','duplicate'),('other','initial');""")
    await operate(service, "prepare")
    await operate(service, "start")
    stop = asyncio.Event()
    counters = [0] * 4

    async def writer(worker):
        async with service.source.connect() as c:
            while not stop.is_set():
                async with c.transaction():
                    await c.execute("UPDATE app SET n=n+1,status='updated' WHERE id=$1", worker + 1)
                    await c.execute(
                        "INSERT INTO app(id,n,payload) VALUES($1,1,$2)",
                        100000 + worker * 100000 + counters[worker],
                        "concurrent",
                    )
                counters[worker] += 1
                await asyncio.sleep(0.002)

    tasks = [asyncio.create_task(writer(i)) for i in range(4)]
    try:
        await ready(service)
        await asyncio.sleep(1)
        await sc.execute(
            "UPDATE full_rows SET value='changed' WHERE key='same'; DELETE FROM full_rows WHERE key='other'; UPDATE events SET value=value+1"
        )
    finally:
        stop.set()
        await asyncio.gather(*tasks)
    await service.barrier(sc, tc)
    report = await service.verify()
    assert report["passed"], report["schema_differences"]
    assert sum(counters) > 50
    await operate(service, "abort")


async def test_apply_recovers_after_target_restart(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY,value text); INSERT INTO app VALUES(1,'before')")
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    process = await asyncio.create_subprocess_exec(
        "docker",
        "restart",
        "--time",
        "5",
        f"lc-development{MAJOR}-logical-pg-stand-managed",
        stdout=asyncio.subprocess.DEVNULL,
    )
    assert await process.wait() == 0
    await sc.execute("INSERT INTO app VALUES(2,'during restart')")
    for _ in range(80):
        try:
            async with service.target.connect() as fresh:
                await service.barrier(sc, fresh)
                assert await fresh.fetchval("SELECT count(*) FROM app") == 2
            break
        except Exception:
            await asyncio.sleep(0.2)
    else:
        raise AssertionError("Replication did not recover after target restart")
    await operate(service, "abort")
