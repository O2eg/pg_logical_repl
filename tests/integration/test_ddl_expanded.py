"""DDL journal, native onboarding, queue recovery and operator intervention."""

import asyncio
import json
import os

import asyncpg
import pytest
from test_lifecycle import migration as migration_fixture
from test_lifecycle import operate, ready

from pg_logical_repl import ddl, ddl_queue
from pg_logical_repl.catalog import difference, inventory, schema_manifest
from pg_logical_repl.errors import PreconditionError
from pg_logical_repl.state import write_private

migration = migration_fixture

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def started(migration, mode="captured"):
    service, sc, tc = migration
    service.config.raw["ddl"].update(mode=mode, new_tables_without_pk="replica_identity_full")
    await sc.execute(
        "CREATE TABLE public.app(id int PRIMARY KEY, n int); INSERT INTO public.app VALUES(1,10),(2,20)"
    )
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    return service, sc, tc


async def command(service, sc, tc, statement):
    if service.config.ddl_mode == "managed_only":
        path = service.store.directory / "ddl-test.sql"
        write_private(path, statement)
        plan = await service.plan("ddl-exec", extra=ddl.classify(statement))
        await ddl.execute(service, plan["path"], plan["plan_hash"], path)
    else:
        await sc.execute(statement)
        await service.barrier(sc, tc)
        await service.drain_ddl(sc, tc)


async def equal_schema(sc, tc):
    a, b = schema_manifest(await inventory(sc)), schema_manifest(await inventory(tc))
    assert a == b, (
        difference(a, b),
        [
            (x["name"], x["owner"], x["acl"], y["owner"], y["acl"])
            for x, y in zip(a["relations"], b["relations"])
            if x != y
        ],
    )


@pytest.mark.parametrize("mode", ["captured", "managed_only"])
async def test_new_tables_partitions_ctas(migration, mode):
    service, sc, tc = await started(migration, mode)
    await command(service, sc, tc, "CREATE SCHEMA extra")
    if mode == "captured":
        await sc.execute(
            "CREATE TABLE extra.new_table(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text)"
        )
        # Writes before REFRESH must be copied once, including updates and deletes.
        await sc.execute(
            "INSERT INTO extra.new_table(value) SELECT 'payload-'||g FROM generate_series(1,500) g"
        )
        await sc.execute(
            "UPDATE extra.new_table SET value='changed' WHERE id=3; DELETE FROM extra.new_table WHERE id=4"
        )
        await service.barrier(sc, tc)
        await service.drain_ddl(sc, tc)
    else:
        await command(
            service,
            sc,
            tc,
            "CREATE TABLE extra.new_table(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text)",
        )
        await sc.execute(
            "INSERT INTO extra.new_table(value) SELECT 'payload-'||g FROM generate_series(1,500) g"
        )
    await service.barrier(sc, tc)
    assert await tc.fetch("SELECT * FROM extra.new_table ORDER BY id") == await sc.fetch(
        "SELECT * FROM extra.new_table ORDER BY id"
    ), [dict(r) for r in await sc.fetch("SELECT id,statement,queue_jobs,actions FROM lc.ddl_log")]
    await command(
        service, sc, tc, "CREATE TABLE extra.parts(id int PRIMARY KEY,n text) PARTITION BY RANGE(id)"
    )
    await command(
        service, sc, tc, "CREATE TABLE extra.part0 PARTITION OF extra.parts FOR VALUES FROM(0) TO(1000)"
    )
    await sc.execute("INSERT INTO extra.parts SELECT g,'partition' FROM generate_series(1,20) g")
    await command(service, sc, tc, "CREATE TABLE extra.copied AS SELECT * FROM extra.new_table")
    await service.barrier(sc, tc)
    assert await tc.fetchval("SELECT count(*) FROM extra.copied") == await sc.fetchval(
        "SELECT count(*) FROM extra.new_table"
    )
    assert await tc.fetchval("SELECT count(*) FROM extra.parts") == 20
    assert not await tc.fetchval("SELECT EXISTS(SELECT FROM lc.table_sync WHERE state<>'ready')")
    await equal_schema(sc, tc)
    assert (await service.verify())["passed"]
    await command(service, sc, tc, "ALTER TABLE extra.copied RENAME TO renamed")
    assert not (await service.run_once())["health"]["blockers"]
    await command(service, sc, tc, "DROP TABLE extra.renamed")
    await equal_schema(sc, tc)
    await operate(service, "abort")


@pytest.mark.parametrize("mode", ["captured", "managed_only"])
async def test_expanded_ddl_objects(migration, mode):
    service, sc, tc = await started(migration, mode)
    statements = [
        "ALTER TABLE public.app ADD COLUMN value text DEFAULT 'a;--b'",
        "ALTER TABLE public.app ALTER COLUMN value SET DEFAULT 'next'",
        "ALTER TABLE public.app ALTER COLUMN n TYPE bigint USING n::bigint",
        "ALTER TABLE public.app ALTER COLUMN n SET NOT NULL",
        "ALTER TABLE public.app ADD CONSTRAINT positive CHECK(n>0)",
        "ALTER TABLE public.app ADD CONSTRAINT unique_n UNIQUE(n)",
        "CREATE INDEX CONCURRENTLY app_value_idx ON public.app(value)",
        "DROP INDEX CONCURRENTLY public.app_value_idx",
        "CREATE VIEW public.app_view AS SELECT id,n FROM public.app",
        "CREATE FUNCTION public.twice(x bigint) RETURNS bigint LANGUAGE SQL IMMUTABLE AS $$ SELECT x*2; $$",
        "CREATE FUNCTION public.mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.value := 'triggered'; RETURN NEW; END $$",
        "CREATE TRIGGER app_mark BEFORE INSERT ON public.app FOR EACH ROW EXECUTE FUNCTION public.mark()",
        "ALTER TABLE public.app DISABLE TRIGGER app_mark",
        "ALTER TABLE public.app ENABLE TRIGGER app_mark",
        "CREATE SEQUENCE public.extra_seq START 10 INCREMENT 3",
        "ALTER SEQUENCE public.extra_seq INCREMENT 5 CACHE 10",
        "COMMENT ON TABLE public.app IS 'table; comment -- literal'",
        "COMMENT ON COLUMN public.app.n IS 'number'",
        "GRANT SELECT ON TABLE public.app TO PUBLIC",
        "REVOKE SELECT ON TABLE public.app FROM PUBLIC",
        "CREATE MATERIALIZED VIEW public.app_mat AS SELECT count(*) n FROM public.app",
    ]
    for statement in statements:
        await command(service, sc, tc, statement)
    await sc.fetchval("SELECT setval('public.extra_seq',1000)")
    await command(service, sc, tc, "ALTER SEQUENCE public.extra_seq START 900 MINVALUE 900")
    assert await tc.fetchval("SELECT last_value FROM public.extra_seq") == 1000
    await sc.execute("INSERT INTO public.app(id,n) VALUES(3,30)")
    await service.barrier(sc, tc)
    assert await tc.fetchval("SELECT value FROM public.app WHERE id=3") == "triggered"
    assert await tc.fetchval("SELECT public.twice(4)") == 8
    assert await tc.fetchval(
        "SELECT convalidated FROM pg_constraint WHERE conrelid='app'::regclass AND conname='positive'"
    )
    assert (
        await tc.fetchval("SELECT count(*) FROM lc.ddl_queue WHERE kind='create_index' AND status='done'")
        == 2
    )
    await equal_schema(sc, tc)
    # The materialized view is deliberately unpopulated until cutover.
    assert not await tc.fetchval("SELECT relispopulated FROM pg_class WHERE oid='app_mat'::regclass")
    await operate(service, "abort")


async def test_queue_invalid_index_retry_and_dependencies(migration):
    service, sc, tc = await started(migration)
    service.config.raw["daemon"] = {"queue_max_attempts": 2}
    # A target-only duplicate causes a real concurrent build failure/INVALID index.
    await tc.execute("INSERT INTO public.app VALUES(3,10)")
    await sc.execute("CREATE UNIQUE INDEX app_unique_n ON public.app(n)")
    with pytest.raises(asyncpg.PostgresError, match="preceding DDL queue"):
        await sc.execute("ALTER TABLE public.app ADD COLUMN blocked text")
    for _ in range(40):
        status = await service.run_once()
        if status["state"] == "BLOCKED":
            break
        await asyncio.sleep(0.1)
    assert status["state"] == "BLOCKED"
    assert not await tc.fetchval("SELECT indisvalid FROM pg_index WHERE indexrelid='app_unique_n'::regclass")
    job = (await ddl_queue.listing(service))[0]
    assert job["status"] == "failed" and job["attempts"] == 2
    await tc.execute("DELETE FROM public.app WHERE id=3")
    await ddl_queue.retry(service, job["id"])
    await service.drain_ddl(sc, tc)
    assert await tc.fetchval("SELECT indisvalid FROM pg_index WHERE indexrelid='app_unique_n'::regclass")
    await command(service, sc, tc, "ALTER TABLE public.app ADD COLUMN unblocked text")
    await equal_schema(sc, tc)
    await operate(service, "abort")


async def test_queue_lost_reply_and_skip(migration, monkeypatch):
    service, sc, tc = await started(migration)
    original = ddl_queue.execute_job
    lost = False

    async def lose_reply(service, sc, tc, job):
        nonlocal lost
        done = await original(service, sc, tc, job)
        if not lost:
            lost = True
            raise TimeoutError("injected lost acknowledgement")
        return done

    monkeypatch.setattr(ddl_queue, "execute_job", lose_reply)
    await command(service, sc, tc, "CREATE INDEX first_idx ON public.app(n)")
    assert (await ddl_queue.listing(service))[0]["attempts"] == 2
    monkeypatch.setattr(ddl_queue, "execute_job", original)
    await sc.execute("CREATE INDEX skipped_idx ON public.app(n)")

    async def fail(*args):
        raise PreconditionError("INJECTED", "injected operator decision")

    monkeypatch.setattr(ddl_queue, "execute_job", fail)
    for _ in range(30):
        status = await service.run_once()
        if status["state"] == "BLOCKED":
            break
        await asyncio.sleep(0.1)
    job = (await ddl_queue.listing(service))[-1]
    reason = "Test intervention: index will be created manually before cutover"
    evidence = service.store.directory / "skip-evidence.json"
    write_private(
        evidence,
        json.dumps(
            dict(
                reason=reason,
                approved_by="integration-test",
                verification="Schema mismatch is intentional and must block cutover",
            )
        ),
    )
    plan = await service.plan(
        "ddl-queue-skip", extra=dict(job_id=job["id"], **ddl_queue.read_evidence(evidence))
    )
    await ddl_queue.skip(service, plan["path"], plan["plan_hash"], job["id"], reason, evidence_file=evidence)
    monkeypatch.setattr(ddl_queue, "execute_job", original)
    await service.drain_ddl(sc, tc)
    assert (await ddl_queue.listing(service))[-1]["status"] == "skipped"
    assert not (await service.verify())["passed"]
    await operate(service, "abort")


async def test_capture_rollback_and_guard_limits(migration):
    service, sc, tc = await started(migration)
    before = await sc.fetchval("SELECT count(*) FROM lc.ddl_log")
    tx = sc.transaction()
    await tx.start()
    await sc.execute("CREATE TABLE public.rolled_back(id int PRIMARY KEY)")
    await sc.execute("INSERT INTO public.rolled_back VALUES(1)")
    await tx.rollback()
    assert await sc.fetchval("SELECT count(*) FROM lc.ddl_log") == before
    assert not await sc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_publication_tables WHERE pubname=$1 AND tablename='rolled_back')",
        service.config.publication,
    )
    for sql in [
        "ALTER TABLE public.app DROP COLUMN id",
        "ALTER TABLE public.app ADD COLUMN n2 int DEFAULT random()::int",
        "DROP TABLE public.app CASCADE",
        "ALTER TABLE public.app ADD COLUMN x int; DROP TABLE public.app",
        "ALTER TABLE public.app REPLICA IDENTITY NOTHING",
    ]:
        with pytest.raises(asyncpg.PostgresError):
            await sc.execute(sql)
    await service.barrier(sc, tc)
    assert await sc.fetchval("SELECT count(*) FROM lc.ddl_log") == before
    await operate(service, "abort")


@pytest.mark.parametrize("mode", ["captured", "managed_only"])
async def test_types_default_acl_and_partition_membership(migration, mode):
    service, sc, tc = await started(migration, mode)
    statements = [
        "CREATE TYPE public.phase AS ENUM ('a','b')",
        "ALTER TYPE public.phase ADD VALUE 'c'",
        "CREATE DOMAIN public.positive_int AS integer CHECK(VALUE>0)",
        "ALTER DOMAIN public.positive_int SET DEFAULT 1",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC",
        "CREATE TABLE public.typed(id int PRIMARY KEY,phase public.phase,n public.positive_int)",
        "CREATE TABLE public.ref(id int PRIMARY KEY,ref int)",
        "ALTER TABLE public.ref ADD CONSTRAINT ref_fk FOREIGN KEY(ref) REFERENCES public.typed(id) ON DELETE CASCADE",
        "CREATE TABLE public.parts(id int PRIMARY KEY) PARTITION BY RANGE(id)",
        "CREATE TABLE public.leaf(id int PRIMARY KEY)",
        "ALTER TABLE public.parts ATTACH PARTITION public.leaf FOR VALUES FROM(0) TO(10)",
    ]
    for statement in statements:
        await command(service, sc, tc, statement)
    await sc.execute(
        "INSERT INTO public.typed VALUES(1,'c',5); INSERT INTO public.ref VALUES(1,1); INSERT INTO public.parts VALUES(1)"
    )
    await service.barrier(sc, tc)
    await command(service, sc, tc, "CREATE INDEX parts_id_idx ON public.parts(id)")
    await equal_schema(sc, tc)
    await command(service, sc, tc, "DROP INDEX public.parts_id_idx")
    await command(service, sc, tc, "ALTER TABLE public.parts DETACH PARTITION public.leaf")
    await sc.execute("INSERT INTO public.leaf VALUES(2)")
    await service.barrier(sc, tc)
    assert await tc.fetchval("SELECT count(*) FROM public.leaf") == 2
    await sc.execute("DELETE FROM public.typed WHERE id=1")
    await service.barrier(sc, tc)
    assert await tc.fetchval("SELECT count(*) FROM public.ref") == 0
    await equal_schema(sc, tc)
    assert (await service.verify())["passed"]
    await operate(service, "abort")


async def test_managed_concurrent_source_recovery(migration, monkeypatch):
    service, sc, tc = await started(migration, "managed_only")
    statement = "CREATE INDEX CONCURRENTLY recovered_idx ON public.app(n)"
    path = service.store.directory / "concurrent.sql"
    write_private(path, statement)
    plan = await service.plan("ddl-exec", extra=ddl.classify(statement))
    # Emulate the PostgreSQL boundary: the concurrent build committed but its
    # final journal transaction/reply was lost. A durable source intent predates it.
    import json

    await sc.fetchval(
        "SELECT set_config('lc.ownership_token',$1,false)", service.store.read()["ownership_token"]
    )
    await sc.execute(
        "INSERT INTO lc.operations(name,details) VALUES($1,$2::jsonb)",
        "ddl_concurrent:" + plan["plan_hash"],
        json.dumps(
            dict(
                plan_hash=plan["plan_hash"],
                schema="public",
                name="recovered_idx",
                kind="create_index",
                oid=None,
                complete=False,
            )
        ),
    )
    await sc.execute(statement)  # owned-token maintenance bypass, deliberately no journal
    state = service.store.read()
    state["active_plan"] = plan["plan_hash"]
    service.store.save(state)
    result = await ddl.execute(service, plan["path"], plan["plan_hash"], path)
    assert result["applied"]
    assert await tc.fetchval("SELECT indisvalid FROM pg_index WHERE indexrelid='recovered_idx'::regclass")
    assert await sc.fetchval("SELECT count(*) FROM lc.ddl_log") == 1
    await equal_schema(sc, tc)
    await operate(service, "abort")


async def test_capture_role_search_path_and_rewrite_limit(migration):
    service, sc, tc = migration
    from pg_logical_repl.security import identifier as qi

    role = service.config.migration_id + "_owner"
    await sc.execute("CREATE ROLE " + qi(role) + " NOLOGIN")
    await sc.execute("CREATE SCHEMA " + qi(role) + " AUTHORIZATION " + qi(role))
    service.config.raw["ddl"]["rewrite_max_table_bytes"] = 0
    await started(migration)
    await sc.execute("SET ROLE " + qi(role))
    await sc.execute('SET search_path="$user",public')
    await sc.execute("CREATE TABLE owned(id int PRIMARY KEY,n int)")
    await sc.execute("INSERT INTO owned VALUES(1,10)")
    await sc.execute("RESET ROLE")
    await service.barrier(sc, tc)
    await service.drain_ddl(sc, tc)
    await sc.execute("SET ROLE " + qi(role))
    # A caller cannot bypass the guard by inventing its internal marker.
    await sc.execute("SET lc.internal='on'")
    with pytest.raises(asyncpg.PostgresError, match="replica identity"):
        await sc.execute("ALTER TABLE owned DROP COLUMN id")
    with pytest.raises(asyncpg.PostgresError):
        await sc.execute("SELECT lc.record_ddl('','','[]','[]')")
    await sc.execute("RESET ROLE")
    await service.barrier(sc, tc)
    await service.drain_ddl(sc, tc)
    table = qi(role) + ".owned"
    assert await tc.fetchval("SELECT n FROM " + table) == 10
    assert (
        await tc.fetchval("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=to_regclass($1)", table)
        == role
    )
    await sc.execute("SET ROLE " + qi(role))
    with pytest.raises(asyncpg.PostgresError, match="size limit"):
        await sc.execute("ALTER TABLE owned ALTER COLUMN n TYPE bigint")
    await sc.execute("RESET ROLE")
    await sc.execute("SET search_path=public")
    await equal_schema(sc, tc)
    await operate(service, "abort")


async def test_constraint_variants_and_full_identity_guard(migration):
    service, sc, tc = await started(migration)
    for statement in [
        "CREATE UNIQUE INDEX candidate_n ON public.app(n)",
        "ALTER TABLE public.app ADD CONSTRAINT chosen_n UNIQUE USING INDEX candidate_n",
        "ALTER TABLE public.app ADD CHECK(n>0)",
        "CREATE TABLE public.parts(id int PRIMARY KEY,n int) PARTITION BY RANGE(id)",
        "CREATE TABLE public.leaf PARTITION OF public.parts FOR VALUES FROM(0) TO(10)",
    ]:
        await command(service, sc, tc, statement)
    await sc.execute("INSERT INTO public.parts VALUES(1,10),(2,20)")
    await service.barrier(sc, tc)
    for statement in [
        "ALTER TABLE public.parts ADD CONSTRAINT parts_check CHECK(n>0)",
        "ALTER TABLE public.parts ADD CONSTRAINT parts_unique UNIQUE(id,n)",
        "ALTER TABLE public.parts ADD CONSTRAINT parts_fk FOREIGN KEY(id) REFERENCES public.app(id)",
    ]:
        await command(service, sc, tc, statement)
    with pytest.raises(asyncpg.PostgresError, match="equality types"):
        await sc.execute("CREATE TABLE public.unsupported_full(payload json)")
    assert not await sc.fetchval("SELECT to_regclass('public.unsupported_full') IS NOT NULL")
    await equal_schema(sc, tc)
    assert (await service.verify())["passed"]
    await operate(service, "abort")


async def test_drop_queue_rejects_replaced_index(migration, monkeypatch):
    service, sc, tc = await started(migration)
    await command(service, sc, tc, "CREATE INDEX replace_test ON public.app(n)")
    original = ddl_queue.execute_job
    lost = False

    async def lose_drop(service, sc, tc, job):
        nonlocal lost
        done = await original(service, sc, tc, job)
        if job["kind"] == "drop_index" and not lost:
            lost = True
            # Emulate an operator replacing the name after our successful DROP.
            await tc.execute("CREATE INDEX replace_test ON public.app(n)")
            raise TimeoutError("injected lost drop reply")
        return done

    monkeypatch.setattr(ddl_queue, "execute_job", lose_drop)
    await sc.execute("DROP INDEX public.replace_test")
    for _ in range(30):
        status = await service.run_once()
        if status["state"] == "BLOCKED":
            break
        await asyncio.sleep(0.1)
    assert status["state"] == "BLOCKED"
    assert await tc.fetchval("SELECT to_regclass('public.replace_test') IS NOT NULL")
    job = (await ddl_queue.listing(service))[-1]
    assert "changed" in job["last_error"]
    monkeypatch.setattr(ddl_queue, "execute_job", original)
    await tc.execute("DROP INDEX public.replace_test")
    await ddl_queue.retry(service, job["id"])
    await service.drain_ddl(sc, tc)
    await equal_schema(sc, tc)
    await operate(service, "abort")


async def test_new_table_renamed_before_refresh(migration):
    service, sc, tc = await started(migration)
    async with sc.transaction():
        await sc.execute("CREATE TABLE public.before_name(id int PRIMARY KEY,value int)")
        await sc.execute("INSERT INTO public.before_name SELECT g,g FROM generate_series(1,1000) g")
        await sc.execute("ALTER TABLE public.before_name RENAME TO after_name")
        await sc.execute("UPDATE public.after_name SET value=value+1")
    await service.barrier(sc, tc)
    await service.drain_ddl(sc, tc)
    state = await tc.fetchrow(
        "SELECT table_name,state FROM lc.table_sync WHERE relation_oid='after_name'::regclass"
    )
    assert dict(state) == dict(table_name="after_name", state="ready")
    assert await tc.fetch("SELECT * FROM public.after_name ORDER BY id") == await sc.fetch(
        "SELECT * FROM public.after_name ORDER BY id"
    )
    await equal_schema(sc, tc)
    await operate(service, "abort")
