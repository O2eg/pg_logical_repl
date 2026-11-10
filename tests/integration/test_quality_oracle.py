"""Independent validation and intentional corruptions of the real target."""

import json
import os

import pytest
from support.oracle import compare, quote
from test_lifecycle import migration as migration_fixture
from test_lifecycle import operate, ready

from pg_logical_repl.state import now, write_private

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.quality,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


def fence(service):
    path = service.config.path.parent / "fence.json"
    write_private(
        path,
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=service.store.read()["source_identity"],
                created_at=now(),
                responsible="test harness",
                external_fence="All owned fixture writers stopped and joined",
            )
        ),
    )
    return path


async def test_independent_oracle_exact_values_metadata_and_corruption(migration):
    service, sc, tc = migration
    service.config.raw["ddl"]["mode"] = "captured"
    role = service.config.migration_id + "_app"
    await sc.execute("CREATE ROLE " + quote(role) + " LOGIN")
    await sc.execute("""CREATE TYPE public.phase AS ENUM('a','b');
        CREATE DOMAIN public.positive AS numeric CHECK(VALUE>0);
        CREATE TABLE public.app(id int PRIMARY KEY,n numeric(30,8),payload text,raw bytea,phase public.phase);
        INSERT INTO app SELECT n,n::numeric/7,repeat('тест',100),decode('00ff','hex'),'a' FROM generate_series(1,600) n;
        CREATE TABLE public.dups(v text); ALTER TABLE dups REPLICA IDENTITY FULL;
        INSERT INTO dups VALUES(NULL),('same'),('same');
        CREATE TABLE public.child(id int PRIMARY KEY,p int REFERENCES public.app(id)); INSERT INTO child VALUES(1,1);
        CREATE VIEW public.names AS SELECT id,payload FROM public.app;
        CREATE FUNCTION public.twice(x int) RETURNS int LANGUAGE sql IMMUTABLE AS $$SELECT x*2$$;
        CREATE UNLOGGED TABLE public.cache(id int PRIMARY KEY); INSERT INTO cache VALUES(99);
        CREATE SEQUENCE public.ticket START 100 INCREMENT -1 MAXVALUE 100; SELECT nextval('ticket'); ALTER SEQUENCE ticket OWNED BY app.id;
        CREATE MATERIALIZED VIEW public.total AS SELECT sum(n) AS n FROM app;
        SELECT lo_from_bytea(481239,decode('0011ff','hex'));
        COMMENT ON TABLE app IS 'independent fixture';""")
    await sc.execute("GRANT SELECT ON app TO " + quote(role))
    await operate(service, "prepare")
    await operate(service, "start")
    await ready(service)
    executor = "lc_ddl_executor_" + service.config.migration_id
    assert (await compare(sc, tc, executor=executor))["passed"]
    await operate(service, "cutover-finalize", write_fence=fence(service))
    assert (await compare(sc, tc, executor=executor, deferred=True))["passed"]
    corruptions = [
        ("ALTER SEQUENCE ticket OWNED BY NONE", "catalog differs: sequence_ownership"),
        ("UPDATE app SET n=n+1 WHERE id=400", "differs at row"),
        ("DELETE FROM dups WHERE ctid=(SELECT ctid FROM dups WHERE v='same' LIMIT 1)", "differs at row"),
        ("ALTER TABLE app ADD COLUMN unexpected text", "catalog differs"),
        ("ALTER TABLE child DROP CONSTRAINT child_p_fkey", "catalog differs"),
        ("REVOKE SELECT ON app FROM " + quote(role), "catalog differs"),
        ("ALTER ROLE " + quote(role) + " NOLOGIN", "catalog differs"),
        ("GRANT pg_read_all_data TO " + quote(role), "catalog differs"),
        ("SELECT lo_put(481239,1,decode('ee','hex'))", "large object pages"),
    ]
    for statement, expected in corruptions:
        tx = tc.transaction()
        await tx.start()
        try:
            await tc.execute(statement)
            with pytest.raises(AssertionError, match=expected):
                await compare(sc, tc, executor=executor, deferred=True)
        finally:
            await tx.rollback()
    await tc.execute("SELECT setval('ticket',42,true)")
    with pytest.raises(AssertionError, match="sequence value"):
        await compare(sc, tc, executor=executor, deferred=True)
    await tc.execute("SELECT setval('ticket',100,true)")
    await operate(service, "cutover-commit")
    assert (await compare(sc, tc, deferred=True))["passed"]
