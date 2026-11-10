"""Owned-database experiment: DDL row-trigger replay inside PostgreSQL apply.

Run manually on pg-stand, not against application databases. Credentials stay private.
"""

import asyncio
import json
import time
from pathlib import Path

import asyncpg

from pg_logical_repl.security import conninfo, identifier, literal

ROOT = Path(__file__).resolve().parents[2]


async def main():
    credentials = json.loads((ROOT / ".pg_stand/credentials/database/passwords.json").read_text())
    pw = credentials["superuser"]["password"]
    user = credentials["superuser"]["username"]
    db = "lc_spike_" + str(int(time.time()))
    source = await asyncpg.connect(host="127.0.0.1", port=55481, user=user, password=pw, database="postgres")
    target = await asyncpg.connect(host="127.0.0.1", port=55483, user=user, password=pw, database="postgres")
    for c in (source, target):
        await c.execute("CREATE DATABASE " + identifier(db))
    await source.close()
    await target.close()
    source = await asyncpg.connect(host="127.0.0.1", port=55481, user=user, password=pw, database=db)
    target = await asyncpg.connect(host="127.0.0.1", port=55483, user=user, password=pw, database=db)
    evidence = []
    try:
        schema = """CREATE TABLE public.app(id integer PRIMARY KEY, n integer);
        CREATE SCHEMA lc;
        CREATE TABLE lc.ddl_log(id bigint PRIMARY KEY, statement text NOT NULL);
        CREATE TABLE lc.barrier(id integer PRIMARY KEY);"""
        for c in (source, target):
            await c.execute(schema)
        await target.execute("""CREATE FUNCTION lc.replay() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN EXECUTE NEW.statement; RETURN NEW; END $$;
        CREATE TRIGGER replay AFTER INSERT ON lc.ddl_log FOR EACH ROW EXECUTE FUNCTION lc.replay();
        ALTER TABLE lc.ddl_log ENABLE REPLICA TRIGGER replay;""")
        await source.execute("INSERT INTO public.app VALUES (1,10)")
        await source.execute("CREATE PUBLICATION lc_spike_pub FOR TABLE public.app,lc.ddl_log,lc.barrier")
        ci = conninfo(
            dict(
                host="lc-development18-primary-pg-stand-managed",
                port=5432,
                user=user,
                password=pw,
                dbname=db,
                sslmode="disable",
            )
        )
        await target.execute(
            "CREATE SUBSCRIPTION lc_spike_sub CONNECTION "
            + literal(ci)
            + " PUBLICATION lc_spike_pub WITH (streaming=on, synchronous_commit=on)"
        )

        async def wait_ready():
            for _ in range(100):
                if await target.fetchval(
                    "SELECT count(*)=3 AND bool_and(srsubstate='r') FROM pg_subscription_rel"
                ):
                    return
                await asyncio.sleep(0.1)
            raise RuntimeError("initial sync timed out")

        await wait_ready()

        async def barrier(n):
            await source.execute("INSERT INTO lc.barrier VALUES($1)", n)
            for _ in range(80):
                if await target.fetchval("SELECT EXISTS(SELECT FROM lc.barrier WHERE id=$1)", n):
                    return
                await asyncio.sleep(0.1)
            stats = [dict(x) for x in await target.fetch("SELECT * FROM pg_stat_subscription_stats")]
            raise RuntimeError("barrier timed out, stats=" + str(stats))

        async with source.transaction():
            await source.execute("ALTER TABLE public.app ADD COLUMN extra text")
            await source.execute(
                "INSERT INTO lc.ddl_log VALUES(1,$1)", "ALTER TABLE public.app ADD COLUMN extra text"
            )
            await source.execute("INSERT INTO public.app VALUES(2,20,'after add')")
        await barrier(1)
        evidence.append(
            {
                "scenario": "add_column_then_insert",
                "passed": await target.fetchval("SELECT extra FROM public.app WHERE id=2") == "after add",
            }
        )
        async with source.transaction():
            await source.execute("UPDATE public.app SET n=n+1 WHERE id=1")
            await source.execute("ALTER TABLE public.app RENAME COLUMN n TO value")
            await source.execute(
                "INSERT INTO lc.ddl_log VALUES(2,$1)", "ALTER TABLE public.app RENAME COLUMN n TO value"
            )
            await source.execute("UPDATE public.app SET value=value+2 WHERE id=1")
        await barrier(2)
        evidence.append(
            {
                "scenario": "dml_rename_dml_same_transaction",
                "passed": await target.fetchval("SELECT value FROM public.app WHERE id=1") == 13,
            }
        )
        tx = source.transaction()
        await tx.start()
        await source.execute("ALTER TABLE public.app ADD COLUMN rolled_back int")
        await source.execute(
            "INSERT INTO lc.ddl_log VALUES(3,$1)", "ALTER TABLE public.app ADD COLUMN rolled_back int"
        )
        await tx.rollback()
        await barrier(3)
        evidence.append(
            {
                "scenario": "ddl_rollback",
                "passed": not await target.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_attribute WHERE attrelid='public.app'::regclass AND attname='rolled_back')"
                ),
            }
        )
        async with source.transaction():
            await source.execute("ALTER TABLE public.app DROP COLUMN extra")
            await source.execute(
                "INSERT INTO lc.ddl_log VALUES(4,$1)", "ALTER TABLE public.app DROP COLUMN extra"
            )
            await source.execute("INSERT INTO public.app VALUES(3,30)")
        await barrier(4)
        evidence.append(
            {
                "scenario": "drop_column_then_insert",
                "passed": await target.fetchval("SELECT value FROM public.app WHERE id=3") == 30,
            }
        )
    except Exception as exc:
        evidence.append(
            {"scenario": "exception", "passed": False, "error": str(exc).replace(pw, "[REDACTED]")}
        )
    finally:
        if await target.fetchval("SELECT EXISTS(SELECT FROM pg_subscription WHERE subname='lc_spike_sub')"):
            await target.execute("DROP SUBSCRIPTION lc_spike_sub")
        await source.close()
        await target.close()
        for port in (55481, 55483):
            c = await asyncpg.connect(
                host="127.0.0.1", port=port, user=user, password=pw, database="postgres"
            )
            await c.execute("DROP DATABASE " + identifier(db))
            await c.close()
    print(json.dumps(evidence, indent=2))
    (ROOT / "docs" / "spike_pg18.json").write_text(json.dumps(evidence, indent=2) + "\n")
    if not all(x["passed"] for x in evidence):
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
