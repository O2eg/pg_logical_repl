"""Sustained installed-CLI migration, transactional writers and captured DDL."""

import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from .cli import CLI, persist_config
from .oracle import compare


def rss(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except FileNotFoundError:
        pass
    return None


async def exercise(service, sc, tc, proxies, *, duration, rows, writers, directory):
    assert duration >= 5 and rows >= writers and 1 <= writers <= 8
    service.config.raw["ddl"]["mode"] = "captured"
    service.config.raw["validation"] = dict(
        online=True, chunk_rows=256, max_retries=2, pause_between_chunks_ms=1
    )
    service.config.raw["daemon"] = dict(
        poll_interval_seconds=1, drift_interval_seconds=5, metrics_textfile="metrics.prom"
    )
    service.config.raw["sequences"] = dict(presync_interval_seconds=3)
    service.config.raw["cutover"] = dict(analyze="all")
    service.config.raw["limits"]["operation_timeout_seconds"] = 120
    persist_config(service)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    cli = CLI(service, directory / "cli")
    await sc.execute("""CREATE TABLE public.accounts(id int PRIMARY KEY,balance bigint NOT NULL,payload text);
        ALTER TABLE accounts ALTER COLUMN payload SET STORAGE EXTERNAL;
        CREATE TABLE public.changes(worker int,seq bigint,amount int,PRIMARY KEY(worker,seq));
        CREATE TABLE public.scratch(worker int,seq bigint,PRIMARY KEY(worker,seq));
        CREATE UNLOGGED TABLE public.cache(worker int PRIMARY KEY,n bigint);
        CREATE SEQUENCE public.ticket START 100;
        CREATE MATERIALIZED VIEW public.total AS SELECT count(*) AS n FROM changes;""")
    await sc.execute(
        "INSERT INTO accounts SELECT n,0,repeat(md5(n::text),64) FROM generate_series(1,$1::int) n", rows
    )
    for worker in range(writers):
        await sc.execute("INSERT INTO cache VALUES($1,0)", worker)
        await sc.fetchval("SELECT lo_from_bytea($1,decode('00ff','hex'))", 481300 + worker)
    await cli.operate("prepare")
    # A real SHARE lock prevents tablesync INSERT while application transactions
    # commit. This proves overlap even on a fast machine with a small fixture.
    copy_gate = tc.transaction()
    await copy_gate.start()
    await tc.execute("LOCK TABLE public.accounts IN SHARE MODE")
    stop = asyncio.Event()
    counters = [0] * writers
    ddl_count = 0
    tasks = []
    daemon = None
    started = time.monotonic()
    bootstrap_started = started
    gate_released = False
    events = []
    samples = []

    async def writer(worker):
        async with service.source.connect() as c:
            while not stop.is_set():
                number = counters[worker] + 1
                async with c.transaction():
                    await c.execute("UPDATE accounts SET balance=balance+1 WHERE id=$1", worker + 1)
                    await c.execute("INSERT INTO changes VALUES($1,$2,1)", worker, number)
                    await c.execute("INSERT INTO scratch VALUES($1,$2)", worker, number)
                    await c.execute("DELETE FROM scratch WHERE worker=$1 AND seq<$2", worker, number - 20)
                    await c.fetchval("SELECT nextval('ticket')")
                    if number % 100 == 0:
                        await c.execute("UPDATE cache SET n=$2 WHERE worker=$1", worker, number)
                        await c.fetchval("SELECT lo_put($1,0,$2)", 481300 + worker, number.to_bytes(8, "big"))
                counters[worker] = number
                await asyncio.sleep(0.05)

    async def ddl_writer():
        nonlocal ddl_count
        statements = [
            "ALTER TABLE public.accounts ADD COLUMN cycle_value integer DEFAULT 0",
            "ALTER TABLE public.accounts RENAME COLUMN cycle_value TO cycle_renamed",
            "CREATE INDEX quality_balance_idx ON public.accounts(balance)",
            "DROP INDEX public.quality_balance_idx",
            "ALTER TABLE public.accounts DROP COLUMN cycle_renamed",
            "CREATE TABLE public.round_table(id int PRIMARY KEY,n int)",
            "ALTER TABLE public.round_table RENAME TO round_renamed",
            "DROP TABLE public.round_renamed",
        ]
        async with service.source.connect() as c:
            while not stop.is_set():
                statement = statements[ddl_count % len(statements)]
                deadline = time.monotonic() + 120
                while True:
                    try:
                        await c.execute(statement)
                        if statement.startswith("CREATE TABLE"):
                            await c.execute(
                                "INSERT INTO public.round_table SELECT n,n FROM generate_series(1,100) n"
                            )
                            await c.execute("UPDATE public.round_table SET n=n+1 WHERE id=1")
                        ddl_count += 1
                        break
                    except Exception as exc:
                        if getattr(exc, "sqlstate", None) != "LC002" or time.monotonic() > deadline:
                            raise
                        await asyncio.sleep(0.1)
                await asyncio.sleep(min(2, duration / 20))

    try:
        await cli.operate("start")
        daemon = await cli.spawn("run")
        tasks = [asyncio.create_task(writer(i)) for i in range(writers)]
        async with asyncio.timeout(30):
            while min(counters) < 3:
                for task in tasks:
                    if task.done():
                        task.result()
                await asyncio.sleep(0.05)
        copy_state = await tc.fetchval(
            "SELECT srsubstate::text FROM pg_subscription_rel WHERE srrelid='accounts'::regclass"
        )
        assert copy_state != "r", "COPY must still be pending while writers commit"
        overlap_transactions = sum(counters)
        await copy_gate.rollback()
        gate_released = True
        await cli.wait_state(daemon, "REPLICATING", timeout=120)
        bootstrap_seconds = time.monotonic() - bootstrap_started
        started = time.monotonic()
        tasks.append(asyncio.create_task(ddl_writer()))
        dropped = False
        restart = 0
        outage_until = None
        while time.monotonic() - started < duration:
            elapsed = time.monotonic() - started
            for task in tasks:
                if task.done():
                    task.result()
                    raise AssertionError("Workload task stopped prematurely")
            if daemon.returncode is not None:
                output, _ = await daemon.communicate()
                raise AssertionError("CLI daemon exited: " + output.decode()[-1000:])
            state = service.store.read()
            assert state["state"] != "BLOCKED", str(state.get("blocked_details"))
            if restart < 2 and elapsed > duration * (restart + 1) / 3:
                await cli.stop(daemon, hard=restart == 0)
                daemon = await cli.spawn("run")
                events.append(dict(event="controller_restart", hard=restart == 0, seconds=elapsed))
                restart += 1
            if not dropped and elapsed > duration / 2:
                proxies["replication"].set_mode("down")
                outage_until = time.monotonic() + min(3, duration / 10)
                dropped = True
                events.append(dict(event="replication_disconnected", seconds=elapsed))
            if outage_until and time.monotonic() >= outage_until:
                proxies["replication"].set_mode("normal")
                outage_until = None
                events.append(dict(event="replication_restored", seconds=elapsed))
            slot = await sc.fetchrow(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint lag_bytes,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint retained_bytes FROM pg_replication_slots WHERE slot_name=$1",
                service.config.subscription,
            )
            sample = dict(
                seconds=elapsed,
                transactions=sum(counters),
                ddl=ddl_count,
                rss_bytes=rss(daemon.pid),
                **dict(slot),
            )
            samples.append(sample)
            with (directory / "samples.jsonl").open("a") as stream:
                stream.write(json.dumps(sample) + "\n")
            if len(samples) % 30 == 0:
                print(json.dumps(dict(progress="cli-workload", **sample)), flush=True)
            await asyncio.sleep(1)
    finally:
        stop.set()
        for proxy in proxies.values():
            proxy.set_mode("normal")
        # Stop the DDL producer at a statement boundary; committed writes have
        # their counters recorded before the coroutine returns.
        try:
            if not gate_released:
                await copy_gate.rollback()
            outcomes = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 130)
        finally:
            if daemon is not None:
                await cli.stop(daemon)
    failures = [value for value in outcomes if isinstance(value, BaseException)]
    if failures:
        raise failures[0]
    await service.barrier(sc, tc)
    await service.drain_ddl(sc, tc)
    for c in (sc, tc):
        assert await c.fetchval("SELECT count(*) FROM changes") == sum(counters)
        for worker, count in enumerate(counters):
            assert await c.fetchval("SELECT balance FROM accounts WHERE id=$1", worker + 1) == count
    await compare(sc, tc, executor="lc_ddl_executor_" + service.config.migration_id)
    evidence = directory / "fence.json"
    evidence.write_text(
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=service.store.read()["source_identity"],
                created_at=datetime.now(UTC).isoformat(),
                responsible="quality harness",
                external_fence="All workload tasks stopped and joined; target has no application writers",
            )
        )
    )
    await cli.operate("cutover-finalize", "--write-fence", str(evidence))
    final = await compare(sc, tc, executor="lc_ddl_executor_" + service.config.migration_id, deferred=True)
    await cli.operate("cutover-commit")
    await compare(sc, tc, deferred=True)
    await tc.execute("INSERT INTO accounts(id,balance,payload) VALUES(-1,0,'independent target')")
    assert not await sc.fetchval("SELECT EXISTS(SELECT FROM accounts WHERE id=-1)")
    assert not await tc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", service.config.subscription
    )
    assert not await sc.fetchval(
        "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", service.config.subscription
    )
    assert ddl_count >= 8 and restart == 2 and dropped
    summary = dict(
        passed=True,
        duration_seconds=time.monotonic() - started,
        requested_seconds=duration,
        initial_rows=rows,
        bootstrap_seconds=bootstrap_seconds,
        transactions_during_copy=overlap_transactions,
        writers=writers,
        transactions=counters,
        ddl_statements=ddl_count,
        events=events,
        max_rss_bytes=max(s["rss_bytes"] or 0 for s in samples),
        max_lag_bytes=max(s["lag_bytes"] or 0 for s in samples),
        max_retained_bytes=max(s["retained_bytes"] or 0 for s in samples),
        oracle=final,
        independent_target_write=True,
    )
    (directory / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary
