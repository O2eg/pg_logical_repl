"""Real TCP failures, including the subscription's independent network path."""

import asyncio
import os

import asyncpg
import pytest
from support.cli import CLI
from support.network import routed
from support.oracle import compare
from test_lifecycle import MAJOR, operate, ready
from test_lifecycle import migration as migration_fixture

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.quality,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def wait_value(c, sql, *args, timeout=45):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if result := await c.fetchval(sql, *args):
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("Database condition was not reached")


@pytest.mark.parametrize("path", ["source", "target", "replication"])
async def test_each_network_path_fails_independently_and_catches_up(migration, path):
    original, sc, tc = migration
    original.config.raw["ddl"]["mode"] = "captured"
    async with routed(original, MAJOR) as (service, proxies):
        await sc.execute("CREATE TABLE app(id int PRIMARY KEY,n int); INSERT INTO app VALUES(1,10)")
        await operate(service, "prepare")
        await operate(service, "start")
        await ready(service)
        proxies[path].set_mode("down")
        try:
            if path == "replication":
                await wait_value(
                    sc,
                    "SELECT NOT active FROM pg_replication_slots WHERE slot_name=$1",
                    service.config.subscription,
                )
            await sc.execute("ALTER TABLE public.app ADD COLUMN note text")
            await sc.execute("INSERT INTO app VALUES(2,20,'after DDL')")
            if path == "replication":
                assert await tc.fetchval("SELECT count(*) FROM app") == 1
                status = await service.status()
                assert status["subscription"]["subenabled"]
            else:
                status = await service.status()
                assert status["availability"][path] is False
                await wait_value(tc, "SELECT EXISTS(SELECT FROM app WHERE id=2)")
        finally:
            proxies[path].set_mode("normal")
        await service.barrier(sc, tc)
        assert (await compare(sc, tc, executor="lc_ddl_executor_" + service.config.migration_id))["passed"]
        await operate(service, "abort")


async def test_stalled_and_delayed_tcp_connection(migration):
    original, sc, tc = migration
    async with routed(original, MAJOR) as (service, proxies):
        async with service.source.connect() as c:
            proxies["source"].set_mode("stall")
            query = asyncio.create_task(c.fetchval("SELECT 42"))
            try:
                await asyncio.sleep(0.15)
                assert not query.done()
            finally:
                proxies["source"].set_mode("normal")
            assert await asyncio.wait_for(query, 3) == 42
            proxies["source"].set_mode("latency", delay=0.02)
            assert await c.fetchval("SELECT 43") == 43


async def test_tcp_lost_reply_after_subscription_commit(migration):
    original, sc, tc = migration
    async with routed(original, MAJOR) as (service, proxies):
        await sc.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
        await operate(service, "prepare")
        plan = await service.plan("start")
        proxies["target"].lose_reply("CREATE SUBSCRIPTION")
        task = asyncio.create_task(service.start(plan["path"], plan["plan_hash"]))
        try:
            await asyncio.wait_for(proxies["target"].matched.wait(), 20)
            oid = await wait_value(
                tc, "SELECT oid FROM pg_subscription WHERE subname=$1", service.config.subscription
            )
            proxies["target"].set_mode("down")
            with pytest.raises((asyncpg.PostgresConnectionError, ConnectionError)):
                await asyncio.wait_for(task, 10)
        finally:
            proxies["target"].set_mode("normal")
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await service.start(plan["path"], plan["plan_hash"])
        await ready(service)
        assert (
            await tc.fetchval("SELECT oid FROM pg_subscription WHERE subname=$1", service.config.subscription)
            == oid
        )
        assert (await compare(sc, tc))["passed"]
        await operate(service, "abort")


async def test_real_source_outage_offline_abort_kill_and_cleanup(migration):
    original, sc, tc = migration
    async with routed(original, MAJOR) as (service, proxies):
        await sc.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
        await operate(service, "prepare")
        await operate(service, "start")
        await ready(service)
        cli = CLI(service)
        proxies["source"].set_mode("down")
        proxies["replication"].set_mode("down")
        try:
            plan = await cli.plan("abort", "--source-unavailable", "--reason", "isolated TCP outage")
            child = await cli.spawn(
                *cli.arguments("abort", plan),
                fault=dict(side="target", contains="DROP SUBSCRIPTION", phase="after"),
            )
            try:
                await cli.wait_gate(child)
            finally:
                await cli.stop(child, hard=True)
            result = await cli.call(*cli.arguments("abort", plan))
            assert result["state"] == "ABORT_CLEANUP_PENDING"
            assert await sc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)",
                service.config.subscription,
            )
        finally:
            proxies["source"].set_mode("normal")
            proxies["replication"].set_mode("normal")
        assert (await cli.operate("cleanup", plan_args=("--reason", "connectivity restored")))[
            "state"
        ] == "ABORTED"
        assert (await compare(sc, tc))["passed"]
