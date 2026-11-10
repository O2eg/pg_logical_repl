"""Deterministic SIGKILL/SIGTERM at durable lifecycle boundaries."""

import os

import pytest
from support.cli import CLI, persist_config
from support.oracle import compare
from test_lifecycle import migration as migration_fixture
from test_lifecycle import operate, ready
from test_p0_recovery import unavailable
from test_quality_oracle import fence

from pg_logical_repl import emergency

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.quality,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]

CASES = [
    ("prepare", dict(side="target", contains="INSERT INTO lc.control", phase="before")),
    ("prepare", dict(side="target", contains="INSERT INTO lc.control", phase="after_commit")),
    ("prepare", dict(restore="schema", phase="after", label="committed schema restore")),
    ("start", dict(state="STARTING", phase="after")),
    ("start", dict(side="target", contains="CREATE SUBSCRIPTION", phase="after")),
    ("resync-table", dict(side="target", contains="TRUNCATE ONLY", phase="before")),
    ("resync-table", dict(side="target", contains="TRUNCATE ONLY", phase="after")),
    (
        "resync-table",
        dict(
            side="target",
            contains="INSERT INTO lc.operations(name) VALUES",
            arg_contains=":truncated",
            phase="after_commit",
        ),
    ),
    ("cutover-finalize", dict(state="FINALIZING", phase="after")),
    ("cutover-finalize", dict(state="CUTOVER_READY", phase="before")),
    ("cutover-commit", dict(state="COMMITTING", phase="after")),
    ("cutover-commit", dict(side="target", contains="DROP SUBSCRIPTION", phase="after")),
    ("abort", dict(state="ABORTING", phase="after")),
    ("abort", dict(side="target", contains="DROP SUBSCRIPTION", phase="after")),
    ("cleanup", dict(side="source", contains="SELECT pg_drop_replication_slot", phase="after")),
]


@pytest.mark.parametrize(
    "kind,fault", CASES, ids=[f"{kind}-{i}-{fault['phase']}" for i, (kind, fault) in enumerate(CASES)]
)
async def test_process_death_and_original_plan_retry(migration, monkeypatch, kind, fault):
    service, sc, tc = migration
    await sc.execute(
        "CREATE TABLE app(id int PRIMARY KEY,n int); INSERT INTO app VALUES(1,10),(2,20); CREATE TABLE child(id int PRIMARY KEY,p int REFERENCES app(id)); INSERT INTO child VALUES(1,1)"
    )
    persist_config(service)
    cli = CLI(service)
    if kind != "prepare":
        await operate(service, "prepare")
    if kind not in {"prepare", "start"}:
        await operate(service, "start")
        await ready(service)
    extra, arguments = (), ()
    if kind == "resync-table":
        await tc.execute("UPDATE app SET n=-1 WHERE id=1")
        extra = ("--table", "public.app", "--reason", "repair owned fixture")
    if kind in {"cutover-finalize", "cutover-commit"}:
        evidence = str(fence(service))
        if kind == "cutover-commit":
            await operate(service, "cutover-finalize", write_fence=evidence)
        else:
            arguments = ("--write-fence", evidence)
    if kind == "cleanup":
        with monkeypatch.context() as patch:
            patch.setattr(service.source, "connect", unavailable())
            plan = await service.plan(
                "abort", extra=dict(source_unavailable=True, reason="owned offline preparation")
            )
            await emergency.execute(service, "abort", plan["path"], plan["plan_hash"])
        extra = ("--reason", "source restored")
    plan = await cli.plan(kind, *extra)
    process = await cli.spawn(*cli.arguments(kind, plan, *arguments), fault=fault)
    try:
        await cli.wait_gate(process)
        await cli.stop(process, hard=True)
    finally:
        await cli.stop(process, hard=True)
    assert process.returncode < 0
    # The process has gone away. PostgreSQL must roll back uncommitted work,
    # release its locks, and the original reviewed plan must remain usable.
    await cli.call(*cli.arguments(kind, plan, *arguments))
    if kind == "prepare":
        await cli.operate("start")
    if kind in {"prepare", "start"}:
        await ready(service)
    assert (await compare(sc, tc))["passed"]
    if kind in {"abort", "cleanup", "cutover-commit"}:
        assert not await sc.fetchval(
            "SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1)", service.config.subscription
        )


async def test_cli_sigterm_and_second_controller_exclusion(migration):
    service, sc, tc = migration
    await sc.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
    persist_config(service)
    cli = CLI(service)
    plan = await cli.plan("prepare")
    first = await cli.spawn(*cli.arguments("prepare", plan), fault=dict(state="PREPARING", phase="after"))
    try:
        await cli.wait_gate(first)
        second = await cli.spawn(*cli.arguments("prepare", plan))
        output, _ = await second.communicate()
        assert second.returncode == 8 and b"OWNERSHIP_ERROR" in output
        await cli.stop(first)
        assert first.returncode == 143
    finally:
        await cli.stop(first, hard=True)
    await cli.call(*cli.arguments("prepare", plan))
    await cli.operate("start")
    await ready(service)
    assert (await compare(sc, tc))["passed"]
