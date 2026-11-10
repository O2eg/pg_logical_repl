"""Optional cross-project CLI qualification using owned, randomly named databases."""

import asyncio
import json
import os
import signal
from pathlib import Path
from urllib.parse import quote

import pytest
import yaml
from support.network import routed
from test_lifecycle import MAJOR, ready
from test_lifecycle import migration as migration_fixture

from pg_logical_repl.config import load
from pg_logical_repl.service import Service
from pg_logical_repl.state import now, write_private

pytest.importorskip("pg_play")
from pg_play.logical_runs import LogicalRunManager

migration = migration_fixture
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


async def wait(manager, directory, *, terminal=True, timeout=180):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        state = await asyncio.to_thread(manager.status, directory)
        if terminal and state["state"] not in {"queued", "running"}:
            return state
        if not terminal and state.get("migration_state") == "REPLICATING":
            return state
        await asyncio.sleep(0.1)
    raise AssertionError("pg_play worker timed out")


async def operation(manager, config, root, name, options=None):
    plan = await asyncio.to_thread(manager.plan, str(config), name, options=options)
    state = await asyncio.to_thread(manager.start, plan, plan["plan_hash"], root, name)
    result = await wait(manager, state["directory"])
    assert result["state"] == "succeeded", result
    return result


@pytest.mark.parametrize("finish", ["abort", "cutover"])
async def test_pg_play_experiment_ddl_diagnostics_oracle_abort(migration, finish):
    service, source, _ = migration
    await source.execute("CREATE TABLE public.play_test(id int PRIMARY KEY, value text)")
    await source.execute(
        "INSERT INTO public.play_test SELECT n,repeat('payload',20) FROM generate_series(1,100) n"
    )
    config = service.config.path
    service.config.raw["ddl"] = {"mode": "managed_only"}
    config.write_text(yaml.safe_dump(service.config.raw))
    sql = config.parent / "ddl.sql"
    sql.write_text("ALTER TABLE public.play_test ADD COLUMN extra integer DEFAULT 7")
    from pg_logical_repl.db import identity

    fence = config.parent / "fence.json"
    write_private(
        fence,
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=await identity(source),
                created_at=now(),
                responsible="Owned integration fixture",
                external_fence="Only this fixture can write; all fixture writers stopped",
            )
        ),
    )
    scenario = config.parent / "scenario.yaml"
    scenario.write_text(
        yaml.safe_dump(
            dict(
                schema_version="pg_play/logical-experiment-v1",
                ddl_files=[str(sql)],
                diagnostics=True,
                oracle=True,
                quiescent_at_verification=True,
                finish=finish,
                write_fence=str(fence),
                readiness_timeout_seconds=90,
            )
        )
    )
    manager = LogicalRunManager()
    plan = await asyncio.to_thread(
        manager.plan,
        str(config),
        "experiment",
        scenario=str(scenario),
        duration_seconds=5,
        poll_interval_seconds=0.2,
    )
    started = await asyncio.to_thread(
        manager.start, plan, plan["plan_hash"], config.parent / "runs", "experiment"
    )
    result = await wait(manager, started["directory"])
    assert result["state"] == "succeeded", result
    report = manager.report(started["directory"])
    assert report["result"]["oracle"]["passed"]
    assert report["result"]["diagnostics"]["source"]["passed"]
    assert report["result"]["diagnostics"]["target"]["passed"]
    assert report["migration_state"] == ("ABORTED" if finish == "abort" else "DETACHED")


@pytest.mark.parametrize(
    "stop_signal, outcome", [(signal.SIGKILL, "interrupted"), (signal.SIGTERM, "cancelled")]
)
async def test_pg_play_controller_sigkill_resume_blocked_recovery(migration, stop_signal, outcome):
    service, source, target = migration
    await source.execute("CREATE TABLE public.play_test(id int PRIMARY KEY, value text)")
    await source.execute("INSERT INTO public.play_test VALUES(1,'original')")
    config = service.config.path
    service.config.raw["daemon"] = {"drift_interval_seconds": 0}
    config.write_text(yaml.safe_dump(service.config.raw))
    service = Service(load(config))
    manager = LogicalRunManager()
    root = config.parent / "runs"
    await operation(manager, config, root, "prepare")
    await operation(manager, config, root, "start")
    plan = await asyncio.to_thread(
        manager.plan, str(config), "controller", duration_seconds=30, poll_interval_seconds=0.2
    )
    started = await asyncio.to_thread(manager.start, plan, plan["plan_hash"], root, "controller")
    state = await wait(manager, started["directory"], terminal=False)
    os.kill(state["worker"]["pid"], stop_signal)
    assert (await wait(manager, started["directory"]))["state"] == outcome
    await source.execute("INSERT INTO public.play_test VALUES(2,'after-controller-loss')")
    deadline = asyncio.get_running_loop().time() + 15
    while not await target.fetchval("SELECT EXISTS(SELECT FROM public.play_test WHERE id=2)"):
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.1)
    await asyncio.to_thread(manager.resume, started["directory"], plan["plan_hash"])
    await wait(manager, started["directory"], terminal=False)
    await target.execute("UPDATE public.play_test SET value='damaged' WHERE id=1")
    blocked = await wait(manager, started["directory"])
    assert blocked["state"] == "blocked", blocked
    await target.execute("UPDATE public.play_test SET value='original' WHERE id=1")
    await operation(manager, config, root, "resume", {"reason": "Repaired target data"})
    await asyncio.to_thread(manager.resume, started["directory"], plan["plan_hash"])
    await wait(manager, started["directory"], terminal=False)
    await asyncio.to_thread(manager.stop, started["directory"], "Recovery verified")
    assert (await wait(manager, started["directory"]))["state"] == "cancelled"
    await operation(manager, config, root, "abort", {"reason": "Integration test complete"})
    assert service.store.read()["state"] == "ABORTED"
    assert (Path(started["directory"]) / "events.jsonl").is_file()


@pytest.mark.parametrize("stage", ["prepare", "start", "ddl-0"])
async def test_pg_play_reconciles_lost_stage_result(migration, monkeypatch, stage):
    from pg_play import logical_experiment

    service, source, target = migration
    await source.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
    config = service.config.path
    service.config.raw["ddl"] = {"mode": "managed_only"}
    config.write_text(yaml.safe_dump(service.config.raw))
    sql = config.parent / "ddl.sql"
    sql.write_text("ALTER TABLE public.app ADD COLUMN extra int DEFAULT 7")
    scenario = config.parent / "scenario.yaml"
    scenario.write_text(
        yaml.safe_dump(
            dict(
                schema_version="pg_play/logical-experiment-v1",
                ddl_files=[str(sql)],
                diagnostics=False,
                oracle=True,
                quiescent_at_verification=True,
                finish="abort",
            )
        )
    )
    manager = LogicalRunManager()
    monkeypatch.setattr(manager, "_spawn", lambda directory, state: state)
    plan = await asyncio.to_thread(
        manager.plan,
        str(config),
        "experiment",
        scenario=str(scenario),
        duration_seconds=1,
        poll_interval_seconds=0.1,
    )
    started = await asyncio.to_thread(
        manager.start, plan, plan["plan_hash"], config.parent / "runs", "receipt"
    )
    directory = Path(started["directory"])
    original = logical_experiment.durable_json

    def lose_result(path, value):
        if path.name == stage + ".result.json":
            raise OSError("Injected loss after native operation completed")
        return original(path, value)

    monkeypatch.setattr(logical_experiment, "durable_json", lose_result)
    assert (await asyncio.to_thread(manager.execute, directory))["state"] == "failed"
    monkeypatch.setattr(logical_experiment, "durable_json", original)
    await asyncio.to_thread(manager.resume, directory, plan["plan_hash"])
    assert (await asyncio.to_thread(manager.execute, directory))["state"] == "succeeded"
    intent = json.loads((directory / "steps" / (stage + ".intent.json")).read_text())
    assert (
        sum(
            row["event"] == "operation_started"
            and row["details"].get("plan_hash") == intent["native"]["plan_hash"]
            for row in service.store.events(limit=10000)
        )
        == 1
    )
    assert await target.fetchval("SELECT extra FROM app") == 7
    assert service.store.read()["state"] == "ABORTED"


async def test_pg_play_final_oracle_failure_reports_cutover_state(migration, monkeypatch):
    from pg_play import logical_evidence

    from pg_logical_repl.db import identity

    service, source, _ = migration
    await source.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
    base = service.config.path.parent
    fence = base / "fence.json"
    write_private(
        fence,
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=await identity(source),
                created_at=now(),
                responsible="Owned integration fixture",
                external_fence="All fixture writers stopped; no external writers exist",
            )
        ),
    )
    scenario = base / "scenario.yaml"
    scenario.write_text(
        yaml.safe_dump(
            dict(
                schema_version="pg_play/logical-experiment-v1",
                diagnostics=False,
                oracle=True,
                quiescent_at_verification=True,
                finish="cutover",
                write_fence=str(fence),
            )
        )
    )
    manager = LogicalRunManager()
    monkeypatch.setattr(manager, "_spawn", lambda directory, state: state)
    original = logical_evidence.exact_oracle
    monkeypatch.setattr(
        logical_evidence,
        "exact_oracle",
        lambda path, deferred=False: dict(passed=False) if deferred else original(path),
    )
    plan = await asyncio.to_thread(
        manager.plan,
        str(service.config.path),
        "experiment",
        scenario=str(scenario),
        duration_seconds=1,
        poll_interval_seconds=0.1,
    )
    started = await asyncio.to_thread(manager.start, plan, plan["plan_hash"], base / "runs", "oracle")
    result = await asyncio.to_thread(manager.execute, started["directory"])
    assert result["state"] == "failed"
    assert service.store.read()["state"] == result["migration_state"] == "CUTOVER_READY"
    assert manager.report(started["directory"])["migration_state"] == "CUTOVER_READY"


async def test_pg_play_source_outage_abort_and_cleanup(migration):
    original, source, target = migration
    await source.execute("CREATE TABLE app(id int PRIMARY KEY); INSERT INTO app VALUES(1)")
    async with routed(original, MAJOR) as (service, proxies):
        manager = LogicalRunManager()
        root = service.config.path.parent / "runs"
        await operation(manager, service.config.path, root, "prepare")
        await operation(manager, service.config.path, root, "start")
        await ready(service)
        proxies["source"].set_mode("down")
        proxies["replication"].set_mode("down")
        try:
            plan = await asyncio.to_thread(
                manager.plan,
                str(service.config.path),
                "abort",
                options={"source_unavailable": True, "reason": "TCP outage"},
            )
            started = await asyncio.to_thread(manager.start, plan, plan["plan_hash"], root, "abort")
            result = await wait(manager, started["directory"])
            assert result["state"] == "partial", result
            assert result["migration_state"] == "ABORT_CLEANUP_PENDING"
            assert not await target.fetchval(
                "SELECT EXISTS(SELECT FROM pg_subscription WHERE subname=$1)", service.config.subscription
            )
        finally:
            proxies["source"].set_mode("normal")
            proxies["replication"].set_mode("normal")
        await operation(manager, service.config.path, root, "cleanup", {"reason": "Source restored"})
        assert service.store.read()["state"] == "ABORTED"


async def test_pg_play_cutover_and_independent_target(migration):
    service, source, target = migration
    await source.execute("CREATE TABLE app(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text)")
    await source.execute("INSERT INTO app(value) VALUES('before cutover')")
    manager = LogicalRunManager()
    root = service.config.path.parent / "runs"
    await operation(manager, service.config.path, root, "prepare")
    await operation(manager, service.config.path, root, "start")
    await ready(service)
    fence = service.config.path.parent / "fence.json"
    write_private(
        fence,
        json.dumps(
            dict(
                migration_id=service.config.migration_id,
                source_identity=service.store.read()["source_identity"],
                created_at=now(),
                responsible="Owned integration fixture",
                external_fence="All fixture writers stopped; no external writers exist",
            )
        ),
    )
    await operation(manager, service.config.path, root, "cutover-finalize", {"write_fence": str(fence)})
    from pg_play.logical_evidence import exact_oracle

    assert (await asyncio.to_thread(exact_oracle, str(service.config.path), deferred=True))["passed"]
    await operation(manager, service.config.path, root, "cutover-commit")
    assert service.store.read()["state"] == "DETACHED"
    assert await target.fetchval("INSERT INTO app(value) VALUES('independent') RETURNING id") == 2
    assert await source.fetchval("SELECT count(*) FROM app") == 1


@pytest.mark.parametrize(
    "disk_limit, outcome, interrupt",
    [(1, "partial", False), (95, "succeeded", False), (95, "succeeded", True)],
    ids=["guarded", "normal", "interrupted"],
)
async def test_pg_play_workload_and_converter_compatibility(migration, disk_limit, outcome, interrupt):
    from pg_play.converter_runs import plan_converter_run
    from pg_play.logical import LogicalError
    from pg_play.runner import ComponentRunner
    from test_lifecycle import ROOT, SOURCE_PORT

    service, source, target = migration
    await source.execute("CREATE TABLE play_test(id int PRIMARY KEY, n int NOT NULL)")
    await source.execute("INSERT INTO play_test VALUES(1,0)")
    base = service.config.path.parent
    password = (base / "password").read_text().strip()
    project = base / "converter"
    for name, sql in (
        ("initialize", "SELECT 1"),
        ("change", "ALTER TABLE public.play_test ADD COLUMN release_number int DEFAULT 9"),
    ):
        packet = project / "packets" / name
        packet.mkdir(parents=True)
        (packet / "meta_data.json").write_text('{"type":"default"}')
        (packet / "01.sql").write_text(sql)
    converter_config = project / "config.ini"
    write_private(
        converter_config,
        "[databases]\nsource=postgresql://postgres:"
        + quote(password, safe="")
        + f"@127.0.0.1:{SOURCE_PORT}/{service.config.migration_id}?sslmode=disable\n"
        "[main]\nschema_location=pgc\n",
    )
    runner = ComponentRunner()
    packet_plan = await asyncio.to_thread(
        plan_converter_run,
        runner,
        project_directory=project,
        config_file=str(converter_config),
        packet_name="change",
    )
    packet_path = base / "packet-plan.json"
    write_private(packet_path, json.dumps(packet_plan))
    workload_root = base / "workload"
    profile = workload_root / "data" / "logical"
    profile.mkdir(parents=True)
    (profile / "update.sql").write_text("UPDATE public.play_test SET n=n+1 WHERE id=1;")
    (profile / "profile.yml").write_text(
        yaml.safe_dump(
            dict(
                api_version="pg_workload/v1",
                name="logical",
                schema="public",
                requires_write=True,
                prepare={"steps": []},
                jobs=[
                    dict(
                        name="writer",
                        type="pgbench",
                        interval=1,
                        scripts=["update.sql"],
                        clients=1,
                        threads=1,
                        transactions=1,
                        no_vacuum=True,
                    )
                ],
            )
        )
    )
    artifact_root = base / "workload-artifacts"
    passfile = artifact_root / "credentials" / "pgpass"
    passfile.parent.mkdir(parents=True)
    escaped = password.replace("\\", "\\\\").replace(":", "\\:")
    write_private(passfile, f"127.0.0.1:{SOURCE_PORT}:*:postgres:{escaped}\n")
    manifest = base / "workload.yaml"
    manifest.write_text(
        yaml.safe_dump(
            dict(
                api_version="pg_play/v1",
                kind="PostgreSQLExperiment",
                metadata={"id": "logical-integration"},
                spec=dict(
                    artifact_root=str(artifact_root),
                    stand=dict(config=str(ROOT / f"infra/pg{MAJOR}.yaml"), project=str(ROOT)),
                    workload=dict(
                        project=str(workload_root),
                        profiles=["logical"],
                        database=service.config.migration_id,
                        user="postgres",
                        install=False,
                        job_interval_seconds=1,
                        resource_guard={"disk_max_used_pct": disk_limit},
                    ),
                    diagnostics=dict(duration_seconds=30, interval_seconds=5),
                ),
            )
        )
    )
    service.config.raw["ddl"] = {"mode": "captured"}
    service.config.path.write_text(yaml.safe_dump(service.config.raw))
    scenario = base / "scenario.yaml"
    scenario.write_text(
        yaml.safe_dump(
            dict(
                schema_version="pg_play/logical-experiment-v1",
                converter_plans=[str(packet_path)],
                workload_manifest=str(manifest),
                diagnostics=False,
                oracle=True,
                quiescent_at_verification=True,
                finish="abort",
                readiness_timeout_seconds=60,
            )
        )
    )
    manager = LogicalRunManager()
    with pytest.raises(LogicalError, match="tracker.*compound DDL"):
        await asyncio.to_thread(
            manager.plan, str(service.config.path), "experiment", scenario=str(scenario), duration_seconds=5
        )
    raw = yaml.safe_load(scenario.read_text())
    raw["converter_plans"] = []
    raw["ddl_files"] = [str(project / "packets/change/01.sql")]
    scenario.write_text(yaml.safe_dump(raw))
    plan = await asyncio.to_thread(
        manager.plan,
        str(service.config.path),
        "experiment",
        scenario=str(scenario),
        duration_seconds=5,
        poll_interval_seconds=0.2,
    )
    started = await asyncio.to_thread(manager.start, plan, plan["plan_hash"], base / "runs", "scenario")
    if interrupt:
        deadline = asyncio.get_running_loop().time() + 30
        while True:
            state = await asyncio.to_thread(manager.status, started["directory"])
            if state.get("workload_owned"):
                break
            assert state["state"] in {"queued", "running"}, state
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        os.kill(state["worker"]["pid"], signal.SIGKILL)
        interrupted = await wait(manager, started["directory"])
        assert interrupted["state"] == "interrupted", interrupted
        assert not interrupted["workload_owned"] and not interrupted["cleanup_pending"]
        await asyncio.to_thread(manager.resume, started["directory"], plan["plan_hash"])
    result = await wait(manager, started["directory"])
    assert result["state"] == outcome, result
    assert await target.fetchval("SELECT release_number FROM play_test") == 9
    evidence = result["result"]["workload"]
    assert evidence["resource_guard_triggered"] is (disk_limit == 1)
    assert evidence["passed"] is (disk_limit != 1)
    if outcome == "succeeded":
        assert await source.fetchval("SELECT n FROM play_test") > 0
    else:
        assert await source.fetchval("SELECT n FROM play_test") == 0
