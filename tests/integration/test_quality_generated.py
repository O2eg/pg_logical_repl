"""Hypothesis generates and shrinks real DML/DDL/recovery operation sequences."""

import asyncio
import json
import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.database import DirectoryBasedExampleDatabase
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule
from support.oracle import compare, quote
from test_lifecycle import ROOT, operate, ready
from test_lifecycle import migration as migration_fixture

from pg_logical_repl import recovery, resync
from pg_logical_repl.service import Service

ARTIFACTS = Path(os.getenv("LC_QUALITY_ARTIFACTS", str(ROOT / ".runtime/quality")))


class ReplicationMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        ARTIFACTS.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.directory = Path(tempfile.mkdtemp(prefix="generated-", dir=ARTIFACTS))
        self.loop = asyncio.Runner()
        self.fixture = migration_fixture.__wrapped__(self.directory)
        self.steps = []
        self.model = {}
        self.column = "n"
        self.extra = False
        self.index = False
        try:
            self.loop.run(self.setup())
        except BaseException:
            self.loop.run(self.fixture.aclose())
            self.loop.close()
            raise

    async def setup(self):
        self.service, self.sc, self.tc = await anext(self.fixture)
        self.service.config.raw["ddl"]["mode"] = "captured"
        self.service.config.raw["validation"] = dict(online=False)
        await self.sc.execute("CREATE TABLE public.app(id int PRIMARY KEY,n int,v text)")
        await operate(self.service, "prepare")
        await operate(self.service, "start")
        await ready(self.service)

    def record(self, operation, **values):
        self.steps.append(dict(operation=operation, **values))
        path = self.directory / "sequence.json"
        path.write_text(json.dumps(self.steps, ensure_ascii=False, indent=2))
        path.chmod(0o600)

    @rule(
        key=st.integers(1, 20),
        value=st.integers(-1000, 1000),
        text=st.text(alphabet="абвxyz'\\\n", max_size=12),
    )
    def upsert(self, key, value, text):
        self.record("upsert", key=key, value=value, text=text)
        self.loop.run(
            self.sc.execute(
                "INSERT INTO app(id,"
                + quote(self.column)
                + ",v) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET "
                + quote(self.column)
                + "=excluded."
                + quote(self.column)
                + ",v=excluded.v",
                key,
                value,
                text,
            )
        )
        self.model[key] = (value, text)

    @rule(key=st.integers(1, 20))
    def delete(self, key):
        self.record("delete", key=key)
        self.loop.run(self.sc.execute("DELETE FROM app WHERE id=$1", key))
        self.model.pop(key, None)

    @rule(rollback=st.booleans())
    def rename_with_dml(self, rollback):
        self.record("rename_with_dml", rollback=rollback)
        other = "amount" if self.column == "n" else "n"

        async def operation():
            tx = self.sc.transaction()
            await tx.start()
            try:
                await self.sc.execute(
                    "UPDATE app SET " + quote(self.column) + "=" + quote(self.column) + "+1"
                )
                await self.sc.execute(
                    "ALTER TABLE public.app RENAME COLUMN " + quote(self.column) + " TO " + quote(other)
                )
                await self.sc.execute("UPDATE app SET " + quote(other) + "=" + quote(other) + "+2")
            except BaseException:
                await tx.rollback()
                raise
            if rollback:
                await tx.rollback()
            else:
                await tx.commit()

        self.loop.run(operation())
        if not rollback:
            self.column = other
            self.model = {key: (value + 3, text) for key, (value, text) in self.model.items()}

    @rule()
    def nullable_column(self):
        self.record("nullable_column", present=not self.extra)
        self.loop.run(
            self.sc.execute(
                "ALTER TABLE public.app " + ("DROP COLUMN extra" if self.extra else "ADD COLUMN extra text")
            )
        )
        self.extra = not self.extra

    @rule()
    def index_queue(self):
        self.record("index_queue", present=not self.index)
        self.loop.run(
            self.sc.execute(
                "DROP INDEX public.generated_index"
                if self.index
                else "CREATE INDEX generated_index ON public.app(" + quote(self.column) + ")"
            )
        )
        self.index = not self.index

    @rule()
    def controller_reconstruction(self):
        self.record("controller_reconstruction")
        self.service = Service(self.service.config)

    @rule()
    def pause_and_resume(self):
        self.record("pause_and_resume")

        async def operation():
            plan = await self.service.plan(
                "pause", extra=dict(reason="generated pause", max_duration=30, wal_rate=1024)
            )
            await recovery.execute(self.service, "pause", plan["path"], plan["plan_hash"])
            plan = await self.service.plan("resume", extra=dict(reason="generated resume"))
            await recovery.execute(self.service, "resume", plan["path"], plan["plan_hash"])

        self.loop.run(operation())

    @rule()
    def resynchronize(self):
        self.record("resync")

        async def operation():
            await self.tc.execute("UPDATE app SET " + quote(self.column) + "=" + quote(self.column) + "+99")
            plan = await self.service.plan(
                "resync-table", extra=dict(table="public.app", reason="generated repair")
            )
            await resync.execute(self.service, plan["path"], plan["plan_hash"])

        self.loop.run(operation())

    @invariant()
    def data_and_schema_match_model(self):
        async def check():
            await self.service.barrier(self.sc, self.tc)
            await self.service.drain_ddl(self.sc, self.tc)
            for c in (self.sc, self.tc):
                observed = {
                    r[0]: (r[1], r[2])
                    for r in await c.fetch("SELECT id," + quote(self.column) + ",v FROM app")
                }
                assert observed == self.model, "Generated workload differs from its independent Python model"
            await compare(self.sc, self.tc, executor="lc_ddl_executor_" + self.service.config.migration_id)

        self.loop.run(check())

    def teardown(self):
        try:
            self.loop.run(self.fixture.aclose())
        finally:
            self.loop.close()


TestGeneratedReplication = ReplicationMachine.TestCase
TestGeneratedReplication.settings = settings(
    max_examples=int(os.getenv("LC_GENERATED_EXAMPLES", "6")),
    stateful_step_count=int(os.getenv("LC_GENERATED_STEPS", "20")),
    deadline=None,
    print_blob=True,
    database=DirectoryBasedExampleDatabase(str(ARTIFACTS / "hypothesis")),
)
TestGeneratedReplication.pytestmark = [
    pytest.mark.integration,
    pytest.mark.quality,
    pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required"),
]


def replay(path):
    """Replay recorded operations through the same independent model and oracle."""
    operations = {
        "upsert": ("upsert", {"key", "value", "text"}),
        "delete": ("delete", {"key"}),
        "rename_with_dml": ("rename_with_dml", {"rollback"}),
        "nullable_column": ("nullable_column", set()),
        "index_queue": ("index_queue", set()),
        "controller_reconstruction": ("controller_reconstruction", set()),
        "pause_and_resume": ("pause_and_resume", set()),
        "resync": ("resynchronize", set()),
    }
    steps = json.loads(Path(path).read_text())
    assert isinstance(steps, list) and steps, "Replay requires a nonempty operation list"
    machine = ReplicationMachine()
    try:
        for step in steps:
            method, keys = operations[step["operation"]]
            assert set(step) - {"operation", "present"} == keys, "Invalid replay arguments"
            getattr(machine, method)(**{key: step[key] for key in keys})
            machine.data_and_schema_match_model()
    finally:
        machine.teardown()


@pytest.mark.integration
@pytest.mark.quality
@pytest.mark.skipif(os.getenv("LC_INTEGRATION") != "1", reason="Owned stand required")
def test_generated_regression_sequence():
    replay(ROOT / "tests/fixtures/quality/recovery.json")


@pytest.mark.integration
@pytest.mark.replay
@pytest.mark.skipif(
    os.getenv("LC_INTEGRATION") != "1" or not os.getenv("LC_REPLAY_SEQUENCE"),
    reason="Explicit owned stand and saved sequence required",
)
def test_replay_saved_sequence():
    replay(os.environ["LC_REPLAY_SEQUENCE"])
