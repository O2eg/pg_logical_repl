"""Instrumentation for killed CLI subprocesses; never installed in the wheel."""

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from pg_logical_repl import schema
from pg_logical_repl.cli import main
from pg_logical_repl.db import Database
from pg_logical_repl.service import Service

spec = json.loads(Path(sys.argv[1]).read_text())
fired = False


async def gate():
    global fired
    if fired:
        return
    fired = True
    path = Path(spec["gate"])
    path.write_text(
        json.dumps(
            dict(boundary=spec.get("label", spec.get("contains", spec.get("state"))), phase=spec["phase"])
        )
    )
    path.chmod(0o600)
    await asyncio.Event().wait()


def matches(sql, args):
    return spec.get("contains", "\x00") in sql and (
        not spec.get("arg_contains") or any(spec["arg_contains"] in str(arg) for arg in args)
    )


class Connection:
    def __init__(self, real):
        self.real, self.pending = real, False

    def __getattr__(self, key):
        return getattr(self.real, key)

    async def invoke(self, method, sql, *args, **kwargs):
        match = matches(sql, args)
        if match and spec["phase"] == "before":
            await gate()
        value = await getattr(self.real, method)(sql, *args, **kwargs)
        if match:
            if spec["phase"] == "after_commit" and self.real.is_in_transaction():
                self.pending = True
            else:
                await gate()
        return value

    async def execute(self, sql, *args, **kwargs):
        return await self.invoke("execute", sql, *args, **kwargs)

    async def fetchval(self, sql, *args, **kwargs):
        return await self.invoke("fetchval", sql, *args, **kwargs)

    def transaction(self, *args, **kwargs):
        real, connection = self.real.transaction(*args, **kwargs), self

        class Transaction:
            async def __aenter__(self):
                return await real.__aenter__()

            async def __aexit__(self, typ, value, tb):
                result = await real.__aexit__(typ, value, tb)
                if typ is None and connection.pending and not connection.real.is_in_transaction():
                    connection.pending = False
                    await gate()
                return result

        return Transaction()


connect = Database.connect


@asynccontextmanager
async def instrument(self, **kwargs):
    async with connect(self, **kwargs) as c:
        yield Connection(c) if self.side == spec.get("side") else c


Database.connect = instrument
state = Service.state


async def state_boundary(self, name, *args, **kwargs):
    match = name == spec.get("state")
    if match and spec["phase"] == "before":
        await gate()
    value = await state(self, name, *args, **kwargs)
    if match and spec["phase"] == "after":
        await gate()
    return value


Service.state = state_boundary
restore = schema.restore


async def restore_boundary(self, major, phase):
    value = await restore(self, major, phase)
    if phase == spec.get("restore"):
        await gate()
    return value


schema.restore = restore_boundary
sys.exit(main(sys.argv[2:]))
