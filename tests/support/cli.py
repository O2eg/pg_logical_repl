"""Real CLI processes; optional test-only fault instrumentation lives in child.py."""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def python():
    return os.environ.get("LC_TEST_PYTHON", sys.executable)


def persist_config(service):
    import yaml

    service.config.path.write_text(yaml.safe_dump(service.config.raw))
    service.config.path.chmod(0o600)


class CLI:
    def __init__(self, service, directory=None):
        self.service = service
        self.directory = directory or service.config.path.parent / "cli"
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.sequence = 0

    async def spawn(self, *args, fault=None):
        self.sequence += 1
        env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "LC_CONFIG"}}
        command = [python(), "-m", "pg_logical_repl"]
        gate = self.directory / f"{self.sequence}.gate.json"
        if fault:
            specification = self.directory / f"{self.sequence}.fault.json"
            specification.write_text(json.dumps(dict(fault, gate=str(gate))))
            specification.chmod(0o600)
            command = [python(), str(Path(__file__).with_name("child.py")), str(specification)]
        command += ["--config", str(self.service.config.path), "--machine", *map(str, args)]
        log = (self.directory / f"{self.sequence}.stderr.log").open("wb")
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=log,
                env=env,
                cwd="/tmp",
                start_new_session=True,
            )
        finally:
            log.close()
        process.gate = gate
        return process

    async def call(self, *args, timeout=120):
        process = await self.spawn(*args)
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout)
        except BaseException:
            await self.stop(process, hard=True)
            raise
        result = json.loads(stdout)
        if process.returncode:
            raise AssertionError(f"CLI {args[0]} failed: {result.get('error')}")
        return result["result"]

    async def plan(self, kind, *args):
        return await self.call("plan", kind, *args)

    def arguments(self, kind, plan, *args):
        command = ["cutover", kind.removeprefix("cutover-")] if kind.startswith("cutover-") else [kind]
        return [*command, "--plan", plan["path"], "--plan-hash", plan["plan_hash"], *args]

    async def operate(self, kind, *args, plan_args=()):
        plan = await self.plan(kind, *plan_args)
        return await self.call(*self.arguments(kind, plan, *args))

    async def wait_state(self, process, expected, *, timeout=90):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if process.returncode is not None:
                raise AssertionError("Daemon exited before expected state: " + str(process.returncode))
            state = self.service.store.read()
            if state["state"] == expected:
                return state
            if state["state"] == "BLOCKED":
                raise AssertionError("Daemon blocked: " + str(state.get("blocked_details")))
            await asyncio.sleep(0.05)
        raise AssertionError("Daemon state timeout: " + expected)

    async def wait_gate(self, process, timeout=90):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if process.gate.exists():
                return json.loads(process.gate.read_text())
            if process.returncode is not None:
                output, _ = await process.communicate()
                raise AssertionError("Child exited without hitting fault: " + output.decode()[-1000:])
            await asyncio.sleep(0.02)
        await self.stop(process, hard=True)
        raise AssertionError("Fault boundary was never reached")

    @staticmethod
    async def stop(process, *, hard=False):
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL if hard else signal.SIGTERM)
        try:
            await asyncio.wait_for(process.communicate(), 10)
        except TimeoutError:
            os.killpg(process.pid, signal.SIGKILL)
            await process.communicate()
