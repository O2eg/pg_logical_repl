"""Bounded PostgreSQL client processes; credentials never enter argv."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

from .errors import ConnectorError, PreconditionError
from .state import write_private


@contextmanager
def pg_environment(db, directory: Path):
    values, pw = db.libpq()
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    mapping = {
        "host": "PGHOST",
        "hostaddr": "PGHOSTADDR",
        "port": "PGPORT",
        "dbname": "PGDATABASE",
        "user": "PGUSER",
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
        "connect_timeout": "PGCONNECT_TIMEOUT",
        "application_name": "PGAPPNAME",
    }
    env.update({mapping[k]: str(v) for k, v in values.items() if k in mapping})
    lock_ms = 1000 * db.config.get("limits", "lock_timeout_seconds", 5)
    env["PGOPTIONS"] = f"-cstatement_timeout=0 -clock_timeout={lock_ms}"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = None
    if pw is not None:
        fd, filename = tempfile.mkstemp(prefix="pgpass-", dir=directory)
        os.close(fd)
        path = Path(filename)

        def escape(value):
            return str(value).replace("\\", "\\\\").replace(":", "\\:")

        lines = [
            ":".join(escape(x) for x in (host, values["port"], values["dbname"], values["user"], pw))
            for host in values["host"].split(",")
        ]
        write_private(path, "\n".join(lines) + "\n")
        env["PGPASSFILE"] = str(path)
    try:
        yield env
    finally:
        if path:
            path.unlink(missing_ok=True)


def tool(config, major, name):
    directory = config.get("client_tools", "bin_dirs", {}).get(str(major))
    if directory:
        path = config.path.parent / directory / name
    else:
        path = Path(f"/usr/lib/postgresql/{major}/bin/{name}")
        if not path.exists():
            path = Path(shutil.which(name) or "/nonexistent/" + name)
    if not path.is_file():
        raise PreconditionError("CLIENT_TOOL_MISSING", f"{name} for major {major} is missing")
    version = subprocess.run(
        [str(path), "--version"], capture_output=True, text=True, timeout=5, check=True
    ).stdout
    match = re.search(r"\(PostgreSQL\) (\d+)", version)
    if not match or int(match[1]) != major:
        raise PreconditionError("CLIENT_TOOL_VERSION", f"{name} must have major version {major}")
    return str(path)


async def execute(argv, *, env=None, stdin=None, stdout_path=None, timeout=3600, redactor):
    out = None
    if stdout_path:
        fd = os.open(stdout_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        out = os.fdopen(fd, "wb")
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=out or asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            start_new_session=True,
            umask=0o077,
        )
    except BaseException:
        if out:
            out.close()
        raise

    stdout_overflow = False

    async def read_bounded(stream, limit=1048576, strict=False):
        nonlocal stdout_overflow
        content = bytearray()
        while chunk := await stream.read(65536):
            content.extend(chunk)
            if len(content) > limit:
                if strict:
                    stdout_overflow = True
                del content[:-limit]
        return bytes(content)

    async def communicate():
        if stdin is not None:
            proc.stdin.write(stdin.encode() if isinstance(stdin, str) else stdin)
            await proc.stdin.drain()
            proc.stdin.close()
        tasks = [read_bounded(proc.stderr)]
        if proc.stdout:
            tasks.append(read_bounded(proc.stdout, strict=True))
        result = await asyncio.gather(*tasks)
        await proc.wait()
        return result

    try:
        result = await asyncio.wait_for(communicate(), timeout)
        if stdout_overflow:
            raise ConnectorError(
                "CLIENT_OUTPUT_LIMIT", "Client stdout exceeded the capture limit; refusing truncated metadata"
            )
        if proc.returncode:
            raise ConnectorError(
                "CLIENT_TOOL_ERROR",
                redactor.text(result[0].decode(errors="replace")),
                recovery_hint="Inspect postconditions before retrying a mutation",
            )
        return result[1].decode(errors="replace") if len(result) > 1 else ""
    except (asyncio.CancelledError, TimeoutError):
        if proc.returncode is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except TimeoutError:
                os.killpg(proc.pid, signal.SIGKILL)
                await proc.wait()
        raise
    finally:
        if out:
            out.close()
