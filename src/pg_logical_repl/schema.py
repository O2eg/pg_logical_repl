"""Matching-major dump/restore, with transactional restore receipts."""

import re

from .process import execute, pg_environment, tool
from .security import literal
from .state import write_private


async def dump_schema(service, major):
    archive = service.store.directory / "schema.dump"
    with pg_environment(service.source, service.store.directory) as env:
        await execute(
            [
                tool(service.config, major, "pg_dump"),
                "--format=custom",
                "--schema-only",
                "--exclude-schema=lc",
                "--no-publications",
                "--no-subscriptions",
                "--file=" + str(archive),
            ],
            env=env,
            redactor=service.redactor,
        )
    archive.chmod(0o600)
    toc = await execute(
        [tool(service.config, major, "pg_restore"), "--list", str(archive)], redactor=service.redactor
    )
    # pg_dump emits public's ACL/owner/comment separately; preserve those entries.
    lines = [
        line
        for line in toc.splitlines()
        if not re.search(r"; \d+ \d+ SCHEMA - public ", line)
        and not re.search(r"; \d+ \d+ EVENT TRIGGER - lc_(guard|capture|rewrite_guard) ", line)
    ]
    pre = []
    post = []
    # Keep RI and unique indexes available before tablesync. Other post-data are restored later.
    for line in lines:
        if re.search(r"; \d+ \d+ (INDEX|CONSTRAINT|INDEX ATTACH) ", line):
            pre.append(line)
        else:
            post.append(line)
    write_private(service.store.directory / "schema-all.list", "\n".join(lines) + "\n")
    write_private(service.store.directory / "schema-indexes.list", "\n".join(pre) + "\n")
    write_private(service.store.directory / "schema-post.list", "\n".join(post) + "\n")


async def restore(service, major, phase):
    files = []
    requests = (
        [("pre-data", "schema-all.list"), ("post-data", "schema-indexes.list")]
        if phase == "schema"
        else [("post-data", "schema-post.list")]
    )
    for i, (section, listing) in enumerate(requests):
        output = service.store.directory / f"{phase}-{i}.sql"
        await execute(
            [
                tool(service.config, major, "pg_restore"),
                "--file=-",
                "--section=" + section,
                "--use-list=" + str(service.store.directory / listing),
                str(service.store.directory / "schema.dump"),
            ],
            stdout_path=output,
            redactor=service.redactor,
        )
        files.extend(["--file", str(output)])
    receipt = "INSERT INTO lc.operations(name) VALUES (" + literal(phase) + ")"
    with pg_environment(service.target, service.store.directory) as env:
        await execute(
            [
                tool(service.config, major, "psql"),
                "-X",
                "-w",
                "--quiet",
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                *files,
                "--command",
                receipt,
            ],
            env=env,
            redactor=service.redactor,
            timeout=service.config.get("limits", "operation_timeout_seconds", 3600),
        )
