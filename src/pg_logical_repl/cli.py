"""Thin CLI adapter; exactly one result envelope in machine mode."""

import argparse
import asyncio
import json
import os
import signal
import sys
import uuid
from pathlib import Path

from . import __version__, component, ddl, ddl_queue, logging_config, recovery
from .capabilities import describe
from .config import load
from .errors import ConfigurationError, ConnectorError
from .plans import KINDS
from .security import Redactor
from .service import Service


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ConfigurationError(message)


def parser():
    p = Parser(prog="pg-logical-repl")
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--capabilities", action="store_true")
    p.add_argument("--component-capabilities", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--contract", choices=[component.CONTRACT], help=argparse.SUPPRESS)
    p.add_argument("--config", default=os.getenv("LC_CONFIG"))
    p.add_argument("--machine", action="store_true")
    p.add_argument("--request-id")
    p.add_argument("--log-level", choices=["debug", "info", "warning"], default="info")
    p.add_argument("--log-file")
    commands = p.add_subparsers(dest="command")
    for name in ("validate", "inspect-config", "check", "status", "report"):
        commands.add_parser(name)
    verify = commands.add_parser("verify")
    verify.add_argument("--table")
    verify.add_argument("--level", choices=["counts", "checksums", "full"], default="checksums")
    plan = commands.add_parser("plan")
    plan.add_argument("kind", choices=sorted(KINDS))
    plan.add_argument("--out")
    plan.add_argument("--sql-file")
    plan.add_argument("--job-id", type=int)
    plan.add_argument("--evidence")
    plan.add_argument("--reason")
    plan.add_argument("--action", choices=["skip", "applied-manually"])
    plan.add_argument("--lsn")
    plan.add_argument("--max-duration", type=int)
    plan.add_argument("--wal-rate", type=int)
    plan.add_argument("--source-unavailable", action="store_true")
    plan.add_argument("--table")
    for name in ("prepare", "start", "abort", "pause", "resume", "resolve", "cleanup", "resync-table"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--plan", required=True)
        cmd.add_argument("--plan-hash", required=True)
    run = commands.add_parser("run")
    run.add_argument("--once", action="store_true")
    events = commands.add_parser("events")
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    cutover = commands.add_parser("cutover").add_subparsers(dest="cutover_action", required=True)
    for name in ("finalize", "commit"):
        cmd = cutover.add_parser(name)
        cmd.add_argument("--plan", required=True)
        cmd.add_argument("--plan-hash", required=True)
        if name == "finalize":
            cmd.add_argument("--write-fence", required=True)
    abort = cutover.add_parser("abort")
    abort.add_argument("--reason", required=True)
    ddls = commands.add_parser("ddl").add_subparsers(dest="ddl_action", required=True)
    dc = ddls.add_parser("check")
    dc.add_argument("file")
    de = ddls.add_parser("exec")
    de.add_argument("--sql-file", required=True)
    de.add_argument("--plan", required=True)
    de.add_argument("--plan-hash", required=True)
    ddls.add_parser("log")
    queue = ddls.add_parser("queue").add_subparsers(dest="queue_action", required=True)
    queue.add_parser("list")
    qr = queue.add_parser("retry")
    qr.add_argument("id", type=int)
    qs = queue.add_parser("skip")
    qs.add_argument("id", type=int)
    qs.add_argument("--evidence", required=True)
    qs.add_argument("--plan", required=True)
    qs.add_argument("--plan-hash", required=True)
    return p


async def dispatch(args, service):
    if args.command == "inspect-config":
        return dict(
            config_hash=service.config.hash,
            migration_id=service.config.migration_id,
            state_dir=str(service.config.state_dir),
            ddl_mode=service.config.ddl_mode,
            endpoints={
                side: {
                    key: service.config.endpoint(side)[key]
                    for key in ("host", "port", "database", "user", "sslmode")
                }
                for side in ("source", "target")
            },
        )
    if args.command == "validate":
        return dict(valid=True, config_hash=service.config.hash)
    if args.command == "verify":
        return await service.verify(level=args.level, table=args.table)
    if args.command in {"check", "status"}:
        return await getattr(service, args.command)()
    if args.command == "cutover":
        if args.cutover_action == "abort":
            return await service.cutover_abort(args.reason)
        kw = {"write_fence": args.write_fence} if args.cutover_action == "finalize" else {}
        return await getattr(service, "cutover_" + args.cutover_action)(args.plan, args.plan_hash, **kw)
    if args.command == "report":
        return dict(status=await service.status(), events=service.store.events())
    if args.command == "plan":
        extra = ddl.classify(Path(args.sql_file).read_text()) if args.sql_file else None
        if args.kind == "ddl-queue-skip":
            if args.job_id is None or not args.evidence:
                raise ConfigurationError("ddl-queue-skip requires --job-id and --evidence")
            extra = dict(job_id=args.job_id, **ddl_queue.read_evidence(args.evidence))
        if args.kind in recovery.KINDS or args.kind in {"cleanup", "resync-table"} or args.source_unavailable:
            extra = {
                key: getattr(args, key)
                for key in (
                    "reason",
                    "action",
                    "lsn",
                    "max_duration",
                    "wal_rate",
                    "source_unavailable",
                    "table",
                )
                if getattr(args, key) is not None
            }
        return await service.plan(args.kind, out=args.out, extra=extra)
    if args.command == "ddl":
        if args.ddl_action == "queue":
            if args.queue_action == "list":
                return await ddl_queue.listing(service)
            if args.queue_action == "retry":
                return await ddl_queue.retry(service, args.id)
            return await ddl_queue.skip(
                service,
                args.plan,
                args.plan_hash,
                args.id,
                ddl_queue.read_evidence(args.evidence)["reason"],
                evidence_file=args.evidence,
            )
        if args.ddl_action == "check":
            return ddl.classify(Path(args.file).read_text())
        if args.ddl_action == "exec":
            return await ddl.execute(service, args.plan, args.plan_hash, args.sql_file)
        async with service.source.connect(readonly=True) as sc:
            return [
                dict(r)
                for r in await sc.fetch("SELECT id,statement_sha256,captured_at FROM lc.ddl_log ORDER BY id")
            ]
    if args.command in recovery.KINDS:
        return await recovery.execute(service, args.command, args.plan, args.plan_hash)
    if args.command == "resync-table":
        from . import resync

        return await resync.execute(service, args.plan, args.plan_hash)
    if args.command == "cleanup":
        from . import emergency

        return await emergency.cleanup(service, args.plan, args.plan_hash)
    if args.command in {"prepare", "start", "abort"}:
        return await getattr(service, args.command)(args.plan, args.plan_hash)
    if args.command == "events":
        return service.store.events(args.after, args.limit)
    if args.command == "run":
        while True:
            result = await service.run_once()
            if args.once:
                return result
            if result["state"] in {"ABORTED", "DETACHED"}:
                return result
            await asyncio.sleep(service.config.get("daemon", "poll_interval_seconds", 5))


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    redactor = Redactor()
    code = 0
    pg_play = "--component-capabilities" in argv or any(
        value == "--contract=" + component.CONTRACT
        or (value == "--contract" and argv[index + 1 : index + 2] == [component.CONTRACT])
        for index, value in enumerate(argv)
    )
    envelope = dict(
        contract_version="pg_logical_connector/result-v2",
        tool="pg-logical-repl",
        tool_version=__version__,
        command=None,
        request_id=str(uuid.uuid4()),
        status="succeeded",
        result=None,
        artifacts=[],
        advisories=[],
        error=None,
    )
    try:
        args = parser().parse_args(argv)
        envelope.update(command=args.command, request_id=args.request_id or envelope["request_id"])
        if args.capabilities or args.component_capabilities:
            envelope.update(
                command="capabilities", result=component.capabilities() if pg_play else describe()
            )
            if pg_play:
                envelope, code = component.envelope(envelope, code)
            print(json.dumps(envelope, ensure_ascii=False, indent=None if args.machine else 2))
            return 0
        if not args.command:
            raise ConfigurationError("A command is required")
        if not args.config:
            raise ConfigurationError("--config or LC_CONFIG is required")
        config = load(args.config)
        service = Service(config, redactor)
        service.store.request_id = envelope["request_id"]
        logging_config.configure(args.log_level, args.log_file)

        async def run_command():
            loop = asyncio.get_running_loop()
            task = asyncio.current_task()
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
            try:
                return await dispatch(args, service)
            finally:
                loop.remove_signal_handler(signal.SIGTERM)

        envelope["result"] = asyncio.run(run_command())
        if args.command == "check" and not envelope["result"]["passed"]:
            envelope["status"] = "blocked"
            code = 3
        if args.command == "verify" and not envelope["result"]["passed"]:
            envelope["status"] = "blocked"
            code = 3
    except (KeyboardInterrupt, asyncio.CancelledError) as exc:
        code = 130 if isinstance(exc, KeyboardInterrupt) else 143
        envelope.update(
            status="cancelled",
            error=dict(code="CANCELLED", message="Interrupted; inspect state before retry"),
        )
    except ConnectorError as exc:
        code = exc.exit_code
        envelope.update(status="failed", error=exc.as_dict())
    except Exception as exc:
        code = 6
        envelope.update(
            status="failed",
            error=dict(
                code="OPERATION_FAILED",
                message=redactor.text(exc),
                sqlstate=getattr(exc, "sqlstate", None),
                retryable=False,
                recovery_hint="Inspect state and catalog before retrying",
            ),
        )
    envelope = redactor.clean(envelope)
    if pg_play:
        envelope, code = component.envelope(envelope, code)
    logging_config.close()
    print(json.dumps(envelope, ensure_ascii=False, default=str, indent=None if "--machine" in argv else 2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
