"""Durable state, append-only events and process-scoped exclusion."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import secrets
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .errors import ConnectorError, OwnershipError


def now():
    return datetime.now(UTC).isoformat()


def write_private(path: Path, content: str | bytes):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise OwnershipError(f"Refusing symlink: {path}")
    fd, tmp = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content.encode() if isinstance(content, str) else content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        d = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class Store:
    def __init__(self, directory: Path, migration_id: str, redactor):
        self.directory, self.migration_id, self.redactor = directory, migration_id, redactor
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if directory.is_symlink() or directory.stat().st_mode & 0o077:
            raise OwnershipError("state_dir must be a private directory (0700)")
        self.path = directory / "state.json"

    def read(self):
        if self.path.is_symlink():
            raise OwnershipError("State file is a symlink")
        if not self.path.exists():
            return dict(
                schema_version="pg_logical_connector/state-v2",
                migration_id=self.migration_id,
                state="NEW",
                completed=[],
                ownership_token=None,
            )
        try:
            result = json.loads(self.path.read_text())
        except (OSError, ValueError):
            raise ConnectorError("STATE_CORRUPT", "Cannot parse state file") from None
        if result.get("migration_id") != self.migration_id:
            raise OwnershipError("state migration_id mismatch")
        if result.get("schema_version") != "pg_logical_connector/state-v2":
            raise OwnershipError("Unsupported state version")
        self.redactor.add(result.get("ownership_token"))
        return result

    def save(self, state):
        state["updated_at"] = now()
        write_private(self.path, json.dumps(state, sort_keys=True, indent=2))

    def initialize(self):
        state = self.read()
        if not state["ownership_token"]:
            state["ownership_token"] = secrets.token_urlsafe(32)
            self.save(state)
        self.redactor.add(state["ownership_token"])
        return state

    def event(self, event_name, **details):
        path = self.directory / "events.jsonl"
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "r+b") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.seek(0, os.SEEK_END)
            size = f.tell()
            sequence = 0
            if size:
                end = size
                tail = b""
                while end and tail.count(b"\n") < 2:
                    start = max(0, end - 4096)
                    f.seek(start)
                    tail = f.read(end - start) + tail
                    end = start
                try:
                    if not tail.endswith(b"\n"):
                        raise ValueError("incomplete event")
                    sequence = json.loads(tail.splitlines()[-1])["sequence"]
                except (ValueError, KeyError):
                    raise ConnectorError(
                        "EVENT_LOG_CORRUPT", "Refusing to append after an incomplete event"
                    ) from None
            event = dict(
                sequence=sequence + 1,
                timestamp=now(),
                migration_id=self.migration_id,
                event=event_name,
                details=self.redactor.clean(details),
            )
            if getattr(self, "request_id", None):
                event["request_id"] = self.request_id
            f.write((json.dumps(event, ensure_ascii=False, default=str) + "\n").encode())
            f.flush()
            os.fsync(f.fileno())
        logging.getLogger("pg_logical_repl").info(json.dumps(event, ensure_ascii=False, default=str))
        return event

    def events(self, after=0, limit=10000):
        path = self.directory / "events.jsonl"
        if not path.exists():
            return []
        result = []
        try:
            if after < 0 or limit < 1:
                raise ValueError("Invalid cursor")
            previous = 0
            with path.open() as stream:
                for line in stream:
                    row = json.loads(line)
                    if row["sequence"] != previous + 1:
                        raise ValueError("Sequence discontinuity")
                    previous = row["sequence"]
                    if row["sequence"] > after:
                        result.append(row)
                        if len(result) >= limit:
                            break
        except (OSError, ValueError, KeyError):
            raise ConnectorError(
                "EVENT_LOG_CORRUPT", "Event log is incomplete or corrupt; reconcile before mutation"
            ) from None
        return result

    @contextmanager
    def lock(self):
        fd = os.open(self.directory / "controller.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise OwnershipError("Another mutation controller is running") from None
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
