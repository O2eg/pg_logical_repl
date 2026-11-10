"""Secret resolution and output redaction independent of CLI transport."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import quote

from .errors import ConfigurationError


class Redactor:
    def __init__(self):
        self._values: set[str] = set()

    def add(self, value: str | None):
        if value:
            self._values.update((value, quote(value, safe=""), value.replace("'", "''")))

    def text(self, value: object) -> str:
        text = str(value)
        for secret in sorted(self._values, key=len, reverse=True):
            text = text.replace(secret, "[REDACTED]")
        text = re.sub(r"SCRAM-SHA-256\$[^\s'\"]+|\bmd5[a-fA-F0-9]{32}\b", "[VERIFIER]", text)
        text = re.sub(r"(?i)(postgres(?:ql)?://[^:/@\s]+:)[^@\s]+(@)", r"\1[REDACTED]\2", text)
        text = re.sub(
            r"(?is)(\bpassword\s*(?:=|\s)\s*)(?:'(?:[^'\\]|\\.|'')*'|\"[^\"]*\"|[^\s;]+)",
            r"\1[REDACTED]",
            text,
        )
        return text

    def clean(self, value):
        if isinstance(value, dict):
            return {
                k: "[REDACTED]"
                if k.lower() in {"password", "rolpassword", "secret", "conninfo", "ownership_token"}
                else self.clean(v)
                for k, v in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self.clean(v) for v in value]
        return self.text(value) if isinstance(value, str) else value


def private_file(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        raise ConfigurationError(f"Secret file does not exist or is a symlink: {path}")
    if path.stat().st_mode & 0o077:
        raise ConfigurationError(f"Secret file must have permissions 0600: {path}")
    return path.read_text().rstrip("\r\n")


def password(settings: dict, base: Path, redactor: Redactor) -> str | None:
    choices = [key for key in ("password_env", "password_file", "passfile") if settings.get(key)]
    if len(choices) > 1:
        raise ConfigurationError("Only one password source is allowed")
    if settings.get("password_env"):
        result = os.environ.get(settings["password_env"])
        if result is None:
            raise ConfigurationError(f"Missing environment variable {settings['password_env']}")
    elif settings.get("password_file"):
        result = private_file(base / settings["password_file"])
    elif settings.get("passfile"):
        contents = private_file(base / settings["passfile"])
        result = None
        for line in contents.splitlines():
            if not line or line.startswith("#"):
                continue
            fields, buf, escaped = [], "", False
            for ch in line:
                if escaped:
                    buf += ch
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == ":" and len(fields) < 4:
                    fields.append(buf)
                    buf = ""
                else:
                    buf += ch
            fields.append(buf)
            expected = [str(settings.get(k, "")) for k in ("host", "port", "database", "user")]
            if len(fields) == 5 and all(a == "*" or a == b for a, b in zip(fields[:4], expected)):
                result = fields[4]
                break
        if result is None:
            raise ConfigurationError("No matching record in passfile")
    else:
        result = None
    redactor.add(result)
    return result


def identifier(value: str) -> str:
    if not isinstance(value, str) or "\x00" in value or not value:
        raise ConfigurationError("Invalid SQL identifier")
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    if "\x00" in value:
        raise ConfigurationError("NUL in SQL value")
    return "'" + value.replace("'", "''") + "'"


def qualified(schema: str, name: str) -> str:
    return identifier(schema) + "." + identifier(name)


def conninfo(values: dict) -> str:
    return " ".join(
        k + "='" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"
        for k, v in values.items()
        if v is not None
    )
