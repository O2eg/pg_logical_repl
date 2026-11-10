"""SQL-only asyncpg connections and libpq configuration for native subscriptions."""

from __future__ import annotations

import ssl
from contextlib import asynccontextmanager

import asyncpg

from .errors import ConnectorError
from .security import password


class Database:
    def __init__(self, config, side, redactor):
        self.config, self.side, self.redactor = config, side, redactor
        self.settings = config.endpoint(side)

    def tls(self):
        e = self.settings
        mode = e.get("sslmode", "verify-full")
        if mode == "disable":
            return False
        if mode == "require":
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            ca = str(self.config.path.parent / e["sslrootcert"]) if e.get("sslrootcert") else None
            ctx = ssl.create_default_context(cafile=ca)
            ctx.check_hostname = mode == "verify-full"
        if e.get("sslcert"):
            ctx.load_cert_chain(
                str(self.config.path.parent / e["sslcert"]), str(self.config.path.parent / e["sslkey"])
            )
        return ctx

    @asynccontextmanager
    async def connect(self, *, readonly=False, timeout=None):
        e = self.settings
        pw = password(e, self.config.path.parent, self.redactor)
        settings = {
            "application_name": "pg-logical-repl/" + self.config.migration_id,
            "statement_timeout": str(int(1000 * self.config.get("limits", "query_timeout_seconds", 60))),
            "lock_timeout": str(int(1000 * self.config.get("limits", "lock_timeout_seconds", 5))),
        }
        if readonly:
            settings["default_transaction_read_only"] = "on"
        try:
            c = await asyncpg.connect(
                host=e["host"].split(","),
                port=e["port"],
                database=e["database"],
                user=e["user"],
                password=pw,
                ssl=self.tls(),
                timeout=timeout
                or e.get("connect_timeout_seconds", self.config.get("limits", "connect_timeout_seconds", 10)),
                server_settings=settings,
                target_session_attrs="primary",
            )
        except Exception as exc:
            raise ConnectorError(
                "DATABASE_UNAVAILABLE",
                self.redactor.text(exc),
                retryable=True,
                sqlstate=getattr(exc, "sqlstate", None),
            ) from None
        try:
            yield c
        finally:
            await c.close(timeout=5)

    def libpq(self, *, replication=False):
        e = self.config.endpoint(self.side, "replication" if replication else "admin")
        pw = password(e, self.config.path.parent, self.redactor)
        values = dict(
            host=e["host"],
            port=e["port"],
            dbname=e["database"],
            user=e["user"],
            sslmode=e.get("sslmode", "verify-full"),
            connect_timeout=e.get(
                "connect_timeout_seconds", self.config.get("limits", "connect_timeout_seconds", 10)
            ),
            application_name=self.config.subscription if replication else "pg-logical-repl",
        )
        if e.get("hostaddr"):
            values["hostaddr"] = e["hostaddr"]
        if replication:
            values.update(
                password=pw,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=6,
                options="-crow_security=off",
            )
            if e.get("sslrootcert_on_target"):
                values["sslrootcert"] = e["sslrootcert_on_target"]
        elif e.get("sslrootcert"):
            values["sslrootcert"] = str(self.config.path.parent / e["sslrootcert"])
        if not replication:
            for key in ("sslcert", "sslkey"):
                if e.get(key):
                    values[key] = str(self.config.path.parent / e[key])
        return values, pw


async def identity(c):
    version = int(await c.fetchval("SHOW server_version_num"))
    locale = "datlocale" if version >= 170000 else "daticulocale"
    row = dict(
        await c.fetchrow(
            """SELECT oid::int AS database_oid, datname AS database,
        pg_encoding_to_char(encoding) AS encoding, datcollate, datctype, datlocprovider::text,
        datcollversion, """
            + locale
            + " AS locale FROM pg_database WHERE datname=current_database()"
        )
    )
    try:
        row["system_identifier"] = str(await c.fetchval("SELECT system_identifier FROM pg_control_system()"))
        row["timeline_id"] = await c.fetchval("SELECT timeline_id FROM pg_control_checkpoint()")
    except asyncpg.InsufficientPrivilegeError:
        row["system_identifier"] = None
        row["timeline_id"] = None
    row.update(
        version=version, major=version // 10000, in_recovery=await c.fetchval("SELECT pg_is_in_recovery()")
    )
    return row
