"""Small local-only validation benchmark; creates and drops its own random databases."""

import asyncio
import json
import tempfile
import time
import uuid
from pathlib import Path

import asyncpg
import yaml

from pg_logical_repl import validation
from pg_logical_repl.config import load
from pg_logical_repl.security import identifier as qi
from pg_logical_repl.service import Service
from pg_logical_repl.state import now, write_private

ROOT = Path(__file__).resolve().parents[1]


class Measured:
    def __init__(self, c):
        self.c = c
        self.response_bytes = self.max_response_bytes = self.calls = 0

    def __getattr__(self, name):
        return getattr(self.c, name)

    def record(self, result):
        size = len(json.dumps(result, default=str).encode())
        self.response_bytes += size
        self.max_response_bytes = max(self.max_response_bytes, size)
        self.calls += 1
        return result

    async def fetch(self, sql, *args):
        return self.record(await self.c.fetch(sql, *args))

    async def fetchval(self, sql, *args):
        return self.record(await self.c.fetchval(sql, *args))

    async def fetchrow(self, sql, *args):
        return self.record(await self.c.fetchrow(sql, *args))


async def main():
    creds = json.loads((ROOT / ".pg_stand/credentials/database/passwords.json").read_text())["superuser"]
    name = "lc_validation_bench_" + uuid.uuid4().hex[:10]
    admins, clients = [], []
    try:
        for port in [55481, 55483]:
            args = dict(host="127.0.0.1", port=port, user=creds["username"], password=creds["password"])
            admin = await asyncpg.connect(**args, database="postgres")
            admins.append(admin)
            await admin.execute("CREATE DATABASE " + qi(name))
            c = await asyncpg.connect(**args, database=name)
            clients.append(c)
            await c.execute("SET statement_timeout='60s'")
            await c.execute("""CREATE TABLE payloads(id bigint PRIMARY KEY,payload text);
                INSERT INTO payloads SELECT n,repeat(md5(n::text),16) FROM generate_series(1,100000) n;
                ANALYZE payloads;""")
        sc, tc = clients
        with tempfile.TemporaryDirectory(prefix="lc-validation-bench-") as directory:
            directory = Path(directory)
            write_private(directory / "password", creds["password"])
            source = dict(
                host="127.0.0.1",
                port=55481,
                database=name,
                user=creds["username"],
                password_file="password",
                sslmode="disable",
            )
            raw = dict(
                schema_version="pg_logical_connector/config-v2",
                migration_id=name,
                state_dir="state",
                source=dict(admin=source),
                target=dict(admin=dict(source, port=55483)),
                validation=dict(chunk_rows=5000, pause_between_chunks_ms=50, max_parallel=1),
            )
            write_private(directory / "config.yaml", yaml.safe_dump(raw))
            service = Service(load(directory / "config.yaml"))
            relation = (await validation.inventory(sc))["relations"][0]
            measured_source, measured_target = Measured(sc), Measured(tc)
            started = time.perf_counter()
            result = await validation.verify_table(
                service, measured_source, measured_target, relation, "checksums", False, lambda _: None
            )
            elapsed = time.perf_counter() - started
            assert result["passed"]
            query, params = validation.digest_query(
                '"public"."payloads"', ["id", "payload"], ["id"], '{"id":45000}', '{"id":50000}'
            )
            plan = json.loads(await sc.fetchval("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, *params))

            def nodes(node):
                yield node["Node Type"]
                for child in node.get("Plans", []):
                    yield from nodes(child)

            plan_nodes = list(nodes(plan[0]["Plan"]))
            assert "Sort" not in plan_nodes and any("Index" in n for n in plan_nodes), plan_nodes
            evidence = dict(
                measured_at=now(),
                server_version=await sc.fetchval("SHOW server_version"),
                rows_per_database=100000,
                payload_bytes_per_database=51200000,
                relation_bytes_per_database=await sc.fetchval("SELECT pg_total_relation_size('payloads')"),
                elapsed_seconds=round(elapsed, 3),
                chunk_rows=5000,
                chunks=result["chunks_checked"],
                pause_between_chunks_ms=50,
                max_parallel=1,
                client_result_bytes_estimated=measured_source.response_bytes + measured_target.response_bytes,
                max_single_response_bytes_estimated=max(
                    measured_source.max_response_bytes, measured_target.max_response_bytes
                ),
                result_calls=measured_source.calls + measured_target.calls,
                query_plan_nodes=plan_nodes,
                query_plan=plan,
                result=result,
                caveats=[
                    "100k rows is a local regression experiment, not production-scale qualification",
                    "Two static databases; no replication wait in this timing",
                    "Response size measured as serialized returned values, excludes protocol overhead",
                    "Full checksums still read and hash all payload bytes inside each PostgreSQL server",
                ],
            )
            path = ROOT / "docs/validation_benchmark_pg18_2026-10-30.json"
            path.write_text(json.dumps(evidence, indent=2) + "\n")
            print(json.dumps({k: v for k, v in evidence.items() if k not in {"result", "query_plan"}}))
    finally:
        for c in clients:
            await c.close()
        for admin in admins:
            await admin.execute("DROP DATABASE IF EXISTS " + qi(name))
            await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
