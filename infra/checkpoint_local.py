"""Checkpoint only an explicitly named owned local test profile."""

import asyncio
import json
import sys
from pathlib import Path

import asyncpg


async def main():
    major = int(sys.argv[1])
    assert major in (15, 16, 17, 18)
    root = Path(__file__).resolve().parents[1]
    creds = json.loads((root / ".pg_stand/credentials/database/passwords.json").read_text())["superuser"]
    for cycle in range(2):
        for offset in (1, 2, 3):
            c = await asyncpg.connect(
                host="127.0.0.1",
                port=55300 + major * 10 + offset,
                database="postgres",
                user=creds["username"],
                password=creds["password"],
            )
            try:
                if offset == 1:
                    assert await c.fetchval("SHOW wal_keep_size") == "32MB"
                    await c.fetchval("SELECT pg_switch_wal()")
                await c.execute("CHECKPOINT")
            finally:
                await c.close()
        await asyncio.sleep(1)


asyncio.run(main())
