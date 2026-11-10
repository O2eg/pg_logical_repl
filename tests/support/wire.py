"""Bounded TCP fault proxy used only on the owned test stand.

No PostgreSQL protocol emulation. Authentication, SQL, COPY and replication all
travel over real sockets. SQL needles are kept in memory and never logged.
"""

import asyncio
from contextlib import suppress


class Proxy:
    def __init__(self, upstream_host, upstream_port, *, bind="127.0.0.1", allowed=None):
        self.upstream = upstream_host, upstream_port
        self.bind, self.allowed = bind, set(allowed or ["127.0.0.1"])
        self.mode, self.delay = "normal", 0.0
        self.changed = asyncio.Event()
        self.changed.set()
        self.sessions, self.tasks = set(), set()
        self.needle = None
        self.matched = asyncio.Event()
        self.server = None

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.accept, self.bind, 0, limit=65536)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        self.set_mode("down")
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.server.wait_closed()

    def set_mode(self, mode, *, delay=0):
        assert mode in {"normal", "down", "stall", "latency"}
        self.mode, self.delay = mode, delay
        if mode == "stall":
            self.changed.clear()
        else:
            self.changed.set()
        if mode == "down":
            for pair in list(self.sessions):
                for writer in pair:
                    writer.transport.abort()

    def lose_reply(self, needle):
        self.needle = needle.encode()
        self.matched.clear()

    async def accept(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        peer = writer.get_extra_info("peername")[0]
        upstream = None
        pair = None
        pumps = []
        try:
            if peer not in self.allowed or self.mode == "down":
                return
            other, upstream = await asyncio.open_connection(*self.upstream, limit=65536)
            pair = (writer, upstream)
            self.sessions.add(pair)
            discard = False

            async def pump(incoming, outgoing, downstream):
                nonlocal discard
                tail = b""
                while chunk := await incoming.read(16384):
                    await self.changed.wait()
                    if self.mode == "down":
                        return
                    if self.mode == "latency":
                        await asyncio.sleep(self.delay)
                    if not downstream and self.needle:
                        tail = (tail + chunk)[-max(16384, 2 * len(self.needle)) :]
                        if self.needle in tail:
                            discard, self.needle = True, None
                            self.matched.set()
                    if downstream and discard:
                        continue
                    outgoing.write(chunk)
                    await outgoing.drain()

            pumps = [
                asyncio.create_task(pump(reader, upstream, False)),
                asyncio.create_task(pump(other, writer, True)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            for pump_task in pumps:
                pump_task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for connection in (writer, upstream):
                if connection:
                    connection.close()
                    with suppress(OSError, TimeoutError):
                        await asyncio.wait_for(connection.wait_closed(), 2)
            if pair:
                self.sessions.discard(pair)
            self.tasks.discard(task)
