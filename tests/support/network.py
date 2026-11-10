"""Three independently controlled paths in the owned pg-stand network."""

import json
import subprocess
from contextlib import AsyncExitStack, asynccontextmanager

from pg_logical_repl.service import Service

from .cli import persist_config
from .wire import Proxy


@asynccontextmanager
async def routed(service, major):
    name = f"lc-development{major}-logical-pg-stand-managed"
    network = f"lc-development{major}-pg-stand-managed"
    container = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]
    assert container["Config"]["Labels"]["io.pg-stand.project"] == f"lc-development{major}"
    route = container["NetworkSettings"]["Networks"][network]
    gateway, target_ip = route["Gateway"], route["IPAddress"]
    async with AsyncExitStack() as stack:
        source = await stack.enter_async_context(Proxy("127.0.0.1", 55300 + major * 10 + 1))
        target = await stack.enter_async_context(Proxy("127.0.0.1", 55300 + major * 10 + 3))
        replication = await stack.enter_async_context(
            Proxy("127.0.0.1", 55300 + major * 10 + 1, bind=gateway, allowed=[target_ip])
        )
        service.config.raw["source"]["admin"].update(
            host="127.0.0.1", port=source.port, connect_timeout_seconds=2
        )
        service.config.raw["target"]["admin"].update(
            host="127.0.0.1", port=target.port, connect_timeout_seconds=2
        )
        service.config.raw["source"]["replication"].update(host=gateway, port=replication.port)
        service.config.raw["validation"] = dict(online=False)
        persist_config(service)
        yield Service(service.config), dict(source=source, target=target, replication=replication)
