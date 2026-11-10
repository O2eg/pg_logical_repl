"""One-off data-preserving reconfiguration of this repository's owned stands.

Run with the pg-stand environment Python. Never use --clear-data. All three
persisted synchronous peers start before pg-stand bootstrap waits for commits.
"""

import os
import subprocess
from pathlib import Path

from pg_stand.config import load_config
from pg_stand.runtime import (
    StandManager,
    logical_postgres_command,
    primary_postgres_command,
    standby_postgres_command,
)

root = Path(__file__).resolve().parents[1]
os.chdir(root)
os.umask(0o077)
for major in (18, 15, 16, 17):
    cfg = load_config(root / f"infra/pg{major}.yaml")
    assert cfg.metadata.name == f"lc-development{major}"
    manager = StandManager(cfg, output=lambda message: None)
    # The manager verifies managed/project/instance/node labels before removal.
    containers = []
    for name, node in [("primary", cfg.primary), ("standby", cfg.standby), ("logical", cfg.logical)]:
        container = manager._owned_container(node, name)
        if container:
            mounts = container.attrs["Mounts"]
            assert any(
                m["Type"] == "bind" and m["Source"].startswith(str(root / ".pg_stand" / cfg.metadata.name))
                for m in mounts
            )
            containers.append(container)
    manager.stop(timeout_seconds=30)
    manager._acquire_active_lock()
    for container in containers:
        container.remove(v=False)  # Persistent bind mounts are deliberately preserved.
    network = manager._ensure_network()
    for name, node, command in [
        ("primary", cfg.primary, primary_postgres_command(cfg)),
        ("standby", cfg.standby, standby_postgres_command(cfg)),
        ("logical", cfg.logical, logical_postgres_command(cfg)),
    ]:
        manager._ensure_database_container(
            node_name=name, node=node, storage=manager._node_storage(name), network=network, command=command
        )
    manager._activate(timeout_seconds=180)
    subprocess.run(
        [str(root / ".venv/bin/python"), str(root / "infra/checkpoint_local.py"), str(major)], check=True
    )
    manager.stop(timeout_seconds=30)
    print(f"RECONFIGURED PG{major}: bind data preserved", flush=True)
# Leave the conventional PG18 profile active.
cfg = load_config(root / "infra/pg18.yaml")
manager = StandManager(cfg, output=lambda message: None)
manager._acquire_active_lock()
for name, node in [("primary", cfg.primary), ("standby", cfg.standby), ("logical", cfg.logical)]:
    manager._owned_container(node, name).start()
manager._activate(timeout_seconds=180)
print("ACTIVE PG18", flush=True)
