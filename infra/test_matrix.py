"""Run owned pg-stand profiles sequentially; keep PG18 active afterward."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import asyncpg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-stand", required=True, help="Path to installed pg-stand CLI")
    parser.add_argument("--current-major", type=int, choices=[15, 16, 17, 18], default=18)
    parser.add_argument("--major", type=int, choices=[15, 16, 17, 18], action="append", dest="majors")
    parser.add_argument("--pytest-filter", default="")
    parser.add_argument("--junit-prefix", default="test")
    parser.add_argument("--artifacts-dir", type=Path, default=Path(".runtime/test-matrix"))
    parser.add_argument("--pytest-marker", default="")
    parser.add_argument("--pytest-path")
    parser.add_argument("--pytest-arg", action="append", default=[])
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument(
        "--existing-containers",
        action="store_true",
        help="Use verified existing containers after a project-directory move",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    os.umask(0o077)
    args.artifacts_dir = args.artifacts_dir.resolve()
    args.artifacts_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    current = args.current_major

    def stand(major, action):
        if args.existing_containers:
            import time

            names = [
                f"lc-development{major}-{node}-pg-stand-managed" for node in ("primary", "standby", "logical")
            ]
            containers = json.loads(
                subprocess.check_output(["docker", "container", "inspect", *names], text=True)
            )
            for container, node in zip(containers, ("primary", "standby", "logical"), strict=True):
                labels = container["Config"]["Labels"]
                if (
                    labels.get("io.pg-stand.managed") != "true"
                    or labels.get("io.pg-stand.project") != f"lc-development{major}"
                    or labels.get("io.pg-stand.node") != node
                    or labels.get("io.pg-stand.postgres-major") != str(major)
                ):
                    raise RuntimeError("Refusing an unowned or mismatched test container")
                data = [
                    m
                    for m in container["Mounts"]
                    if m["Destination"] in {"/var/lib/postgresql", "/var/lib/postgresql/data"}
                ]
                expected = root / f".pg_stand/lc-development{major}/{node}/data"
                if (
                    len(data) != 1
                    or not expected.is_dir()
                    or Path(data[0]["Source"]).resolve() != expected.resolve()
                ):
                    raise RuntimeError("Existing test data bind mount does not resolve to this project")
            subprocess.run(
                ["docker", "start" if action == "up" else "stop", *names],
                check=True,
                timeout=90,
                stdout=subprocess.DEVNULL,
            )
            if action == "up":
                deadline = time.monotonic() + 60
                for name in names:
                    while subprocess.run(
                        ["docker", "exec", name, "pg_isready", "-q", "-U", "postgres"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    ).returncode:
                        if time.monotonic() >= deadline:
                            raise RuntimeError("Owned test stand did not become ready")
                        time.sleep(0.2)
            return
        log = root / ".runtime" / f"matrix-pg{major}-{action}.log"
        log.parent.mkdir(exist_ok=True)
        with log.open("w") as output:
            if action == "up":
                # Start existing synchronous peers together. Waiting for the
                # primary's PgBouncer bootstrap before starting its standby can
                # otherwise deadlock a persisted synchronous cluster.
                names = [
                    f"lc-development{major}-{node}-pg-stand-managed"
                    for node in ["primary", "standby", "logical"]
                ]
                probe = subprocess.run(
                    ["docker", "container", "inspect", "--format", "{{json .Config.Labels}}", *names],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if probe.returncode == 0:
                    labels = [json.loads(line) for line in probe.stdout.splitlines()]
                    if len(labels) != 3 or not all(
                        item.get("io.pg-stand.managed") == "true"
                        and item.get("io.pg-stand.project") == f"lc-development{major}"
                        for item in labels
                    ):
                        raise RuntimeError("Refusing to start containers without owned stand labels")
                    subprocess.run(
                        ["docker", "start", *names],
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=30,
                    )
            result = subprocess.run(
                [args.pg_stand, "-c", f"infra/pg{major}.yaml", action],
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=300,
            )
        if result.returncode:
            raise RuntimeError(f"pg-stand {action} PG{major} failed; inspect {log}")

    async def bound_test_wal(major):
        # Dedicated tiny fixture databases only. Avoid the default 1GB checkpoint
        # budget on each of twelve persisted local nodes sharing this laptop disk.
        creds = json.loads((root / ".pg_stand/credentials/database/passwords.json").read_text())["superuser"]
        for offset in (1, 2, 3):
            c = await asyncpg.connect(
                host="127.0.0.1",
                port=55300 + major * 10 + offset,
                database="postgres",
                user=creds["username"],
                password=creds["password"],
            )
            try:
                for key, value in (
                    ("max_wal_size", "128MB"),
                    ("min_wal_size", "32MB"),
                    ("checkpoint_timeout", "60s"),
                ):
                    await c.execute(f"ALTER SYSTEM SET {key}='{value}'")
                await c.fetchval("SELECT pg_reload_conf()")
                await c.execute("CHECKPOINT")
            finally:
                await c.close()

    try:
        for major in args.majors or [15, 16, 17, 18]:
            if current != major:
                stand(current, "stop")
                current = major
                stand(major, "up")
            asyncio.run(bound_test_wal(major))
            print(f"MATRIX_START PG{major}", flush=True)
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    args.pytest_path or ("tests" if major == 18 else "tests/integration"),
                    "-q",
                    "-x",
                    f"--junitxml={args.artifacts_dir}/{args.junit_prefix}_pg{major}.xml",
                    f"--basetemp={args.artifacts_dir}/pytest-pg{major}",
                    "--tb=short",
                    "-s",
                    *(["-m", args.pytest_marker] if args.pytest_marker else []),
                    *args.pytest_arg,
                    *(["-k", args.pytest_filter] if args.pytest_filter else []),
                ],
                env=dict(os.environ, LC_INTEGRATION="1", LC_TEST_MAJOR=str(major)),
                timeout=args.timeout,
            )
            if result.returncode:
                return result.returncode
    finally:
        if current != 18:
            stand(current, "stop")
            stand(18, "up")
    return 0


if __name__ == "__main__":
    sys.exit(main())
