"""Local pg-stand image workaround for an unavailable upstream PgBouncer pin."""

import argparse
from pathlib import Path

import docker
import pg_stand

parser = argparse.ArgumentParser()
parser.add_argument("--pgbouncer-version", default="1.26.1")
args = parser.parse_args()
image, _ = docker.from_env().images.build(
    path=str(Path(pg_stand.__file__).parent),
    dockerfile="docker/Dockerfile.pg13-pg17",
    tag="pg-logical-connector/pgstand16:" + args.pgbouncer_version,
    buildargs={
        "POSTGRES_BASE_IMAGE": "postgres:16",
        "PG_MAJOR": "16",
        "PGBOUNCER_VERSION": args.pgbouncer_version,
    },
    rm=True,
    forcerm=True,
)
print(image.id)
