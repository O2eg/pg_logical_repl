# pg-logical-repl

A Python CLI for managing PostgreSQL native logical replication. Python manages
checks, schema, and the migration lifecycle; PostgreSQL `tablesync`/`apply` workers
copy and apply rows. The runtime uses neither pgcopydb nor pgstream and has no
custom WAL/DML engine.

**The current version is development alpha `0.1.0a4`; the full SPEC v2 is not yet implemented.**
Actual coverage and test results are documented in
[IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md). Unsupported settings are
rejected; `--capabilities` lists implemented commands and limitations.

## Installation

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/pg-logical-repl --capabilities --machine
```

The pip distribution and CLI command are named `pg-logical-repl`; the Python
package is `pg_logical_repl`. The CLI also runs as `python -m pg_logical_repl`.

When upgrading an environment from the previous package name, run
`python -m pip uninstall pg-logical-connector`, then reinstall this project.
Replace `pg_logical_connector` with `pg_logical_repl` in Python imports.

For compatibility, existing `schema_version` and `contract_version` values retain
the `pg_logical_connector/` prefix. These identify formats, not the importable
package. The `LC_*` variables, internal `lc` schema, SQL resources, and lock keys
are also retained. Configuration, plan, and state files do not need to be renamed.

The checks introduced in 0.1.0a3 require a new migration: `prepare` records the
initial state of roles and internal resources. An active migration from an earlier
alpha without this baseline receives `INTEGRITY_BASELINE_MISSING`; current roles
and resources are not automatically accepted as a baseline. Finish an interrupted
older migration using its corresponding version of the utility.

`pg_dump`, `pg_restore`, and `psql` must match the major version of both databases.
Their paths can be configured in `client_tools.bin_dirs`. Passwords are referenced
through environment variables, a file with mode `0600`, or pgpass. Do not put
passwords in YAML or command arguments. Internal files use mode `0600`; the state
directory uses `0700`.

## Project structure

| Directory / modules | Purpose |
|---|---|
| `src/pg_logical_repl/cli.py`, `service.py` | CLI and asynchronous migration lifecycle API |
| `config.py`, `plans.py`, `state.py`, `preflight.py` | Configuration, operation plans, state, and preflight checks |
| `db.py`, `catalog.py`, `roles.py`, `schema.py` | Connections, PostgreSQL catalogs, role and schema transfer |
| `ddl.py`, `ddl_queue.py`, `sql/` | DDL validation, queue, and server SQL resources |
| `health.py`, `validation.py`, `cutover.py` | Replication monitoring, data validation, and replica detachment |
| `integrity.py`, `monitoring.py` | Drift detection, background data validation, sequences, and metrics |
| `recovery.py`, `resync.py`, `emergency.py` | Pause/resume/resolve, table resynchronization, emergency detachment, and cleanup |
| `tests/`, `infra/` | Unit and integration tests, local test environments, and build checks |
| `examples/`, `docs/` | Sample configuration and guides |

## Connections

Start with [examples/local.yaml](examples/local.yaml). Relative paths are resolved
from the YAML file. The sidecar process uses `source.admin`, while the PostgreSQL
target uses `source.replication`, so their addresses and ports may differ.
The example uses `sslmode: disable` for an isolated local Docker test environment.
For TLS, use `verify-full`, `sslrootcert`, and `sslrootcert_on_target`.

The current profile requires superuser access on both **dedicated test** servers:
on the source to read role passwords and install event triggers, and on the target
to restore roles and create a subscription. An empty target database does not imply
an empty cluster: existing roles with the same names must match, including their
password verifiers. The same plaintext password with a different SCRAM salt does
not produce a matching verifier. The utility does not replace such roles.

`capacity.json` contains actual external measurements of free space and estimates
of the required capacity, along with the identities of both servers:

```json
{
  "measured_at": "2026-10-30T09:00:00+00:00",
  "source": {"system_identifier": "SOURCE_ID", "free_bytes": 20000000000, "required_bytes": 5000000000},
  "target": {"system_identifier": "TARGET_ID", "free_bytes": 20000000000, "required_bytes": 5000000000}
}
```

Do not copy the numbers or timestamp from this example: stale or missing
measurements block preparation. The `check` command reports the identities.

Before preparation, the profile rejects unsupported RLS, rules, foreign servers
and mappings, custom collations/types/operators/casts, extended statistics, text
search, nonstandard tablespaces, and security labels. `check` writes a partial
`coverage/latest.json`; FDW options and mapping secrets are excluded. This manifest
does not yet fully implement SPEC §3.8.

## Main workflow

```sh
export LC_CONFIG=/absolute/path/migration.yaml
pg-logical-repl --machine validate
pg-logical-repl --machine check
pg-logical-repl --machine plan prepare --out prepare.json
pg-logical-repl --machine prepare --plan prepare.json --plan-hash HASH_FROM_PLAN
pg-logical-repl --machine plan start --out start.json
pg-logical-repl --machine start --plan start.json --plan-hash HASH_FROM_PLAN
pg-logical-repl --machine run
```

`run` completes bootstrap and monitors the migration. Use `run --once` for a single
iteration. Native data replication continues after Python stops. `run` checks
identities, the publication, subscription, and slot; configuration drift or a
persistent cycle of apply/sync errors moves the migration to `BLOCKED`.
`status.health.checked_at` records the daemon's latest check. A WAL retention
warning does not delete the slot. Use `ddl queue retry/skip` for queue errors and
planned `resume/resolve` operations to leave `BLOCKED` after fixing the cause.
Inspection commands:

```sh
pg-logical-repl --machine status
pg-logical-repl --machine verify
pg-logical-repl --machine events --after 0 --limit 100
pg-logical-repl --machine report
```

`verify` compares catalogs and server aggregates: row count and the sum of 128-bit
MD5 hashes of row text representations, using identical GUC settings on both
servers. Row payloads and large object pages are not transferred to Python. MD5
serves as a checksum for detecting differences, not a cryptographic proof of equality.

Tables with primary keys are scanned by key range, including composite keys.
Ranges also cover extra target rows before the first or after the last source key.
There is no full data sort; each SQL query returns only aggregates or a chunk
boundary. After a source REPEATABLE READ transaction, the utility waits for the
slot to acknowledge the LSN, then reads the target. Mismatches are rechecked;
source changes are marked as `unstable`. With an active source, the full scan is
not a single consistent snapshot. Final validation runs after writers are stopped
externally.

```sh
pg-logical-repl --machine verify --table public.orders --level checksums
pg-logical-repl --machine verify --level counts
```

Workload limits are configured under `validation`: `chunk_rows` (default 10000),
`max_parallel` (1, the number of tables validated concurrently),
`pause_between_chunks_ms` (50), and `max_retries` (3). Each query is subject to
`limits.query_timeout_seconds`; additional parallel query workers are disabled
during validation. At cutover, tables are checked sequentially while locks are
held. Chunk metadata is written to a separate private JSONL file.

Tables without a primary key require a full server scan, reported as
`NO_PRIMARY_KEY_FULL_SCAN`. The `counts` level checks row counts only and does not
detect changed values. Both `checksums` and `full` currently scan all selected
tables in full: cold chunk caching and incremental cutover are not yet implemented.
A full scan of a large database remains expensive in PostgreSQL I/O and CPU; choose
limits according to the test workload. See the [testing guide](docs/TESTING.md).

## Background monitoring and settings

`run` validates one primary key range per daemon iteration, saving the boundary,
source aggregate, and expected LSN. Waiting for apply resumes after a process
restart. Schema changes reset the cursor. A confirmed mismatch causes `BLOCKED`;
a changing range is marked as `unstable`. For a table without a primary key, one
range means a full server scan. `validation.max_parallel` limits manual `verify`;
background validation is sequential.

| Setting | Behavior |
|---|---|
| `validation.online: true` | Enable background data validation; `false` disables it while manual verify remains available |
| `daemon.drift_interval_seconds: 900` | Interval for checking schema, ACLs, roles, membership, verifiers, internal functions/triggers, and the journal; `0` means every iteration |
| `sequences.presync_interval_seconds: 300` | Presynchronize sequences; `0` disables this, but final synchronization is mandatory |
| `daemon.metrics_textfile` | Atomically updated Prometheus textfile; path relative to YAML, mode `0600` |
| `limits.catchup_lag_seconds`, `catchup_stable_seconds` | Acceptable measured lag and the time it must remain stable before entering `REPLICATING` |
| `limits.alert_lag_seconds`, `alert_slot_retained_bytes`, `alert_refresh_age_seconds`, `alert_queue_age_seconds` | Warnings in health/events; these do not remove resources by themselves |
| `cutover.analyze: never` or `all` | Skip or run ANALYZE after the final data transfer |

Catalog comparison waits for a barrier and completion of the DDL queue so that
normal schema lag is not treated as drift. Roles are also compared with the
`prepare` baseline: identical changes on both sides do not hide drift. Verifiers
are compared using HMAC; reports list only the paths of differing properties.
Acknowledged journal entries older than an hour are pruned; compact receipts for
managed DDL are retained for retries. `run` resumes unfinished pruning intents.

Intervals, validation limits, and the ANALYZE policy can change between iterations.
Changes to endpoints, the publication, server DDL policy, or the installed
`lock_timeout_seconds` require restoring the original configuration. Every
mutation plan is bound to the entire configuration at the time it was created.

`validate` rejects unsupported settings: `mode: replica`, waivers, reconciliation
of existing roles, publication filtering, binary/failover/parallel apply, parallel
index builds, and alternative transfer policies. `admin.hostaddr` and replication
client certificates are unsupported; administrative `sslcert/sslkey` must be
provided together. External controls must keep applications off the target. The
utility detects ordinary client sessions but does not manage pg_hba or restrict
superusers.

## Recovery and pausing

These operations require a plan, its hash, and a reason:

```sh
pg-logical-repl --machine plan resume --reason "blocking issue resolved" --out resume.json
pg-logical-repl --machine resume --plan resume.json --plan-hash HASH_FROM_PLAN
pg-logical-repl --machine plan pause --max-duration 60 --wal-rate 1048576 --reason "maintenance" --out pause.json
pg-logical-repl --machine pause --plan pause.json --plan-hash HASH_FROM_PLAN
```

The pause duration is in seconds; `wal-rate` is a conservative upper estimate of
WAL generation in bytes per second. Planning and execution check fresh capacity
evidence, slot headroom, and the available idle timeout with a safety factor.
This is an estimate for the specified workload. The daemon continues monitoring
in `PAUSED` and reports an exceeded deadline, but does not automatically enable
the subscription. Leaving this state through `resume` requires ownership, health,
role, resource, schema, and data checks.

```sh
pg-logical-repl --machine plan resolve --action applied-manually --reason "data repaired" --out resolve.json
pg-logical-repl --machine resolve --plan resolve.json --plan-hash HASH_FROM_PLAN
pg-logical-repl --machine plan resolve --action skip --lsn 0/12345678 --reason "justification for skipping the entire transaction" --out skip.json
pg-logical-repl --machine resolve --plan skip.json --plan-hash HASH_FROM_PLAN
pg-logical-repl --machine plan resync-table --table public.orders --reason "recopy table" --out resync.json
pg-logical-repl --machine resync-table --plan resync.json --plan-hash HASH_FROM_PLAN
```

The LSN for `skip` comes from the apply worker's message identifying the finish LSN
of the **specific** problematic transaction; replace the example with the actual
value. The entire transaction is skipped. Data validation after skipping is
mandatory: incomplete repairs leave the migration in `BLOCKED`. Interventions are
recorded in state, events, and the final cutover report.

`resync-table` supports ordinary logged tables: it temporarily removes the table
from the publication, clears only that table's data on the target, starts native
tablesync, restores incoming foreign keys, and checks the result. Partitions and
extension tables are explicitly rejected. After a failure, retry with the
**original plan**, including its hash; a started plan can resume after its initial
execution deadline. Aborting during copying preserves partial data; removed
incoming foreign keys are restored as `NOT VALID` and listed in
`resync_abort_constraints`.

## DDL

`frozen` blocks DDL until replication is detached. In `managed_only`, DDL goes
through `ddl exec`; `captured` also captures application DDL through an event
trigger. In both modes, a dedicated role without SUPERUSER and an `ENABLE REPLICA`
trigger execute DDL in the target replication stream.

Supported operations include new tables and partitions, CTAS, column/type/DEFAULT
changes, constraints, indexes, views/functions/triggers, sequences, enum/domain,
ACLs, and comments. New tables are connected automatically through the publication
and `REFRESH PUBLICATION WITH (copy_data=true)`, with READY tracking.

A durable queue handles CONCURRENTLY indexes and VALIDATE operations. Partitioned
indexes are built as a tree: ON ONLY → leaf indexes → ATTACH. Retries verify
intents, OIDs, and definitions. Further source DDL is rejected until the queue
finishes; DML continues. Pending jobs or tables block cutover.

```sh
pg-logical-repl --machine ddl check change.sql
pg-logical-repl --machine plan ddl-exec --sql-file change.sql --out ddl.json
pg-logical-repl --machine ddl exec --sql-file change.sql --plan ddl.json --plan-hash HASH_FROM_PLAN
pg-logical-repl --machine ddl queue list
pg-logical-repl --machine ddl queue retry 42
```

Use fully qualified names and one statement per CLI invocation. Unsupported
classes, CASCADE, DO, nondeterministic rewrites, and operations exceeding limits
are rejected. For the detailed matrix, settings, recovery, `queue skip` evidence,
and profile limitations, see [DDL_IMPLEMENTATION.md](docs/DDL_IMPLEMENTATION.md).

SQL resource version 2 requires a new migration; version 1 resources are not
automatically upgraded in an active migration.

## Detaching the replica

The operator stops all writers, including background jobs, DDL, `nextval`, large
object writes, and unlogged table writes, and drains old sessions. Applications
must remain disconnected from the target. Record this in `write-fence.json`:

```json
{
  "migration_id": "example_migration",
  "source_identity": {"COPY": "exact source identity from state.json"},
  "created_at": "2026-10-30T09:00:00+00:00",
  "responsible": "operator",
  "external_fence": "Describe the actual mechanism that stopped every writer"
}
```

Replace `source_identity` with the complete object from state. The document records
the external fence; it does not itself block superuser writers.

```sh
pg-logical-repl --machine plan cutover-finalize --out finalize.json
pg-logical-repl --machine cutover finalize --plan finalize.json --plan-hash HASH_FROM_PLAN --write-fence write-fence.json
pg-logical-repl --machine plan cutover-commit --out commit.json
pg-logical-repl --machine cutover commit --plan commit.json --plan-hash HASH_FROM_PLAN
```

Finalize waits for a barrier, copies unlogged tables and large objects with their
OIDs/ACLs, synchronizes sequence `last_value/is_called`, refreshes materialized
views in dependency order, and validates data. SHARE locks are held only during
the command. An external fence is mandatory between finalize and commit; commit
repeats validation, including comparison with the source contents recorded at
finalize. Applications can switch over after `DETACHED`.

To cancel before finalize: `plan abort` → `abort --plan ... --plan-hash ...`.
From `FINALIZING` or `CUTOVER_READY`, return to replication with
`cutover abort --reason "reason"`; the operator removes the external fence.
Only internal resources owned by the migration are removed. Application tables
and transferred roles remain on the target. Data is not deleted automatically.
After a failure, use the original plan to retry the interrupted step. An ownership
error requires investigation, not recreation of resources by name.

When the source is unavailable, create a separate emergency plan:

```sh
pg-logical-repl --machine plan abort --source-unavailable --reason "source unavailable" --out offline.json
pg-logical-repl --machine abort --plan offline.json --plan-hash HASH_FROM_PLAN
# Alternatively, after successful finalize with an active external fence:
pg-logical-repl --machine plan cutover-commit --source-unavailable --reason "source lost after finalize" --out detach.json
pg-logical-repl --machine cutover commit --plan detach.json --plan-hash HASH_FROM_PLAN
# After access to the source server is restored:
pg-logical-repl --machine plan cleanup --reason "source restored" --out cleanup.json
pg-logical-repl --machine cleanup --plan cleanup.json --plan-hash HASH_FROM_PLAN
```

Emergency detachment checks the saved identity and subscription OID, disables the
subscription, unlinks its slot, and drops the subscription without contacting the
source. The resulting state is `ABORT_CLEANUP_PENDING` or `DETACH_CLEANUP_PENDING`;
the source slot and guard remain until cleanup. Emergency commit is allowed only
after finalize, with a fresh report, a fence, and a repeated check that the target
has not changed. The report explicitly states that the source was not rechecked.
Emergency abort requires identities recorded by a completed prepare.

`cleanup` requires both original identities, ownership receipts, and the exact
inactive slot state recorded in the plan. Unknown additional slots, including
leftover tablesync slots, block automatic cleanup and require an ownership
investigation. If a response is lost, retry the original plan; application data
is not deleted.

## Local testing

The `infra/pg15.yaml` … `infra/pg18.yaml` environments use pg-stand. Only one profile
is active at a time; unrelated Docker containers are not affected.

```sh
pg-stand -c infra/pg18.yaml up
.venv/bin/python -m pytest tests/test_foundations.py -q
LC_INTEGRATION=1 LC_TEST_MAJOR=18 .venv/bin/python -m pytest tests/integration/test_lifecycle.py -q
```

Tests create separate databases with random names, exercise workloads and failures,
and remove their own resources. The restart test restarts only the selected
environment's logical container. pg-stand credentials remain in `.pg_stand`, which
is excluded from artifacts.

Run all owned profiles sequentially (PG18 remains active afterward):

```sh
.venv/bin/python infra/test_matrix.py --pg-stand /path/to/pg-stand --current-major 18
```

After moving the project directory, use `--existing-containers` to test an existing
environment: the script checks project/version/node labels and actual bind mounts
before start/stop. If Docker retains an old path, it must resolve to the same data
directory.

The extended test suite builds and installs a separate wheel:

```sh
.venv/bin/python infra/quality.py --profile matrix --pg-stand /path/to/pg-stand
.venv/bin/python infra/quality.py --profile soak --pg-stand /path/to/pg-stand --seconds 1800
```

The [testing guide](docs/TESTING.md) describes the independent oracle, crash
boundaries, network faults, generator, exact replay, and limitations. New reports
and logs are stored in `.runtime/`, outside the source tree tracked by Git.

The full DDL matrix, automatic repair, comprehensive coverage of all objects, and
operational qualification remain separate work; successful local tests do not
replace them.
