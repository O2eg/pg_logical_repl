# Correctness and recovery testing

Tests run in dedicated environments defined by `infra/pg15.yaml` … `infra/pg18.yaml`.
Each version needs a source primary, its synchronous physical standby, and an
independent logical target. Linux, Docker, pg-stand, Python 3.11+, and matching
versions of `pg_dump`/`pg_restore` are required. Credentials are read from
`.pg_stand/credentials/database/passwords.json`; these tests are not intended for
application or shared PostgreSQL clusters.

Test databases have random names. Each scenario removes its subscriptions, slots,
databases, and created roles afterward. Because roles are shared across a cluster,
parallel pytest/xdist runs on the same environment are prohibited. `infra/quality.py`
prevents concurrent runs with its own file lock. Version profiles are switched
sequentially; PG18 remains active after the run.

## Five testing areas

| Area | What is checked | Files |
|---|---|---|
| Independent oracle | Exact rows and duplicate multiplicity; schema definitions, constraints, indexes, ACLs, roles, and membership; sequences, unlogged tables, and large objects after finalize | `tests/support/oracle.py`, `test_quality_oracle.py` |
| Process interruptions and state writes | SIGKILL before/after server COMMIT, restore, subscription creation/removal, TRUNCATE, resync receipts, finalize, commit, abort, cleanup; SIGTERM and a competing controller; atomic JSON writes under fsync/rename and write failures | `test_quality_process.py`, `test_quality_stateio.py` |
| Network | Independent controller→source, controller→target, and target→source replication paths; dropping existing TCP sessions, rejecting new connections, latency, stalls, lost responses after SQL has actually executed, and offline abort/cleanup | `tests/support/wire.py`, `test_quality_network.py` |
| Full migration under load | Installed CLI, guaranteed overlap between DML and initial COPY, continuous DML+DDL, background validation, controller restarts, replication interruption, finalize/commit, and an independent target | `tests/support/workload.py`, `test_quality_cli.py` |
| Generated sequences | Hypothesis state machine, a Python model of expected rows, DML around DDL in one transaction, rollback, index queue, pause/resume, controller reconstruction, and resync; comparison after every step | `test_quality_generated.py`, `tests/fixtures/quality/` |

The oracle does not import production validation/fingerprint code or rely solely
on row counts or aggregate checksums. It reads rows in small batches and reports
the table and position of a mismatch without exposing row contents. This is a
separate test mechanism; the utility still computes aggregates on the server.
A target materialized view is compared with the result of its defining query on
the source: finalize refreshes the target, while stored source rows may be stale.
Nested materialized views with stale dependencies require a separate oracle for
the expected result; the general comparator does not compute recursive refreshes.
Deliberate target corruption verifies that the oracle detects differences.
Verifiers are compared using an in-memory hash and omitted from diagnostic messages.

`test_quality_ddl_validation.py` controls catalog and aggregate read boundaries
and executes real captured DDL at those points. It checks column rename/drop,
table drop, cursor invalidation when the original schema is restored, and
preservation of BLOCKED for corruption confined to the target. These regressions
prevent false schema errors during background validation; SQL errors are not
hidden without a confirmed change to the DDL journal.

Process interruption points are instrumented only in the test child process;
the main library contains no fault hooks. PostgreSQL, SQL, transactions, locks,
COPY, and network connections are real. The original plan is retried, and final
data and resource cleanup are verified. File tests use actual SIGKILL and kernel
RLIMIT_FSIZE; ENOSPC is injected at the fsync boundary. This does not test actual
disk exhaustion, power loss, or storage controller behavior.

## Running from a clean wheel

Install development tools:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
```

Quick run of the new checks on PG18:

```sh
.venv/bin/python infra/quality.py --profile quick --pg-stand /path/to/pg-stand --seed 12345
```

Full regression suite and new checks on PG15, PG16, PG17, and PG18:

```sh
.venv/bin/python infra/quality.py --profile matrix --pg-stand /path/to/pg-stand --seed 12345
```

After moving the directory, add `--existing-containers` if needed. Before
start/stop, the script verifies ownership labels and that bind mounts resolve to
this project's data. `--current-major` must match the active profile. To select
specific versions, pass one or more `--major` options.

Each run builds a wheel from a separate source directory, installs it with test
dependencies into a new venv, verifies the import path, and runs pytest with that
Python interpreter. The child CLI runs from `/tmp` without PYTHONPATH. This tests
the installed package, including SQL resources. `report.json` records the seed,
versions, command, and SHA-256 hashes of the wheel and source manifest; `tests.log`
and JUnit contain the results.

All new artifacts are stored in a private directory,
`.runtime/quality-runs/<run>/`, already excluded through `.gitignore`. Reports from
individual runs, secrets, dumps, logs, and local incident accounts are not included
in the source repository. The pytest directory also contains credentials and test
migration state and must not be published in full. A reviewed report and JUnit
output are sufficient for sharing results.

## Sustained workload

```sh
.venv/bin/python infra/quality.py --profile soak --pg-stand /path/to/pg-stand \
  --major 18 --seconds 1800 --rows 10000 --writers 4
```

Defaults: 30 minutes after reaching REPLICATING, 10,000 initial rows with payloads
of about 2 KiB, and four transactional writers. Increase `--seconds` and `--rows`
for an overnight profile; the writer count is limited to eight. A short 30-second
variant is included in the regular matrix. Soak/replay are excluded from ordinary
pytest runs by default; they are explicit profiles rather than silently skipped
checks.

The test holds a SHARE lock on the target table until source transactions are
confirmed and tablesync is confirmed to be unfinished. It then checks the balance
and transaction count against independent counters, the DDL sequence, unlogged
tables/large objects, finalize, and commit. After commit, it verifies independent
writes on the target and the absence of the subscription/slot. Process RSS, lag,
and retained WAL are sampled once per second into `soak/samples.jsonl`; the outcome,
DML/DDL counts, and actual duration are recorded in `soak/summary.json`.
Metrics are used for analysis; there are no universal memory/WAL thresholds because
these depend on dataset size and the test environment.

## Replaying generated sequences

`--examples` and `--steps` control the number of examples and steps. Hypothesis
shrinks a failing sequence and prints a reproducer; each example saves
`generated-*/sequence.json`. The seed is recorded in the report. Reusing a seed
requires the same Python/Hypothesis versions and source code; whether a failure
occurs also depends on the external PostgreSQL instance. To replay an exact list
of operations, use the saved JSON:

```sh
.venv/bin/python infra/quality.py --profile replay --pg-stand /path/to/pg-stand \
  --major 18 --sequence .runtime/quality-runs/RUN/generated-EXAMPLE/sequence.json
```

When a defect is found, remove any secrets from the minimal scenario, then add it
to `tests/fixtures/quality/` and a permanent regression test. The existing
`recovery.json` is a deterministic rollback/DDL/recovery scenario that tests the
replay path itself; it is not a recording of an incident in a user's environment.

## Scope of the evidence

Passing tests confirms the exercised scenarios on the specified versions and
sizes. A same-major matrix does not validate migrations between major versions.
Sequence shrinking does not replace testing every DDL, object, and type category.
Local TCP faults do not cover DNS, TLS, primary changes, or a physical node reboot;
a separate earlier test restarts the logical container. Multi-terabyte datasets,
WAL disk exhaustion, and all SPEC variants remain unqualified. New checks do not
automatically expand the utility's publicly stated support.
