# Implementation status — 2026-11-10

The `pg-logical-repl` package, version `0.1.0a4`, is a working development alpha.

The pip package and CLI were renamed on 2026-11-10; Python imports use
`pg_logical_repl`. Configuration, state, and JSON contract formats retain
`pg_logical_connector/*` identifiers for format compatibility.

**The production readiness criteria for version 1.0 have not yet been met.**

Tests run in local pg-stand environments. Cloud and managed PostgreSQL services
require separate qualification.

## 0.1.0a4: concurrent DDL and background validation

Background validation discards a chunk if captured DDL changes the schema between
reading the catalog and computing the aggregate. The cursor now includes the DDL
journal version, so a sequence of changes that restores the original schema also
invalidates the old result. Corruption confined to the target still moves the
migration to BLOCKED. Deterministic regressions cover column rename/drop, table
drop, changes at both aggregate computation boundaries, and restoration of the
original schema.

## Extended testing

Added an independent exact oracle, actual CLI process interruptions at operation
boundaries, a TCP fault proxy on three paths, sustained CLI migration with DML/DDL,
and a Hypothesis state machine with replay of saved operations. The runner builds
and installs a clean wheel. Profiles, verified properties, and limitations are
documented in [TESTING.md](docs/TESTING.md). New run results are stored separately
in `.runtime/quality-runs/`, excluded through `.gitignore`.

## P0 in 0.1.0a3

- Accepted settings have implemented behavior or a fixed supported profile.
  `validate` rejects unsupported policies and parameters. Online validation,
  drift and sequence intervals, textfile metrics, lag/queue/refresh alerts,
  catchup stability, and ANALYZE at cutover are connected to runtime behavior.
- Continuous scans of primary key ranges persist the cursor, aggregate, and
  awaited LSN. Confirmed mismatches block migration; source changes are
  distinguished from mismatches. Waiting for apply does not stall the entire
  daemon iteration.
- Checks cover supported schema/ACLs, the initial role state, verifiers and
  membership, SQL resource identities and definitions, and DDL journal integrity.
  The baseline is created during prepare; role changes on both sides are detected.
  Verifiers are excluded from public reports. Pruning the acknowledged journal
  preserves compact managed DDL receipts and resumes after a failure.
- Planned pause/resume/resolve operations include a WAL budget and pause deadline,
  checks before leaving BLOCKED, whole-transaction skip by finish LSN, and
  applied-manually resolution. Skipping without restoring the complete transaction
  result leaves the migration BLOCKED.
- Resync of ordinary logged tables uses native tablesync and preserves incoming
  foreign keys, validation flags, and comments. The original plan resumes an
  interrupted step. Abort preserves partial data and restores removed foreign
  keys as NOT VALID, explicitly listing them in state. This repair does not
  support partitions or extension tables.
- Emergency abort and post-finalize commit with an unavailable source support
  target-only detachment, ABORT_CLEANUP_PENDING/DETACH_CLEANUP_PENDING states, and
  separate cleanup with ownership checks. The target is checked for changes and
  finalize evidence for freshness. Unknown additional slots are not deleted
  automatically.
- Partial status remains available when an endpoint is unavailable or the internal
  schema is damaged. Schema/permission errors during a daemon iteration persist
  a diagnosable BLOCKED state.

Commands, parameters, recovery, and limitations are documented in [README.md](README.md).
This version requires a new migration to record an integrity baseline. Automatic
adoption of old state, recovery from lost local state, takeover, and arbitrary
repair of damaged server resources are not implemented.

## 0.1.0a4 validation

The local PostgreSQL 15–18 matrix completed **417 successful test executions**, with
no failures, errors, or skipped tests. Each version passed 93 integration scenarios;
PG18 also passed 45 unit tests.

A separate 30-minute CLI run used 10,000 initial rows and four writers, executing
128,759 transactions and 892 DDL commands. It verified controller SIGKILL/SIGTERM,
a replication connection interruption, exact data and metadata comparisons,
finalize/commit, and independent writes on the target.

Ruff, formatting, wheel installation in a separate environment, both CLI entry
points, and SQL resources were checked. Instructions and the scope of these
results are documented in [TESTING.md](docs/TESTING.md). Machine reports, dumps,
credentials, and logs from individual runs remain local and are not included in
the repository.

## Implemented and verified in the test environment

- CLI and asynchronous service API; JSON `pg_logical_connector/result-v2`,
  capabilities, request ID, JSON events to stderr/file, private files, and redaction.
- Validation of configuration, identities, major versions, locale/collation,
  target emptiness, roles/verifiers, extensions, replica identity, and slot/disk
  capacity.
- Conservative preflight rejection of unsupported FDW, RLS, rules, extended
  statistics, custom collations/types/operators/casts/aggregates, text search,
  tablespaces, and security labels; a partial coverage manifest excludes external
  mapping secrets. Fingerprints include partition bounds/parents, column
  storage/collation, and sequence definitions. Unknown classes require further
  qualification.
- Basic health controller: identities, publication options/membership, subscription,
  slot, and WAL retention; BLOCKED on drift and repeated apply/sync errors within
  a 300-second window. These checks also run before finalize/commit. Conflicting
  data is not skipped automatically, and slots are not automatically deleted.
- RFC 8785/SHA-256 plans bound to configuration and catalogs; HMAC for verifiers.
- Server validation using count + MD5 sum, chunks based on single/composite primary
  keys, waiting for confirmed_flush_lsn, retries of changing chunks, concurrency
  and pause limits, identical GUC settings, and table/level selection. Python
  receives only aggregates and key boundaries. Large objects are checked using
  server page aggregates. Tables without primary keys require a full scan with a
  warning; cold chunk caching is not yet implemented.
- Transfer of roles, passwords, membership/options, settings, owners, ACLs, and the
  database comment. Existing roles that differ are not overwritten.
- Schema-only pg_dump/pg_restore with matching major versions, pre-data/indexes
  before loading, post-data after tablesync, and transactional step completion
  receipts.
- Native publication/slot/subscription, tablesync and DML, initial partitions,
  TOAST, RI FULL for tested types; a target → source connection probe without
  creating a slot.
- Extended DDL in captured/managed_only modes: new tables/partitions/CTAS, columns,
  types, DEFAULT, constraints, views/functions/procedures/triggers, sequences,
  enum/domain, owners, ACLs/default privileges, and comments. See the
  [matrix and limitations](docs/DDL_IMPLEMENTATION.md). Replica identity checks,
  rewrite/SET NOT NULL limits, and safe session context requirements remain enforced.
- New tables: publication changes in the source transaction, REFRESH with
  copy_data=true, native tablesync, and READY tracking; source data written before
  refresh is preserved.
- Durable sequential DDL queue: CONCURRENTLY, NOT VALID → VALIDATE, UNIQUE/PK via
  indexes, partitioned index trees, intents/OIDs, retries, and skip with a plan
  hash and evidence. An unfinished queue blocks cutover; skips are recorded as
  interventions and do not disable schema validation.
- Recovery of source CONCURRENTLY operations from the catalog and saved intent
  after loss of the final journal record or response. Further source DDL must wait
  for the queue to finish (receiving an explicit rejection/retry); DML continues.
- Finalize: external fence, SHARE locks, barrier, exact last_value/is_called,
  unlogged tables, large objects with OID/owner/ACL, materialized views in dependency
  order, and data validation.
- Commit rechecks the fence and changes since finalize, leaving an independent
  target. Cutover abort returns to replication before the irreversible boundary.
- Abort preserves application data and transferred roles while removing owned
  resources.
- Retrying prepare after a lost restore response, start after a lost CREATE
  SUBSCRIPTION response, and ddl exec after a lost acknowledgment.

## Remaining readiness gaps

1. The rest of the full DDL matrix: synthesis of supported DO/compound statements,
   exhaustive sql_drop metadata, qualification of volatility/casts, temporary and
   unlogged objects, and all SPEC variants. See DDL_IMPLEMENTATION.md for exact
   limitations.
2. Full object and dependency coverage: FDW/user mappings, extension configuration
   data, application event triggers, ALWAYS/REPLICA triggers, security labels,
   nonstandard types/casts/aggregates/tablespaces, and exhaustive comparison.
   Preflight blocks these known unsupported categories; a comprehensive object
   manifest and qualification of all properties remain incomplete. Full support
   for arbitrary databases is not claimed.
3. Cold chunk caching, incremental cutover, lag history, lease/takeover, extended
   observability, and operational qualification of the daemon.
4. Resync of partition trees; reconciliation of arbitrary partial states, unknown
   tablesync slots, lost state, and disk exhaustion.
5. Qualification of TLS/mTLS, RLS, complex grantor graphs, large transactions,
   parallel apply, failover, and SQL resource upgrades.
6. Not all SPEC options are implemented, including waivers and reconciliation of
   existing roles. Configuration validation rejects unsupported values; a full
   acceptance matrix covering every setting remains outstanding.
7. Qualification at production scale and with extended overnight profiles has not
   been completed. The local 30-minute run checks correctness on small datasets
   and does not replace testing with the intended production workload.

Next priorities: complete the object manifest, incremental validation, and remaining
DDL variants. Passing the current matrix does not mean the requirements above
have been met.

## Environment and artifacts

- The `infra/pg15.yaml` … `infra/pg18.yaml` environments run sequentially; PG18
  remains active after the matrix.
- Credentials are stored in `.pg_stand/`, results in `.runtime/`, and builds in
  `dist/`. These directories are excluded through `.gitignore`.
- Tests normalize bootstrap role verifiers only in their own environment; the
  utility does not change verifiers of existing roles on arbitrary targets.
- Matching major versions of `pg_dump`/`pg_restore` are required. PG16 has a helper
  image build script, `infra/build_pg16.py`.
- Tested environment: Python 3.12, Linux ARM64. A separate Python 3.11 run has not
  yet been completed.
- Guide: [README.md](README.md); example: [examples/local.yaml](examples/local.yaml).
