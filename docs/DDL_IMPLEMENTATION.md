# DDL and queue implementation

Development and testing use a local pg-stand environment. No cloud endpoints were
used. The SQL resource version is **2**.

## Statement flow

```text
application / ddl exec
        |
        v
source: guard + DDL + lc.ddl_log journal
        |                  one source transaction
        v
pgoutput -> subscription/apply -> ENABLE REPLICA trigger
                                  |             |
                                  |             +--> lc.ddl_queue
                                  v                       |
                           DDL in the replay        Python run
                           transaction              separate statements
                                                    outside the apply transaction
```

The journal contains the version, XID, command tag, original SQL, session context,
synthesized actions, queue jobs, and SHA-256 of the entire executable payload.
The `lc.ddl_applied` receipt is written in the same target transaction. Rolling
back the source DDL also rolls back the journal and publication change.

`managed_only` accepts changes through `ddl exec` with the exact SQL and plan hash;
`captured` also accepts direct application DDL. In the current profile, both modes
use source event triggers and require superuser access during preparation.
A dedicated NOLOGIN/NOSUPERUSER role executes DDL in the target stream. The queue
runs through the sidecar's administrative connection with configured timeouts.

## Coverage

| Class | Implementation |
|---|---|
| CREATE/DROP/RENAME TABLE, new partition | Structure applied in the stream; publication and tablesync with READY tracking |
| CTAS / SELECT INTO | Structure synthesized from the catalog; SELECT is not executed on the target |
| ATTACH / DETACH PARTITION | DDL in the stream + REFRESH; a detached table remains in the publication |
| ADD/DROP/RENAME COLUMN | Applied in the stream; ADD allows literal DEFAULT values, with type and constraint checks |
| ALTER TYPE, DEFAULT, NULL, storage/statistics | Applied in the stream; rewrite and SET NOT NULL operations have size limits |
| CHECK / FOREIGN KEY | NOT VALID first, then queued VALIDATE; an original NOT VALID flag is preserved |
| UNIQUE / PRIMARY KEY | CREATE UNIQUE INDEX CONCURRENTLY, then dependent constraint attachment; USING INDEX preserves the original name until attachment |
| Constraints on a partitioned root | CHECK uses NOT VALID/VALIDATE; UNIQUE/PK/FK are applied in the stream with size limits because PostgreSQL 15–18 does not support USING INDEX or NOT VALID FK on the root |
| EXCLUDE | Applied in the stream with size limits; PostgreSQL does not allow attaching an existing index as EXCLUDE |
| Ordinary index | Queued CREATE/DROP CONCURRENTLY; an INVALID index after a failure is removed only after intent/definition checks |
| Partitioned table index | ON ONLY parents, CONCURRENTLY leaf indexes, attachment from leaves to root; parent removal uses a separate ordinary DROP INDEX |
| Views / functions / procedures | Definitions applied in the stream; function languages must be trusted |
| Application triggers | Creation, alteration, removal, ordinary ENABLE/DISABLE; REPLICA/ALWAYS modes are prohibited |
| Materialized views | Definitions created WITH NO DATA; populated at cutover |
| Sequences | Definitions applied in the stream except RESTART; values are synchronized before ALTER so new bounds do not conflict with the old target counter |
| Enum / domain | Creation/alteration; new enum values become available after the source commit |
| Owners, ACLs, DEFAULT ACLs, comments | Transferred in the stream; new objects receive the source owners and ACLs |

Comments, strings containing `;`, dollar-quoted function bodies, and quoted
identifiers are handled lexically. A delimiter inside a string is not treated as
a second statement. `ddl exec` uses pglast. The source guard also checks the catalog.

Partition index creation follows the [PostgreSQL ON ONLY / CONCURRENTLY / ATTACH procedure](https://www.postgresql.org/docs/18/ddl-partitioning.html).

## New tables

1. The source creates the object, adds an ordinary table to its explicit publication,
   and writes the journal. A new partition of a published root is included automatically.
2. Apply creates the target structure, adds a refresh job, and inserts an
   `lc.table_sync` record with status `pending_refresh`.
3. The daemon runs `REFRESH PUBLICATION WITH (copy_data=true)` outside a transaction.
4. PostgreSQL copies the snapshot and catches up with changes; the status becomes
   `copying`, then `ready` once `pg_subscription_rel.srsubstate='r'`.

This uses native [REFRESH PUBLICATION](https://www.postgresql.org/docs/18/sql-altersubscription.html).
The initial copy covers changes made before the table was connected. CTAS does not
execute its SELECT a second time. Retrying REFRESH after a lost response does not
restart copying for tables that are already included.

Progress is bound to the target OID: RENAME/SET SCHEMA before REFRESH does not make
an unfinished table copy appear to have been dropped.

`status` exposes `new_tables`, `ddl_queue`, and `pending_refresh_age_seconds`.
Finalize/commit are prohibited while the queue or tablesync is unfinished.
By default, creating a table without a primary key is rejected; an explicit policy
can be configured:

```yaml
ddl:
  mode: captured
  new_tables_without_pk: replica_identity_full  # default: reject
  rewrite_max_table_bytes: 67108864
  set_not_null_max_table_bytes: 67108864
daemon:
  queue_max_attempts: 3
```

## Ordering and recovery

- Source DDL transactions are serialized using an advisory transaction lock:
  sequence IDs alone do not guarantee commit order. Ordinary DML does not take
  this lock.
- While a queue job is pending, further source DDL is rejected with a pending
  queue message. This is a deliberate limitation of the current implementation:
  it prevents operations from overtaking dependencies or exposing structure that
  has not yet been replayed during REFRESH. DML continues to replicate.
- After a statement that creates an index/validation job, another DDL statement
  cannot run in the same transaction: such a source transaction is prevented
  from committing. Otherwise, apply would wait for its own uncommitted job.
  Multiple CREATE TABLE statements with refresh jobs in one transaction are allowed.
- The queue runs sequentially; `max_parallel_index_builds` is an upper bound, and
  the current implementation uses one worker. Parallel builds are not supported.
- An intent is persisted before a nontransactional action. After a lost response,
  the index OID, definition, and validity are checked; unrelated objects are not
  deleted. Source CONCURRENTLY operations through `ddl exec` also save a separate
  intent and recover a missing journal record.
- Exhausting retries moves the migration to BLOCKED. `retry` returns it to the
  previous state only if the queue was the cause of BLOCKED.
- `setval` is not transactional. A presynchronized sequence value is not final:
  applications remain disconnected from the target, and exact synchronization
  of `last_value/is_called` occurs at cutover.

```sh
pg-logical-repl --machine ddl queue list
pg-logical-repl --machine ddl queue retry 42
```

Skipping requires JSON evidence and a plan. Example `skip.json`:

```json
{
  "reason": "The index will be created manually before cutover",
  "approved_by": "DBA",
  "verification": "Verified the definition, absence of replica identity/constraint dependencies, and recovery plan"
}
```

```sh
pg-logical-repl --machine plan ddl-queue-skip --job-id 42 --evidence skip.json --out skip-plan.json
pg-logical-repl --machine ddl queue skip 42 --evidence skip.json --plan skip-plan.json --plan-hash HASH_FROM_PLAN
```

The plan binds the exact job and the evidence hash. REFRESH, constraint attachment,
and indexes required by subsequent constraints cannot be skipped. A skip is
recorded as an intervention. It does not bypass schema validation: a missing index
or an unvalidated constraint still blocks successful cutover.

## Current limitations

ON DELETE/UPDATE CASCADE on foreign keys is allowed: native replication applies
the cascading data changes.

The full SPEC v2 is not yet implemented. The guard explicitly rejects:

- DO and compound top-level statements, multiple ALTER TABLE actions;
- DROP … CASCADE, global role/database DDL, changes to extensions and their members;
- function calls in ADD COLUMN DEFAULT/USING, including potentially immutable
  functions: the current policy is conservative rejection rather than incomplete
  volatility checks;
- operations exceeding rewrite/SET NOT NULL thresholds, sequence RESTART;
- temporary or new unlogged tables, traditional inheritance, concurrent detach;
- classes outside current object coverage: RLS, rules, foreign objects, nonstandard
  types/operators/casts/access methods/tablespaces/security labels, and other
  categories already rejected by preflight.

Further work is needed for supported DO/compound statement synthesis, exhaustive
`sql_drop` metadata, complete volatility/rewrite qualification, SQL resource
upgrades in active migrations, a general drift controller, and production workloads.

Version coverage and validation results: [IMPLEMENTATION_STATUS.md](../IMPLEMENTATION_STATUS.md).
Test commands and local JUnit report locations: [TESTING.md](TESTING.md).
Test datasets are small; a local run does not measure production performance.
