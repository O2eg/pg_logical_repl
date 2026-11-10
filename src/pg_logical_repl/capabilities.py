"""Machine-readable implemented surface; planned features are explicit."""


def describe():
    commands = {}
    for name in [
        "validate",
        "inspect-config",
        "check",
        "plan",
        "status",
        "events",
        "verify",
        "report",
        "ddl check",
        "ddl log",
        "ddl queue list",
    ]:
        commands[name] = dict(
            mutates_source=False,
            mutates_target=False,
            mutates_global_roles=False,
            requires_plan=False,
            machine_output=True,
        )
    for name in [
        "prepare",
        "start",
        "abort",
        "cutover finalize",
        "cutover commit",
        "ddl exec",
        "ddl queue skip",
        "pause",
        "resume",
        "resolve",
        "resync-table",
        "cleanup",
    ]:
        commands[name] = dict(
            mutates_source=True,
            mutates_target=True,
            mutates_global_roles=name in ["prepare", "abort", "cutover commit", "cleanup"],
            requires_plan=True,
            machine_output=True,
        )
    commands["run"] = dict(
        mutates_source=True,
        mutates_target=True,
        mutates_global_roles=False,
        requires_plan="authorization persisted by start",
        machine_output=True,
        long_running=True,
    )
    commands["cutover abort"] = dict(
        mutates_source=True,
        mutates_target=True,
        mutates_global_roles=False,
        requires_plan=False,
        requires_reason=True,
        machine_output=True,
    )
    commands["ddl queue retry"] = dict(
        mutates_source=True,
        mutates_target=True,
        mutates_global_roles=False,
        requires_plan=False,
        machine_output=True,
    )
    return dict(
        schema_version="pg_logical_connector/capabilities-v2",
        maturity="development",
        same_major_versions=[15, 16, 17, 18],
        data_plane="PostgreSQL pgoutput/publication/subscription",
        validation=dict(
            execution="postgresql_aggregates",
            levels=["counts", "checksums", "full"],
            primary_key_chunks=True,
            composite_primary_keys=True,
            replication_lsn_wait=True,
            payload_rows_returned=False,
            continuous_background=True,
            resumable_background_cursor=True,
            incremental_cold_chunk_cache=False,
        ),
        commands=commands,
        integrity=dict(
            roles=True,
            password_verifiers=True,
            membership=True,
            schema=True,
            installed_resources=True,
            journal=True,
            baseline_required=True,
        ),
        recovery=dict(
            planned=True,
            pause_requires_wal_budget=True,
            resume_requires_validation=True,
            resync_profile="ordinary logged tables; preserves referencing foreign keys",
            source_offline_abort=True,
            source_offline_finalized_detach=True,
            deferred_cleanup_requires_plan=True,
        ),
        metrics="prometheus_textfile",
        ddl_profile=[
            "logged tables/partitions, CTAS structure + native tablesync, automatic publication refresh/READY",
            "columns, deterministic type changes/defaults, bounded rewrites and SET NOT NULL",
            "CHECK/FK NOT VALID + queued VALIDATE; UNIQUE/PK concurrent index + constraint attachment",
            "ordinary and partitioned index queues, durable intents, retries, planned/evidenced skip",
            "views/materialized-view definitions, trusted functions/procedures, application triggers",
            "sequence DDL (except RESTART), enum/domain, ownership, ACL/default privileges and comments",
        ],
        ddl_queue=dict(
            order="serialized",
            retry_limit=True,
            partition_index_tree=True,
            concurrent_source_reconciliation=True,
            skip_requires_evidence=True,
        ),
        sql_resource_version=2,
        ddl_restrictions=[
            "one top-level command and one ALTER action; no DO/CASCADE/global/extension DDL",
            "function calls in ADD COLUMN defaults/USING are conservatively rejected",
            "pending queue jobs block subsequent source DDL; DML continues",
            "custom object classes outside coverage remain blocked; no automatic SQL resource upgrade",
        ],
        not_implemented=[
            "takeover",
            "doctor bundle",
            "incremental cold-chunk cache",
            "partition-tree resync",
            "automatic failover",
            "full SPEC v2 object coverage",
        ],
    )
