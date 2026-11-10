CREATE FUNCTION lc.guard_check(command_tag text, stmt text, caller_path text, caller_role text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $body$
DECLARE code text; token boolean; opts jsonb; rel oid; bytes bigint;
BEGIN
  token:=current_setting('lc.ownership_token',true) IS NOT DISTINCT FROM
    (SELECT ownership_token FROM lc.control LIMIT 1);
  IF current_setting('lc.internal',true) IS NOT DISTINCT FROM (SELECT ownership_token FROM lc.control LIMIT 1) THEN RETURN; END IF;
  IF token AND current_setting('lc.managed',true) IS DISTINCT FROM 'on' THEN RETURN; END IF;
  IF NOT EXISTS(SELECT FROM lc.control WHERE state='REPLICATING' AND
      (ddl_mode='captured' OR (ddl_mode='managed_only' AND token))) THEN
    RAISE EXCEPTION 'pg-logical-repl: DDL % is frozen; use pg-logical-repl ddl check',command_tag USING ERRCODE='LC001';
  END IF;
  -- Serialize DDL transactions: sequence IDs alone do not imply commit order.
  -- DML does not acquire this lock. Controller connections set a bounded lock_timeout.
  PERFORM pg_advisory_xact_lock(hashtextextended('logical_connector/ddl/'||current_database(),0));
  code:=lc.clean_sql(stmt);
  IF command_tag IN ('ALTER TABLE','DROP INDEX') AND lc.top_comma(code) THEN
    RAISE EXCEPTION 'pg-logical-repl: use one ALTER TABLE action per command' USING ERRCODE='LC001';
  END IF;
  IF command_tag NOT IN ('CREATE TABLE','CREATE TABLE AS','SELECT INTO','ALTER TABLE','DROP TABLE',
      'CREATE INDEX','ALTER INDEX','DROP INDEX','CREATE VIEW','ALTER VIEW','DROP VIEW',
      'CREATE MATERIALIZED VIEW','ALTER MATERIALIZED VIEW','DROP MATERIALIZED VIEW',
      'CREATE FUNCTION','ALTER FUNCTION','DROP FUNCTION','CREATE PROCEDURE','ALTER PROCEDURE','DROP PROCEDURE',
      'CREATE TRIGGER','ALTER TRIGGER','DROP TRIGGER','CREATE SEQUENCE','ALTER SEQUENCE','DROP SEQUENCE',
      'CREATE SCHEMA','ALTER SCHEMA','DROP SCHEMA','CREATE TYPE','ALTER TYPE','DROP TYPE',
      'CREATE DOMAIN','ALTER DOMAIN','DROP DOMAIN','COMMENT','GRANT','REVOKE','ALTER DEFAULT PRIVILEGES')
     OR code ~ '(^|[^A-Z_0-9])(RESTART|TABLESPACE|UNLOGGED|TEMPORARY|TEMP|EXTENSION|RULE|EVENT TRIGGER)([^A-Z_0-9]|$)'
     OR (code ~ ' CASCADE$' AND (command_tag LIKE 'DROP%' OR command_tag='REVOKE' OR (command_tag='ALTER TABLE' AND code ~ ' DROP ')))
     OR code ~ '(^|[^A-Z_0-9])"?LC"?([^A-Z_0-9]|$)'
     OR code ~ 'REPLICA IDENTITY NOTHING|ENABLE (ALWAYS|REPLICA) TRIGGER|DETACH PARTITION .* CONCURRENTLY'
     OR code ~ '^(GRANT|REVOKE|COMMENT ON|ALTER) (DATABASE|ROLE|TABLESPACE|PUBLICATION|SUBSCRIPTION)'
     OR code ~ '^CREATE (OR REPLACE )?FUNCTION .* LANGUAGE (C|INTERNAL)'
     OR code ~ '^CREATE TYPE .*\(' AND code !~ '^CREATE TYPE .* AS ENUM '
     OR (code ~ '^ALTER TABLE .* ADD (COLUMN )?[^ ]+ .* (PRIMARY KEY|UNIQUE|REFERENCES|CHECK|EXCLUDE)' AND code !~ ' ADD CONSTRAINT ')
     OR code ~ 'ROW LEVEL SECURITY|SET ACCESS METHOD|SET LOGGED|INHERIT |OF TYPE|NOT OF|SET SCHEMA LC'
     OR code ~ '^CREATE SCHEMA .* (CREATE|GRANT) ' THEN
    RAISE EXCEPTION 'pg-logical-repl: unsupported or unsafe DDL; use ddl check' USING ERRCODE='LC001';
  END IF;
  IF EXISTS(SELECT FROM lc.ddl_log d WHERE jsonb_array_length(d.queue_jobs)>0
      AND d.source_xid<>COALESCE(txid_current_if_assigned(),-1) AND NOT EXISTS(SELECT FROM lc.operations o WHERE o.name='ddl_done:'||d.id)) THEN
    RAISE EXCEPTION 'pg-logical-repl: preceding DDL queue is pending; run the controller and retry' USING ERRCODE='LC002';
  END IF;
  IF EXISTS(SELECT FROM lc.operations WHERE name LIKE 'ddl_concurrent:%'
      AND NOT (details->>'complete')::boolean
      AND details->>'plan_hash' IS DISTINCT FROM current_setting('lc.operation_key',true)) THEN
    RAISE EXCEPTION 'pg-logical-repl: incomplete concurrent source DDL; retry the original plan' USING ERRCODE='LC002';
  END IF;
  -- A queue-producing command must end its transaction. Otherwise the apply worker
  -- could wait for a job which is itself uncommitted in that same transaction.
  IF EXISTS(SELECT FROM lc.ddl_log d, LATERAL jsonb_array_elements(d.queue_jobs) j
       WHERE d.source_xid=COALESCE(txid_current_if_assigned(),-1) AND j->>'kind'<>'refresh_publication') THEN
    RAISE EXCEPTION 'pg-logical-repl: commit the preceding queued DDL before another DDL command' USING ERRCODE='LC001';
  END IF;
  PERFORM set_config('lc.caller_search_path',replace(caller_path,'"$user"',quote_ident(caller_role)),false);
  PERFORM set_config('lc.caller_role',caller_role,false);
  PERFORM set_config('lc.before_constraints',COALESCE((SELECT jsonb_agg(oid::bigint)::text FROM pg_constraint),'[]'),false);
  PERFORM set_config('lc.before_relations',COALESCE((SELECT jsonb_agg(oid::bigint)::text FROM pg_class),'[]'),false);
  -- Deterministic rewrite expressions only; no function invocations in ADD COLUMN
  -- defaults or USING. SET DEFAULT affects future source writes and is safe.
  IF command_tag='ALTER TABLE' AND (code ~ ' ADD (COLUMN )?' OR code ~ ' USING ') AND
     code ~ '(DEFAULT|USING) .*([A-Z_][A-Z_0-9]*[ ]*\(|CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME)' THEN
    RAISE EXCEPTION 'pg-logical-repl: expression must be deterministic; function defaults/USING require a separate migration' USING ERRCODE='LC001';
  END IF;
  IF command_tag='ALTER TABLE' THEN
    rel:=lc.resolve_relation((regexp_match(stmt,'(?i)^\s*ALTER\s+TABLE\s+(?:ONLY\s+)?((?:"(?:[^"]|"")+"|[a-zA-Z_][a-zA-Z_0-9$]*)(?:\.(?:"(?:[^"]|"")+"|[a-zA-Z_][a-zA-Z_0-9$]*))?)'))[1],replace(caller_path,'"$user"',quote_ident(caller_role)));
    PERFORM set_config('lc.affected_relation',COALESCE(rel::text,''),false);
  ELSE PERFORM set_config('lc.affected_relation','',false); END IF;
  IF command_tag='DROP INDEX' THEN
    rel:=lc.resolve_relation((regexp_match(stmt,'(?i)^\s*DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?((?:"(?:[^"]|"")+"|[a-zA-Z_][a-zA-Z_0-9$]*)(?:\.(?:"(?:[^"]|"")+"|[a-zA-Z_][a-zA-Z_0-9$]*))?)'))[1],replace(caller_path,'"$user"',quote_ident(caller_role)));
    PERFORM set_config('lc.drop_index_object',COALESCE((SELECT jsonb_build_object('schema',n.nspname,'name',c.relname,'definition',pg_get_indexdef(c.oid))::text
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=rel),'null'),false);
    PERFORM set_config('lc.drop_index_kind',COALESCE((SELECT relkind::text FROM pg_class WHERE oid=rel),''),false);
    IF EXISTS(SELECT FROM pg_index WHERE indexrelid=rel AND indisreplident) THEN
      RAISE EXCEPTION 'pg-logical-repl: DDL would remove replica identity index' USING ERRCODE='LC001';
    END IF;
  END IF;
  IF command_tag='ALTER TABLE' AND (code ~ 'SET NOT NULL' OR code ~ 'ADD CONSTRAINT .* EXCLUDE' OR (code ~ ' ADD (CONSTRAINT|CHECK|FOREIGN|UNIQUE|PRIMARY|EXCLUDE)' AND EXISTS(SELECT FROM pg_class WHERE oid=rel AND relkind='p'))) THEN
    SELECT settings INTO opts FROM lc.control LIMIT 1;
    SELECT COALESCE(sum(pg_total_relation_size(relid)),0) INTO bytes FROM pg_partition_tree(rel);
    IF greatest(COALESCE(bytes,0),COALESCE(pg_total_relation_size(rel),0)) > (opts->>'set_not_null_max_table_bytes')::bigint THEN
      RAISE EXCEPTION 'pg-logical-repl: constraint/SET NOT NULL exceeds the configured size limit' USING ERRCODE='LC001';
    END IF;
  END IF;
END
$body$;
REVOKE ALL ON FUNCTION lc.guard_check(text,text,text,text) FROM PUBLIC;
-- The wrapper has no search_path SET clause so it can record caller context.
-- Every function it invokes is schema-qualified; all actual work is in guard_check.
CREATE FUNCTION lc.guard() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  PERFORM lc.guard_check(TG_TAG,pg_catalog.current_query(),pg_catalog.current_setting('search_path'),
    COALESCE(NULLIF(pg_catalog.current_setting('role'),'none'),session_user));
END
$$;
REVOKE ALL ON FUNCTION lc.guard() FROM PUBLIC;
CREATE EVENT TRIGGER lc_guard ON ddl_command_start EXECUTE FUNCTION lc.guard();
ALTER EVENT TRIGGER lc_guard ENABLE ALWAYS;

CREATE FUNCTION lc.rewrite_guard() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE opts jsonb; rel oid:=pg_event_trigger_table_rewrite_oid();
BEGIN
  IF current_setting('lc.internal',true) IS NOT DISTINCT FROM (SELECT ownership_token FROM lc.control LIMIT 1) OR
     (current_setting('lc.ownership_token',true) IS NOT DISTINCT FROM (SELECT ownership_token FROM lc.control LIMIT 1)
      AND current_setting('lc.managed',true) IS DISTINCT FROM 'on') THEN RETURN; END IF;
  SELECT settings INTO opts FROM lc.control LIMIT 1;
  IF pg_event_trigger_table_rewrite_reason() & 2 <> 0 THEN
    RAISE EXCEPTION 'pg-logical-repl: volatile/default rewrite is not replayable' USING ERRCODE='LC001';
  END IF;
  IF pg_total_relation_size(rel)>(opts->>'rewrite_max_table_bytes')::bigint THEN
    RAISE EXCEPTION 'pg-logical-repl: table rewrite exceeds configured size limit' USING ERRCODE='LC001';
  END IF;
END
$$;
REVOKE ALL ON FUNCTION lc.rewrite_guard() FROM PUBLIC;
CREATE EVENT TRIGGER lc_rewrite_guard ON table_rewrite EXECUTE FUNCTION lc.rewrite_guard();
ALTER EVENT TRIGGER lc_rewrite_guard ENABLE ALWAYS;
