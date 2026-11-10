CREATE FUNCTION lc.replay() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE pair record; previous jsonb:='{}'; action text; job jsonb; item jsonb; seq int:=0; commands text[];
BEGIN
  IF NEW.migration_id IS DISTINCT FROM (SELECT migration_id FROM lc.control LIMIT 1) OR NEW.capture_version<>2 THEN
    RAISE EXCEPTION 'pg-logical-repl: journal migration/version mismatch';
  END IF;
  IF encode(sha256(convert_to(NEW.statement,'UTF8')),'hex')<>NEW.statement_sha256 OR
     encode(sha256(convert_to(jsonb_build_array(2,NEW.statement,NEW.context,NEW.actions,NEW.queue_jobs)::text,'UTF8')),'hex')<>NEW.payload_sha256 THEN
    RAISE EXCEPTION 'pg-logical-repl: journal checksum mismatch for %',NEW.id;
  END IF;
  IF EXISTS(SELECT FROM lc.ddl_applied WHERE id=NEW.id) THEN
    IF NOT EXISTS(SELECT FROM lc.ddl_applied WHERE id=NEW.id AND statement_sha256=NEW.statement_sha256) THEN
      RAISE EXCEPTION 'pg-logical-repl: journal identity conflict for %',NEW.id;
    END IF;
    RETURN NEW;
  END IF;
  IF EXISTS(SELECT FROM lc.ddl_queue WHERE kind<>'refresh_publication' AND status NOT IN ('done','skipped')) THEN
    RAISE EXCEPTION 'pg-logical-repl: prior DDL queue dependency is not ready' USING ERRCODE='LC002';
  END IF;
  SELECT array_agg(value) INTO commands FROM jsonb_array_elements_text(NEW.actions);
  FOR pair IN SELECT * FROM jsonb_each_text(NEW.context) LOOP
    IF pair.key NOT IN ('TimeZone','DateStyle','IntervalStyle','standard_conforming_strings','search_path','lock_timeout') THEN
      RAISE EXCEPTION 'pg-logical-repl: forbidden session setting';
    END IF;
    previous:=previous||pg_catalog.jsonb_build_object(pair.key,pg_catalog.current_setting(pair.key));
    PERFORM pg_catalog.set_config(pair.key,pair.value,true);
  END LOOP;
  FOREACH action IN ARRAY COALESCE(commands,ARRAY[]::text[]) LOOP EXECUTE action; END LOOP;
  FOR pair IN SELECT * FROM pg_catalog.jsonb_each_text(previous) LOOP PERFORM pg_catalog.set_config(pair.key,pair.value,true); END LOOP;
  FOR job IN SELECT jsonb_array_elements(NEW.queue_jobs) LOOP
    seq:=seq+1;
    INSERT INTO lc.ddl_queue(ddl_log_id,seq,kind,statement,details) VALUES(NEW.id,seq,job->>'kind',job->>'statement',job);
    IF job->>'kind'='refresh_publication' THEN
      FOR item IN SELECT jsonb_array_elements(job->'tables') LOOP
        INSERT INTO lc.table_sync(relation_oid,schema_name,table_name,ddl_log_id)
          VALUES(pg_catalog.to_regclass(pg_catalog.format('%I.%I',item->>'schema',item->>'name')),item->>'schema',item->>'name',NEW.id)
          ON CONFLICT(relation_oid) DO UPDATE SET schema_name=EXCLUDED.schema_name,table_name=EXCLUDED.table_name,ddl_log_id=EXCLUDED.ddl_log_id,state='pending_refresh',ready_at=NULL;
      END LOOP;
    END IF;
  END LOOP;
  INSERT INTO lc.ddl_applied(id,statement_sha256) VALUES(NEW.id,NEW.statement_sha256);
  RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION lc.replay() FROM PUBLIC;
CREATE TRIGGER lc_replay AFTER INSERT ON lc.ddl_log FOR EACH ROW EXECUTE FUNCTION lc.replay();
ALTER TABLE lc.ddl_log ENABLE REPLICA TRIGGER lc_replay;
