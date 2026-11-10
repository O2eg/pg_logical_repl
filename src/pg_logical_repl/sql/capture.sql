CREATE FUNCTION lc.capture() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $body$
DECLARE
  statement text:=current_query(); code text; cmd record; rel record; con record; obj record; aclrow record;
  actions jsonb:='[]'; presync jsonb:='[]'; owners jsonb:='[]'; acls jsonb:='[]'; jobs jsonb:='[]'; additions jsonb:='[]';
  opts jsonb; pub text; before_rel jsonb; before_con jsonb; definition text; replay text;
  target text; object_kind text; object_owner text; object_acl aclitem[]; aclkind "char";
  action text; index_name text; id bigint; is_new boolean;
  token boolean; prior_internal text:=current_setting('lc.internal',true); affected oid[]:='{}';
BEGIN
  token:=current_setting('lc.ownership_token',true) IS NOT DISTINCT FROM (SELECT ownership_token FROM lc.control LIMIT 1);
  IF current_setting('lc.internal',true) IS NOT DISTINCT FROM (SELECT ownership_token FROM lc.control LIMIT 1) OR (token AND current_setting('lc.managed',true) IS DISTINCT FROM 'on') THEN RETURN; END IF;
  IF NOT EXISTS(SELECT FROM lc.control WHERE state='REPLICATING' AND
      (ddl_mode='captured' OR (ddl_mode='managed_only' AND token))) THEN RETURN; END IF;
  code:=lc.clean_sql(statement);
  SELECT settings,'lc_pub_'||migration_id INTO opts,pub FROM lc.control LIMIT 1;
  before_rel:=current_setting('lc.before_relations')::jsonb;
  before_con:=current_setting('lc.before_constraints')::jsonb;
  replay:=statement;
  IF TG_TAG='ALTER DEFAULT PRIVILEGES' AND code !~ ' FOR (ROLE|USER) ' THEN
    replay:=regexp_replace(statement,'(?i)^\s*ALTER\s+DEFAULT\s+PRIVILEGES\s+',
      format('ALTER DEFAULT PRIVILEGES FOR ROLE %I ',current_setting('lc.caller_role')));
  END IF;
  IF NULLIF(current_setting('lc.affected_relation',true),'') IS NOT NULL THEN
    affected:=array_append(affected,current_setting('lc.affected_relation')::oid);
  END IF;
  PERFORM set_config('lc.internal',(SELECT ownership_token FROM lc.control LIMIT 1),true);
  FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    IF cmd.in_extension OR EXISTS(SELECT FROM pg_depend WHERE classid=cmd.classid AND objid=cmd.objid AND deptype='e') THEN RAISE EXCEPTION 'logical_connector: extension member DDL is frozen' USING ERRCODE='LC001'; END IF;
    IF cmd.schema_name='lc' OR cmd.schema_name ~ '^pg_' THEN
      RAISE EXCEPTION 'logical_connector: system/temporary DDL is outside the application profile' USING ERRCODE='LC001';
    END IF;
    object_kind:=NULL; target:=NULL; object_owner:=NULL; object_acl:=NULL; aclkind:=NULL;
    IF cmd.classid='pg_class'::regclass THEN
      SELECT c.*,n.nspname,pg_get_userbyid(c.relowner) AS owner INTO rel FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=cmd.objid;
      IF NOT FOUND THEN CONTINUE; END IF;
      IF EXISTS(SELECT FROM pg_depend WHERE classid=cmd.classid AND objid=cmd.objid AND deptype='e') THEN
        RAISE EXCEPTION 'logical_connector: extension members are frozen' USING ERRCODE='LC001';
      END IF;
      is_new:=NOT before_rel @> to_jsonb(cmd.objid::bigint);
      target:=format('%I.%I',rel.nspname,rel.relname); object_owner:=rel.owner;
      IF rel.relkind IN ('r','p') THEN
        IF NOT cmd.objid=ANY(affected) THEN affected:=array_append(affected,cmd.objid); END IF;
        IF rel.relpersistence<>'p' OR (rel.relkind='r' AND rel.relam<>(SELECT oid FROM pg_am WHERE amname='heap')) OR (NOT rel.relispartition AND EXISTS(SELECT FROM pg_inherits WHERE inhrelid=cmd.objid)) THEN
          RAISE EXCEPTION 'logical_connector: new/altered tables must be logged, without classical inheritance' USING ERRCODE='LC001';
        END IF;
        object_kind:='TABLE'; aclkind:='r'; object_acl:=rel.relacl;
        IF is_new THEN
          additions:=additions||jsonb_build_array(jsonb_build_object('schema',rel.nspname,'name',rel.relname,'kind',rel.relkind));
          IF TG_TAG IN ('CREATE TABLE AS','SELECT INTO') THEN
            SELECT string_agg(format('%I %s%s',a.attname,format_type(a.atttypid,a.atttypmod),
              CASE WHEN a.attcollation<>0 THEN ' COLLATE '||(SELECT format('%I.%I',n.nspname,c.collname)
                FROM pg_collation c JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.oid=a.attcollation) ELSE '' END),',' ORDER BY a.attnum)
              INTO definition FROM pg_attribute a WHERE a.attrelid=cmd.objid AND a.attnum>0 AND NOT a.attisdropped;
            replay:=format('CREATE TABLE %s (%s)%s',target,definition,
              CASE WHEN rel.reloptions IS NOT NULL THEN ' WITH ('||(SELECT string_agg(format('%I=%L',option_name,option_value),',') FROM pg_options_to_table(rel.reloptions))||')' ELSE '' END);
          END IF;
          IF NOT rel.relispartition AND NOT EXISTS(SELECT FROM pg_publication_rel WHERE prpubid=(SELECT oid FROM pg_publication WHERE pubname=pub) AND prrelid=cmd.objid) THEN
            EXECUTE format('ALTER PUBLICATION %I ADD TABLE %s',pub,target);
          END IF;
        END IF;
      ELSIF rel.relkind IN ('i','I') AND TG_TAG='CREATE INDEX' THEN
        IF rel.relkind='I' THEN
          IF NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=cmd.objid) THEN
            SELECT jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind,
              'definition',pg_get_indexdef(c.oid),'depth',tree.depth,'valid',(SELECT indisvalid FROM pg_index WHERE indexrelid=c.oid),
              'parent_schema',pn.nspname,'parent_name',pc.relname,'created',NOT before_rel @> to_jsonb(c.oid::bigint)) ORDER BY tree.depth,n.nspname,c.relname)
              INTO obj FROM (WITH RECURSIVE t AS (SELECT cmd.objid AS oid,NULL::oid AS parent,0 AS depth
                UNION ALL SELECT i.inhrelid,i.inhparent,t.depth+1 FROM t JOIN pg_inherits i ON i.inhparent=t.oid) SELECT * FROM t) tree
              JOIN pg_class c ON c.oid=tree.oid JOIN pg_namespace n ON n.oid=c.relnamespace
              LEFT JOIN pg_class pc ON pc.oid=tree.parent LEFT JOIN pg_namespace pn ON pn.oid=pc.relnamespace;
            jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','create_partitioned_index','statement',pg_get_indexdef(cmd.objid),'nodes',obj.jsonb_agg));
          END IF;
        ELSIF NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=cmd.objid) THEN
          definition:=pg_get_indexdef(cmd.objid);
          action:=regexp_replace(definition,'^CREATE (UNIQUE )?INDEX ',E'CREATE \\1INDEX CONCURRENTLY ');
          jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','create_index','statement',action,'schema',rel.nspname,'name',rel.relname,'definition',definition));
        END IF;
        replay:=NULL;
      ELSIF rel.relkind='S' THEN
        object_kind:='SEQUENCE'; aclkind:='s'; object_acl:=rel.relacl;
        IF TG_TAG='ALTER SEQUENCE' THEN
          EXECUTE format('SELECT last_value,is_called FROM %s',target) INTO obj;
          presync:=presync||jsonb_build_array(format('SELECT pg_catalog.setval(%L::regclass,%s,%L)',target,obj.last_value,obj.is_called));
        END IF;
      ELSIF rel.relkind='v' THEN object_kind:='VIEW'; aclkind:='r'; object_acl:=rel.relacl;
      ELSIF rel.relkind='m' THEN
        object_kind:='MATERIALIZED VIEW'; aclkind:='r'; object_acl:=rel.relacl;
        IF TG_TAG='CREATE MATERIALIZED VIEW' THEN replay:=format('CREATE MATERIALIZED VIEW %s%s AS %s WITH NO DATA',target,
          CASE WHEN rel.reloptions IS NOT NULL THEN ' WITH ('||(SELECT string_agg(format('%I=%L',option_name,option_value),',') FROM pg_options_to_table(rel.reloptions))||')' ELSE '' END,
          regexp_replace(pg_get_viewdef(cmd.objid,true),';[[:space:]]*$','')); END IF;
      END IF;
      -- Correct ownership for objects implicitly created by SERIAL/IDENTITY too.
      IF object_kind IS NOT NULL AND (is_new OR TG_TAG LIKE 'CREATE%') AND NOT (rel.relkind='S' AND EXISTS(SELECT FROM pg_depend WHERE classid='pg_class'::regclass AND objid=cmd.objid AND deptype IN ('a','i'))) THEN
        owners:=owners||jsonb_build_array(format('ALTER %s %s OWNER TO %I',object_kind,target,object_owner));
      END IF;
    ELSIF cmd.classid='pg_proc'::regclass AND TG_TAG IN ('CREATE FUNCTION','CREATE PROCEDURE') THEN
      SELECT p.*,n.nspname,l.lanpltrusted,pg_get_userbyid(p.proowner) AS owner INTO obj
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=cmd.objid;
      IF NOT obj.lanpltrusted THEN RAISE EXCEPTION 'logical_connector: untrusted function language is unsupported' USING ERRCODE='LC001'; END IF;
      target:=format('%I.%I(%s)',obj.nspname,obj.proname,pg_get_function_identity_arguments(cmd.objid));
      object_kind:=CASE WHEN obj.prokind='p' THEN 'PROCEDURE' ELSE 'FUNCTION' END;
      object_owner:=obj.owner; object_acl:=obj.proacl; aclkind:='f';
      replay:=pg_get_functiondef(cmd.objid);
      owners:=owners||jsonb_build_array(format('ALTER %s %s OWNER TO %I',object_kind,target,object_owner));
    ELSIF cmd.classid='pg_namespace'::regclass AND TG_TAG='CREATE SCHEMA' THEN
      SELECT nspname,pg_get_userbyid(nspowner) AS owner,nspacl INTO obj FROM pg_namespace WHERE oid=cmd.objid;
      object_kind:='SCHEMA'; target:=format('%I',obj.nspname); object_owner:=obj.owner; object_acl:=obj.nspacl; aclkind:='n';
      owners:=owners||jsonb_build_array(format('ALTER SCHEMA %s OWNER TO %I',target,object_owner));
    ELSIF cmd.classid='pg_type'::regclass AND TG_TAG IN ('CREATE TYPE','CREATE DOMAIN') THEN
      SELECT t.*,n.nspname,pg_get_userbyid(t.typowner) AS owner INTO obj FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE t.oid=cmd.objid;
      IF obj.typtype NOT IN ('d','e') THEN RAISE EXCEPTION 'logical_connector: only enum/domain types are qualified' USING ERRCODE='LC001'; END IF;
      object_kind:='TYPE'; target:=format('%I.%I',obj.nspname,obj.typname); object_owner:=obj.owner; object_acl:=obj.typacl; aclkind:='T';
      owners:=owners||jsonb_build_array(format('ALTER TYPE %s OWNER TO %I',target,object_owner));
    END IF;
    IF object_kind IS NOT NULL AND aclkind IS NOT NULL AND TG_TAG LIKE 'CREATE%' THEN
      acls:=acls||jsonb_build_array(format('REVOKE ALL ON %s %s FROM PUBLIC',CASE WHEN object_kind IN ('VIEW','MATERIALIZED VIEW') THEN 'TABLE' ELSE object_kind END,target));
      FOR aclrow IN SELECT * FROM aclexplode(COALESCE(object_acl,acldefault(aclkind,(SELECT oid FROM pg_roles WHERE rolname=object_owner)))) LOOP
        IF aclrow.grantee=(SELECT oid FROM pg_roles WHERE rolname=object_owner) THEN CONTINUE; END IF;
        acls:=acls||jsonb_build_array(format('GRANT %s ON %s %s TO %s%s',aclrow.privilege_type,
          CASE WHEN object_kind IN ('VIEW','MATERIALIZED VIEW') THEN 'TABLE' ELSE object_kind END,target,
          CASE WHEN aclrow.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(aclrow.grantee)) END,
          CASE WHEN aclrow.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
      END LOOP;
    END IF;
  END LOOP;
  -- Reject loss of replica identity; new tables may opt in to REPLICA IDENTITY FULL.
  FOREACH id IN ARRAY affected LOOP
    IF NOT EXISTS(SELECT FROM pg_class c WHERE c.oid=id AND (c.relreplident='f' OR EXISTS(SELECT FROM pg_index i
      WHERE i.indrelid=c.oid AND i.indisvalid AND ((c.relreplident='d' AND i.indisprimary) OR (c.relreplident='i' AND i.indisreplident))))) THEN
      IF NOT before_rel @> to_jsonb(id) AND opts->>'new_tables_without_pk'='replica_identity_full' THEN
        SELECT format('%I.%I',n.nspname,c.relname) INTO target FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=id;
        action:=format('ALTER TABLE %s REPLICA IDENTITY FULL',target); EXECUTE action;
        owners:=owners||jsonb_build_array(action);
      ELSE RAISE EXCEPTION 'logical_connector: DDL would remove replica identity or new table has no PK; configure new_tables_without_pk=replica_identity_full' USING ERRCODE='LC001'; END IF;
    END IF;
    IF EXISTS(SELECT FROM pg_class WHERE oid=id AND relreplident='f') AND EXISTS(
      SELECT FROM pg_attribute a WHERE a.attrelid=id AND a.attnum>0 AND NOT a.attisdropped
      AND NOT EXISTS(SELECT FROM pg_opclass oc JOIN pg_am am ON am.oid=oc.opcmethod
        WHERE oc.opcdefault AND oc.opcintype=a.atttypid AND am.amname IN ('btree','hash'))) THEN
      RAISE EXCEPTION 'logical_connector: REPLICA IDENTITY FULL requires qualified equality types' USING ERRCODE='LC001';
    END IF;
  END LOOP;
  IF TG_TAG='ALTER TABLE' AND code ~ ' ADD (CONSTRAINT|CHECK|FOREIGN|UNIQUE|PRIMARY|EXCLUDE)( |\()' THEN
    -- Canonical constraint text avoids rewriting arbitrary SQL tokens.
    FOR con IN SELECT c.*,format('%I.%I',n.nspname,r.relname) AS target, n.nspname
      FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace
      WHERE c.conrelid=NULLIF(current_setting('lc.affected_relation',true),'')::oid AND NOT before_con @> to_jsonb(c.oid::bigint) LOOP
      definition:=regexp_replace(pg_get_constraintdef(con.oid,true),' NOT VALID$','');
      IF con.contype IN ('u','p','f') AND EXISTS(SELECT FROM pg_class WHERE oid=con.conrelid AND relkind='p') THEN
        -- PG15-18 cannot attach a prebuilt index constraint / NOT VALID FK on a
        -- partitioned root. Preserve PostgreSQL's recursive semantics under a size guard.
        replay:=statement;
      ELSIF con.contype IN ('c','f') THEN
        replay:=format('ALTER TABLE %s ADD CONSTRAINT %I %s NOT VALID',con.target,con.conname,definition);
        IF con.convalidated THEN jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','validate_constraint',
          'statement',format('ALTER TABLE %s VALIDATE CONSTRAINT %I',con.target,con.conname),'table',con.target,'name',con.conname)); END IF;
      ELSIF con.contype='x' THEN
        -- EXCLUDE cannot attach a prebuilt index; the guard bounds this in-stream build.
        replay:=statement;
      ELSIF con.contype IN ('u','p') THEN
        definition:=pg_get_indexdef(con.conindid);
        SELECT relname INTO index_name FROM pg_class WHERE oid=con.conindid;
        -- USING INDEX already has a prior create-index queue entry; do not rebuild it.
        IF code ~ ' USING INDEX ' THEN
          index_name:=(parse_ident((regexp_match(statement,'(?i)USING\s+INDEX\s+("(?:[^"]|"")+"|[a-zA-Z_][a-zA-Z_0-9$]*)'))[1],true))[1];
        ELSE
          action:=regexp_replace(definition,'^CREATE (UNIQUE )?INDEX ',E'CREATE \\1INDEX CONCURRENTLY ');
          jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','create_index','statement',action,'schema',con.nspname,'name',index_name,'definition',definition));
        END IF;
        jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','attach_constraint','statement',
          format('ALTER TABLE %s ADD CONSTRAINT %I %s USING INDEX %I%s',con.target,con.conname,
            CASE WHEN con.contype='p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,index_name,
            CASE WHEN con.condeferrable THEN ' DEFERRABLE'||CASE WHEN con.condeferred THEN ' INITIALLY DEFERRED' ELSE '' END ELSE '' END),
          'table',con.target,'name',con.conname));
        replay:=NULL;
      ELSE RAISE EXCEPTION 'logical_connector: this added constraint cannot be queued safely' USING ERRCODE='LC001'; END IF;
    END LOOP;
  ELSIF TG_TAG='ALTER TABLE' AND code ~ ' VALIDATE CONSTRAINT ' THEN
    jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','validate_constraint','statement',statement)); replay:=NULL;
  ELSIF TG_TAG='DROP INDEX' THEN
    IF current_setting('lc.drop_index_object',true)::jsonb IS DISTINCT FROM 'null'::jsonb THEN
      jobs:=jobs||jsonb_build_array(current_setting('lc.drop_index_object')::jsonb || jsonb_build_object(
        'kind',CASE WHEN current_setting('lc.drop_index_kind',true)='I' THEN 'drop_partitioned_index' ELSE 'drop_index' END,
        'statement',statement));
    END IF;
    replay:=NULL;
  END IF;
  IF replay IS NOT NULL THEN actions:=jsonb_build_array(replay); END IF;
  actions:=presync||actions||owners||acls;
  IF jsonb_array_length(additions)>0 OR TG_TAG='DROP TABLE' OR code ~ '(ATTACH|DETACH) PARTITION' THEN
    jobs:=jobs||jsonb_build_array(jsonb_build_object('kind','refresh_publication','statement','', 'tables',additions));
  END IF;
  IF code ~ 'DETACH PARTITION' THEN
    -- A detached leaf is now an ordinary table and must remain in full-DB replication.
    FOR rel IN SELECT c.oid,format('%I.%I',n.nspname,c.relname) AS target
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.relkind IN ('r','p') AND c.relpersistence='p' AND NOT c.relispartition
      AND n.nspname NOT IN ('lc','information_schema') AND n.nspname !~ '^pg_'
      AND NOT EXISTS(SELECT FROM pg_depend WHERE classid='pg_class'::regclass AND objid=c.oid AND deptype='e')
      AND NOT EXISTS(SELECT FROM pg_publication_rel WHERE prpubid=(SELECT oid FROM pg_publication WHERE pubname=pub) AND prrelid=c.oid) LOOP
        EXECUTE format('ALTER PUBLICATION %I ADD TABLE %s',pub,rel.target);
    END LOOP;
  END IF;
  -- Expected explicit membership changes with CREATE/DROP/RENAME, in this source transaction.
  UPDATE lc.operations SET details=jsonb_set(details,'{tables}',COALESCE((SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname) ORDER BY n.nspname,c.relname)
    FROM pg_publication_rel r JOIN pg_class c ON c.oid=r.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE r.prpubid=(SELECT oid FROM pg_publication WHERE pubname=pub)),'[]')) WHERE name='publication';
  PERFORM lc.record_ddl(statement,TG_TAG,actions,jobs);
  PERFORM set_config('lc.internal',COALESCE(prior_internal,''),true);
END
$body$;
REVOKE ALL ON FUNCTION lc.capture() FROM PUBLIC;
CREATE EVENT TRIGGER lc_capture ON ddl_command_end EXECUTE FUNCTION lc.capture();
ALTER EVENT TRIGGER lc_capture ENABLE ALWAYS;
