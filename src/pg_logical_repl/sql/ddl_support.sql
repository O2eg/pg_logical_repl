-- Shared, protected capture helpers. No untrusted procedural language is required.
CREATE FUNCTION lc.clean_sql(input text) RETURNS text
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $body$
DECLARE i int:=1; n int:=length(input); ch text; quote text; tag text; result text:=''; depth int;
        closed boolean; ended boolean:=false; escaped boolean;
BEGIN
  WHILE i<=n LOOP
    ch:=substr(input,i,1);
    IF ch ~ '[[:space:]]' THEN result:=result||' '; i:=i+1; CONTINUE; END IF;
    IF substr(input,i,2)='--' THEN
      WHILE i<=n AND substr(input,i,1)<>chr(10) LOOP i:=i+1; END LOOP;
      result:=result||' '; CONTINUE;
    END IF;
    IF substr(input,i,2)='/*' THEN
      depth:=1; i:=i+2;
      WHILE i<=n AND depth>0 LOOP
        IF substr(input,i,2)='/*' THEN depth:=depth+1; i:=i+2;
        ELSIF substr(input,i,2)='*/' THEN depth:=depth-1; i:=i+2;
        ELSE i:=i+1; END IF;
      END LOOP;
      IF depth<>0 THEN RAISE EXCEPTION 'pg-logical-repl: unterminated SQL comment'; END IF;
      result:=result||' '; CONTINUE;
    END IF;
    IF ended THEN RAISE EXCEPTION 'pg-logical-repl: exactly one top-level command is required' USING ERRCODE='LC001'; END IF;
    IF ch=';' THEN ended:=true; i:=i+1; CONTINUE; END IF;
    IF ch IN (chr(39),'"') THEN
      quote:=ch; escaped:=quote=chr(39) AND (upper(substr(input,greatest(i-1,1),1))='E' OR current_setting('standard_conforming_strings')='off');
      i:=i+1; closed:=false;
      IF quote='"' THEN result:=result||'"'; ELSE result:=result||'?'; END IF;
      WHILE i<=n LOOP
        ch:=substr(input,i,1);
        IF escaped AND ch=chr(92) THEN i:=i+2; CONTINUE; END IF;
        IF ch=quote THEN
          IF substr(input,i+1,1)=quote THEN
            IF quote='"' THEN result:=result||'""'; END IF;
            i:=i+2; CONTINUE;
          END IF;
          closed:=true; i:=i+1; EXIT;
        END IF;
        IF quote='"' THEN result:=result||upper(ch); END IF;
        i:=i+1;
      END LOOP;
      IF NOT closed THEN RAISE EXCEPTION 'pg-logical-repl: unterminated SQL quote'; END IF;
      IF quote='"' THEN result:=result||'"'; END IF;
      CONTINUE;
    END IF;
    IF ch='$' THEN
      tag:=substring(substr(input,i) FROM '^\$([a-zA-Z_][a-zA-Z_0-9]*)?\$');
      -- substring with a capturing group returns the group, so obtain the whole delimiter separately.
      tag:=(regexp_match(substr(input,i),'^(\$[a-zA-Z_0-9]*\$)'))[1];
      IF tag IS NOT NULL THEN
        i:=i+length(tag); depth:=strpos(substr(input,i),tag);
        IF depth=0 THEN RAISE EXCEPTION 'pg-logical-repl: unterminated dollar quote'; END IF;
        i:=i+depth-1+length(tag); result:=result||'?'; CONTINUE;
      END IF;
    END IF;
    result:=result||upper(ch); i:=i+1;
  END LOOP;
  RETURN trim(regexp_replace(result,'[[:space:]]+',' ','g'));
END
$body$;
REVOKE ALL ON FUNCTION lc.clean_sql(text) FROM PUBLIC;

CREATE FUNCTION lc.resolve_relation(name text, path text) RETURNS oid
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  PERFORM pg_catalog.set_config('search_path',path,true);
  RETURN pg_catalog.to_regclass(name);
END
$$;
REVOKE ALL ON FUNCTION lc.resolve_relation(text,text) FROM PUBLIC;

CREATE FUNCTION lc.top_comma(code text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE i int:=1; depth int:=0; quoted boolean:=false; ch text;
BEGIN
  WHILE i<=length(code) LOOP
    ch:=substr(code,i,1);
    IF ch='"' THEN
      IF quoted AND substr(code,i+1,1)='"' THEN i:=i+2; CONTINUE; END IF;
      quoted:=NOT quoted;
    ELSIF NOT quoted THEN
      IF ch='(' THEN depth:=depth+1;
      ELSIF ch=')' THEN depth:=depth-1;
      ELSIF ch=',' AND depth=0 THEN RETURN true; END IF;
    END IF;
    i:=i+1;
  END LOOP;
  RETURN false;
END
$$;
REVOKE ALL ON FUNCTION lc.top_comma(text) FROM PUBLIC;

CREATE FUNCTION lc.record_ddl(statement text, command_tag text, actions jsonb, jobs jsonb) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE context jsonb; result bigint;
BEGIN
  context:=jsonb_build_object('lock_timeout',(SELECT settings->>'lock_timeout_ms' FROM lc.control LIMIT 1),'TimeZone',current_setting('TimeZone'),'DateStyle',current_setting('DateStyle'),
    'IntervalStyle',current_setting('IntervalStyle'),'standard_conforming_strings',current_setting('standard_conforming_strings'),
    'search_path',COALESCE(NULLIF(current_setting('lc.caller_search_path',true),''),'pg_catalog'));
  SELECT COALESCE(jsonb_agg(j||jsonb_build_object('context',context)),'[]') INTO jobs FROM jsonb_array_elements(jobs) j;
  INSERT INTO lc.ddl_log(migration_id,statement,statement_sha256,context,command_tag,actions,queue_jobs,operation_key,payload_sha256)
    SELECT migration_id,statement,encode(sha256(convert_to(statement,'UTF8')),'hex'),context,command_tag,actions,jobs,CASE WHEN current_setting('lc.ownership_token',true)=ownership_token AND current_setting('lc.managed',true)='on' THEN NULLIF(current_setting('lc.operation_key',true),'') END,
      encode(sha256(convert_to(jsonb_build_array(2,statement,context,actions,jobs)::text,'UTF8')),'hex') FROM lc.control
    RETURNING id INTO result;
  RETURN result;
END
$$;
REVOKE ALL ON FUNCTION lc.record_ddl(text,text,jsonb,jsonb) FROM PUBLIC;
