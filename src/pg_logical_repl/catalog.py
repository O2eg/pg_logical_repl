"""Version-aware inventory; SQL definitions and verifiers remain private."""

from __future__ import annotations

import asyncpg

from .config import digest
from .coverage import unsupported_objects
from .db import identity


def acl(expression):
    return (
        """(SELECT jsonb_agg(jsonb_build_object('grantor',pg_get_userbyid(a.grantor),
        'grantee',CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
        'privilege',a.privilege_type,'grantable',a.is_grantable)
        ORDER BY pg_get_userbyid(a.grantor),CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,a.privilege_type)
        FROM aclexplode("""
        + expression
        + ") a)"
    )


RELATIONS = """
SELECT c.oid::int AS oid, n.nspname AS schema, c.relname AS name, c.relkind::text AS kind,
 c.relpersistence::text AS persistence, c.relreplident::text AS replica_identity,
 c.relispartition AS is_partition, pg_get_userbyid(c.relowner) AS owner,
 c.relrowsecurity AS row_security, c.relforcerowsecurity AS force_row_security,
 c.relispopulated AS populated, c.relacl::text AS acl,
 c.reloptions, pg_get_expr(c.relpartbound,c.oid) AS partition_bound,
 CASE WHEN c.relkind='p' THEN pg_get_partkeydef(c.oid) END AS partition_key,
 (SELECT array_agg(format('%I.%I',pn.nspname,pc.relname) ORDER BY i.inhseqno)
  FROM pg_inherits i JOIN pg_class pc ON pc.oid=i.inhparent
  JOIN pg_namespace pn ON pn.oid=pc.relnamespace WHERE i.inhrelid=c.oid) AS parents,
 (SELECT jsonb_build_object('type',format_type(s.seqtypid,NULL),'start',s.seqstart,
    'increment',s.seqincrement,'min',s.seqmin,'max',s.seqmax,'cache',s.seqcache,'cycle',s.seqcycle)
  FROM pg_sequence s WHERE s.seqrelid=c.oid) AS sequence_definition,
 obj_description(c.oid,'pg_class') AS comment,
 EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary AND i.indisvalid) AS has_pk,
 EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND i.indisreplident AND i.indisvalid) AS has_identity_index,
 (SELECT e.extname FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid
 WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e' LIMIT 1) AS extension,
 CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid,true) ELSE NULL END AS view_definition,
 pg_total_relation_size(c.oid)::bigint AS bytes
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND n.nspname <> 'lc'
 AND c.relkind IN ('r','p','v','m','S','f') ORDER BY n.nspname,c.relname
"""
RELATIONS = RELATIONS.replace(
    "c.relacl::text AS acl",
    acl("COALESCE(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's' ELSE 'r' END::\"char\",c.relowner))")
    + " AS acl",
)


async def inventory(c, *, include_passwords=False):
    ident = await identity(c)
    major = ident["major"]
    relations = [dict(r) for r in await c.fetch(RELATIONS)]
    columns = await c.fetch("""SELECT a.attrelid::int AS relid,a.attnum,a.attname,
        format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,
        a.attidentity::text,a.attgenerated::text,pg_get_expr(d.adbin,d.adrelid) AS default,
        col_description(a.attrelid,a.attnum) AS comment,a.attacl::text AS acl,
        a.attstorage::text,a.attcompression::text,
        CASE WHEN a.attcollation<>0 THEN
          (SELECT format('%I.%I',cn.nspname,cl.collname) FROM pg_collation cl
           JOIN pg_namespace cn ON cn.oid=cl.collnamespace WHERE cl.oid=a.attcollation)
        END AS collation,
        EXISTS(SELECT FROM pg_opclass oc JOIN pg_am am ON am.oid=oc.opcmethod
          WHERE oc.opcdefault AND oc.opcintype=a.atttypid AND am.amname IN ('btree','hash')) AS full_identity_equality
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attnum>0 AND NOT a.attisdropped AND n.nspname !~ '^pg_'
        AND n.nspname NOT IN ('information_schema','lc') ORDER BY a.attrelid,a.attnum""")
    constraints = await c.fetch("""SELECT conrelid::int AS relid,conname,contype::text,convalidated,
        pg_get_constraintdef(oid,true) AS definition,obj_description(oid,'pg_constraint') AS comment FROM pg_constraint
        WHERE conrelid<>0 ORDER BY conrelid,conname""")
    indexes = await c.fetch("""SELECT i.indrelid::int AS relid,ci.relname AS name,
        i.indisvalid,pg_get_indexdef(i.indexrelid) AS definition,obj_description(ci.oid,'pg_class') AS comment
        FROM pg_index i JOIN pg_class ci ON ci.oid=i.indexrelid ORDER BY i.indrelid,ci.relname""")
    triggers = await c.fetch("""SELECT t.tgrelid::int AS relid,t.tgname,t.tgenabled::text,
        pg_get_triggerdef(t.oid,true) AS definition,obj_description(t.oid,'pg_trigger') AS comment FROM pg_trigger t
        WHERE NOT t.tgisinternal ORDER BY t.tgrelid,t.tgname""")
    for rel in relations:
        for name, rows in [
            ("columns", columns),
            ("constraints", constraints),
            ("indexes", indexes),
            ("triggers", triggers),
        ]:
            rel[name] = [
                {k: v for k, v in dict(row).items() if k != "relid"}
                for row in rows
                if row["relid"] == rel["oid"]
            ]
    extensions = [
        dict(r)
        for r in await c.fetch("""SELECT e.extname AS name,e.extversion AS version,
        n.nspname AS schema,pg_get_userbyid(e.extowner) AS owner,e.extconfig::text,e.extcondition::text
        FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY e.extname""")
    ]
    role_columns = "rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit,rolvaliduntil::text,rolconfig"
    roles = [
        dict(r)
        for r in await c.fetch(
            "SELECT " + role_columns + " FROM pg_roles WHERE rolname !~ '^pg_' ORDER BY rolname"
        )
    ]
    password_access = False
    if include_passwords:
        try:
            hashes = {
                r["rolname"]: r["rolpassword"]
                for r in await c.fetch("SELECT rolname,rolpassword FROM pg_authid WHERE rolname !~ '^pg_'")
            }
            for role in roles:
                role["rolpassword"] = hashes[role["rolname"]]
            password_access = True
        except asyncpg.InsufficientPrivilegeError:
            pass
    options = ",inherit_option,set_option" if major >= 16 else ""
    memberships = [
        dict(r)
        for r in await c.fetch(
            "SELECT pg_get_userbyid(roleid) AS role,pg_get_userbyid(member) AS member,pg_get_userbyid(grantor) AS grantor,admin_option"
            + options
            + " FROM pg_auth_members ORDER BY roleid,member,grantor"
        )
    ]
    functions = [
        dict(r)
        for r in await c.fetch(
            """SELECT n.nspname AS schema,p.proname AS name,
        pg_get_function_identity_arguments(p.oid) AS arguments,pg_get_functiondef(p.oid) AS definition,
        obj_description(p.oid,'pg_proc') AS comment,pg_get_userbyid(p.proowner) AS owner,p.proacl::text AS acl
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname !~ '^pg_' AND n.nspname NOT IN ('lc','information_schema')
        AND p.prokind IN ('f','p') AND NOT EXISTS(SELECT FROM pg_depend d
        WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
        ORDER BY n.nspname,p.proname,pg_get_function_identity_arguments(p.oid)""".replace(
                "p.proacl::text AS acl", acl("COALESCE(p.proacl,acldefault('f',p.proowner))") + " AS acl"
            )
        )
    ]
    schemas = [
        dict(r)
        for r in await c.fetch(
            """SELECT obj_description(oid,'pg_namespace') AS comment,nspname AS name,pg_get_userbyid(nspowner) AS owner,nspacl::text AS acl
        FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname NOT IN ('lc','information_schema') ORDER BY nspname""".replace(
                "nspacl::text AS acl", acl("COALESCE(nspacl,acldefault('n',nspowner))") + " AS acl"
            )
        )
    ]
    settings = {
        r["name"]: r["setting"]
        for r in await c.fetch(
            "SELECT name,setting FROM pg_settings WHERE name IN ('wal_level','max_replication_slots','max_wal_senders','max_logical_replication_workers','max_worker_processes','max_sync_workers_per_subscription','max_active_replication_origins','fsync','synchronized_standby_slots','idle_replication_slot_timeout')"
        )
    }
    sessions = [
        dict(r)
        for r in await c.fetch("""SELECT pid,usename,application_name,client_addr::text,state,backend_type
        FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
        AND application_name NOT LIKE 'pg-logical-repl/%' ORDER BY pid""")
    ]
    slots = [
        dict(r)
        for r in await c.fetch(
            "SELECT slot_name,slot_type,plugin,database,active FROM pg_replication_slots ORDER BY slot_name"
        )
    ]
    role_settings = [
        dict(r)
        for r in await c.fetch("""SELECT CASE WHEN setrole=0 THEN NULL ELSE pg_get_userbyid(setrole) END AS role, setconfig
        FROM pg_db_role_setting WHERE setdatabase=(SELECT oid FROM pg_database WHERE datname=current_database())
        ORDER BY 1 NULLS FIRST""")
    ]
    types = [
        dict(r)
        for r in await c.fetch(
            """SELECT obj_description(t.oid,'pg_type') AS comment,n.nspname AS schema,t.typname AS name,t.typtype::text AS kind,
        pg_get_userbyid(t.typowner) AS owner,t.typnotnull,t.typdefault,t.typacl::text AS acl,
        CASE WHEN t.typbasetype<>0 THEN format_type(t.typbasetype,t.typtypmod) END AS base_type,
        (SELECT array_agg(enumlabel ORDER BY enumsortorder) FROM pg_enum WHERE enumtypid=t.oid) AS labels,
        (SELECT jsonb_agg(jsonb_build_object('name',conname,'definition',pg_get_constraintdef(oid,true)) ORDER BY conname)
         FROM pg_constraint WHERE contypid=t.oid) AS constraints
        FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
        WHERE n.nspname !~ '^pg_' AND n.nspname NOT IN ('lc','information_schema')
        AND t.typtype IN ('d','e','r','m','c') AND t.typelem=0
        AND (t.typrelid=0 OR EXISTS(SELECT FROM pg_class c WHERE c.oid=t.typrelid AND c.relkind='c'))
        AND NOT EXISTS(SELECT FROM pg_depend d WHERE d.classid='pg_type'::regclass AND d.objid=t.oid AND d.deptype='e')
        ORDER BY n.nspname,t.typname""".replace(
                "t.typacl::text AS acl", acl("COALESCE(t.typacl,acldefault('T',t.typowner))") + " AS acl"
            )
        )
    ]
    default_acl = [
        dict(r)
        for r in await c.fetch("""SELECT pg_get_userbyid(d.defaclrole) AS role,
        n.nspname AS schema,d.defaclobjtype::text AS kind,d.defaclacl::text AS acl FROM pg_default_acl d
        LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace ORDER BY 1,2,3""")
    ]
    database_acl = [
        dict(r)
        for r in await c.fetch("""SELECT pg_get_userbyid(a.grantor) AS grantor,
        CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
        a.privilege_type AS privilege,a.is_grantable AS grantable
        FROM pg_database d,LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a
        WHERE d.datname=current_database() ORDER BY 1,2,3""")
    ]
    database = dict(
        await c.fetchrow("""SELECT pg_get_userbyid(datdba) AS owner,datconnlimit,
        shobj_description(oid,'pg_database') AS comment FROM pg_database WHERE datname=current_database()""")
    )
    database["acl"] = database_acl
    return dict(
        identity=ident,
        relations=relations,
        extensions=extensions,
        schemas=schemas,
        roles=roles,
        memberships=memberships,
        password_access=password_access,
        functions=functions,
        settings=settings,
        sessions=sessions,
        slots=slots,
        role_settings=role_settings,
        types=types,
        default_acl=default_acl,
        database=database,
        unsupported_objects=await unsupported_objects(c),
        large_objects=await c.fetchval("SELECT count(*) FROM pg_largeobject_metadata"),
        publications=[
            dict(x) for x in await c.fetch("SELECT pubname,puballtables FROM pg_publication ORDER BY pubname")
        ],
        subscriptions=[
            dict(x)
            for x in await c.fetch(
                "SELECT subname,subenabled FROM pg_subscription WHERE subdbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY subname"
            )
        ],
        event_triggers=[
            dict(x)
            for x in await c.fetch("SELECT evtname,evtenabled::text FROM pg_event_trigger ORDER BY evtname")
        ],
    )


def schema_manifest(inv):
    relations = []
    for row in inv["relations"]:
        if row["extension"]:
            continue
        relations.append({k: v for k, v in row.items() if k not in {"oid", "bytes", "populated"}})
    return dict(
        relations=relations,
        functions=inv["functions"],
        extensions=inv["extensions"],
        schemas=inv["schemas"],
        types=inv["types"],
        default_acl=inv["default_acl"],
        database=inv["database"],
        role_settings=inv["role_settings"],
        unsupported_objects=inv["unsupported_objects"],
    )


def fingerprint(inv):
    return digest(schema_manifest(inv))


def difference(left, right, path=""):
    if type(left) is not type(right):
        return [path]
    if isinstance(left, dict):
        return [
            p
            for k in sorted(set(left) | set(right))
            for p in difference(left.get(k), right.get(k), path + "/" + str(k))
        ]
    if isinstance(left, list):
        if len(left) != len(right):
            return [path + "/length"]
        return [p for i, (a, b) in enumerate(zip(left, right)) for p in difference(a, b, path + "/" + str(i))]
    return [] if left == right else [path]


def role_fingerprint(inv):
    return digest(
        dict(
            roles=[{k: v for k, v in r.items() if k != "rolpassword"} for r in inv["roles"]],
            memberships=inv["memberships"],
        )
    )


def public_inventory(inv):
    return dict(
        identity=inv["identity"],
        schema_fingerprint=fingerprint(inv),
        role_fingerprint=role_fingerprint(inv),
        password_access=inv["password_access"],
        objects=[
            dict(
                schema=r["schema"],
                name=r["name"],
                kind=r["kind"],
                owner=r["owner"],
                bytes=r["bytes"],
                extension=r["extension"],
            )
            for r in inv["relations"]
        ],
        roles=[r["rolname"] for r in inv["roles"]],
        extensions=inv["extensions"],
        settings=inv["settings"],
        unsupported_objects=inv["unsupported_objects"],
    )
