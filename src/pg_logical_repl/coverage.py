"""Conservative catalog coverage for the qualified local profile.

Return object identities, never foreign-server options, mappings or label values:
those may contain credentials. Extension members are handled by extension policy.
"""


async def unsupported_objects(c):
    # All identifiers below are internal constants, never user-provided SQL.
    catalogs = {
        "pg_operator": ("oprnamespace", "true"),
        "pg_opclass": ("opcnamespace", "true"),
        "pg_opfamily": ("opfnamespace", "true"),
        "pg_collation": ("collnamespace", "true"),
        "pg_conversion": ("connamespace", "true"),
        "pg_ts_config": ("cfgnamespace", "true"),
        "pg_ts_dict": ("dictnamespace", "true"),
        "pg_ts_parser": ("prsnamespace", "true"),
        "pg_ts_template": ("tmplnamespace", "true"),
        "pg_statistic_ext": ("stxnamespace", "true"),
        "pg_proc": ("pronamespace", "o.prokind IN ('a','w')"),
        "pg_type": (
            "typnamespace",
            "o.typelem=0 AND o.typtype IN ('b','r','m','c') AND "
            "(o.typrelid=0 OR EXISTS(SELECT FROM pg_class cr WHERE cr.oid=o.typrelid AND cr.relkind='c'))",
        ),
        "pg_foreign_data_wrapper": (None, "true"),
        "pg_foreign_server": (None, "true"),
        "pg_user_mapping": (None, "true"),
        "pg_transform": (None, "true"),
        # User casts have non-bootstrap OIDs, including casts between built-in types.
        "pg_cast": (None, "o.oid>=16384"),
        "pg_language": (None, "o.lanname NOT IN ('internal','c','sql','plpgsql')"),
    }
    queries = []
    for catalog, (namespace, predicate) in catalogs.items():
        query = (
            f"SELECT '{catalog}' AS category, "
            f"(pg_identify_object('{catalog}'::regclass,o.oid,0)).identity AS identity "
            f"FROM {catalog} o "
        )
        if namespace:
            query += f"JOIN pg_namespace n ON n.oid=o.{namespace} "
            predicate += " AND n.nspname !~ '^pg_' AND n.nspname NOT IN ('information_schema','lc')"
        query += (
            f"WHERE ({predicate}) AND NOT EXISTS(SELECT FROM pg_depend d "
            f"WHERE d.classid='{catalog}'::regclass AND d.objid=o.oid AND d.deptype='e')"
        )
        queries.append(query)
    queries.extend(
        [
            """SELECT 'row_security', format('%I.%I',n.nspname,r.relname)
            FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE (r.relrowsecurity OR r.relforcerowsecurity) AND n.nspname !~ '^pg_'
            AND n.nspname NOT IN ('lc','information_schema')""",
            """SELECT 'pg_policy',(pg_identify_object('pg_policy'::regclass,p.oid,0)).identity
            FROM pg_policy p JOIN pg_class r ON r.oid=p.polrelid
            JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE n.nspname !~ '^pg_' AND n.nspname NOT IN ('lc','information_schema')""",
            """SELECT 'pg_rewrite',(pg_identify_object('pg_rewrite'::regclass,p.oid,0)).identity
            FROM pg_rewrite p JOIN pg_class r ON r.oid=p.ev_class
            JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE p.rulename<>'_RETURN' AND n.nspname !~ '^pg_'
            AND n.nspname NOT IN ('lc','information_schema')""",
            """SELECT 'inheritance',format('%I.%I',n.nspname,r.relname)
            FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE NOT r.relispartition AND EXISTS(SELECT FROM pg_inherits i WHERE i.inhrelid=r.oid)
            AND n.nspname !~ '^pg_' AND n.nspname NOT IN ('lc','information_schema')""",
            """SELECT 'table_access_method',format('%I.%I',n.nspname,r.relname)
            FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace JOIN pg_am a ON a.oid=r.relam
            WHERE r.relkind IN ('r','m') AND a.amname<>'heap' AND n.nspname !~ '^pg_'
            AND n.nspname NOT IN ('lc','information_schema')""",
            """SELECT 'tablespace',format('%I.%I',n.nspname,r.relname)
            FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE r.reltablespace<>0 AND n.nspname !~ '^pg_'
            AND n.nspname NOT IN ('lc','information_schema')""",
            """SELECT 'database_tablespace',d.datname FROM pg_database d
            JOIN pg_tablespace t ON t.oid=d.dattablespace
            WHERE d.datname=current_database() AND t.spcname<>'pg_default'""",
            """SELECT 'security_label',(pg_identify_object(classoid,objoid,objsubid)).identity
            FROM pg_seclabel""",
            """SELECT 'shared_security_label',(pg_identify_object(classoid,objoid,0)).identity
            FROM pg_shseclabel WHERE classoid<>'pg_database'::regclass
            OR objoid=(SELECT oid FROM pg_database WHERE datname=current_database())""",
        ]
    )
    return [dict(r) for r in await c.fetch(" UNION ALL ".join(queries) + " ORDER BY 1,2")]


def manifest(inv):
    objects = []
    for r in inv["relations"]:
        status = "schema_only"
        if r["extension"]:
            status = "extension_managed"
        elif r["kind"] in {"m", "S"} or r["persistence"] == "u":
            status = "deferred_cutover"
        elif r["kind"] in {"r", "p"}:
            status = "replicated"
        elif r["kind"] == "f":
            status = "blocked"
        objects.append(dict(category="relation", identity=[r["schema"], r["name"]], status=status))
    for role in inv["roles"]:
        objects.append(dict(category="role", identity=role["rolname"], status="schema_only"))
    for category in ("schemas", "functions", "types", "extensions"):
        for r in inv[category]:
            objects.append(
                dict(category=category, identity=[r.get("schema"), r["name"]], status="schema_only")
            )
    for r in inv["unsupported_objects"]:
        objects.append(dict(r, status="blocked", reason="OBJECT_PROFILE_UNSUPPORTED"))
    return dict(
        schema_version="pg_logical_connector/coverage-alpha",
        exhaustive=False,
        objects=objects,
        note="Qualified local profile; full SPEC object-by-object coverage is not yet implemented",
    )
