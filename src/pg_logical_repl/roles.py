"""Exact role copy; existing cluster-wide roles are never silently altered."""

from .errors import PreconditionError
from .security import identifier as qi
from .security import literal as ql

FLAGS = {
    "rolsuper": "SUPERUSER",
    "rolinherit": "INHERIT",
    "rolcreaterole": "CREATEROLE",
    "rolcreatedb": "CREATEDB",
    "rolcanlogin": "LOGIN",
    "rolreplication": "REPLICATION",
    "rolbypassrls": "BYPASSRLS",
}


async def copy_roles(source, target, source_inventory, redactor):
    existing = {r["rolname"] for r in await target.fetch("SELECT rolname FROM pg_roles")}
    for r in source_inventory["roles"]:
        if r["rolname"] in existing:
            continue
        redactor.add(r["rolpassword"])
        clauses = [flag if r[key] else "NO" + flag for key, flag in FLAGS.items()]
        clauses += [
            "CONNECTION LIMIT " + str(r["rolconnlimit"]),
            "PASSWORD " + ("NULL" if r["rolpassword"] is None else ql(r["rolpassword"])),
        ]
        if r["rolvaliduntil"] is not None:
            clauses.append("VALID UNTIL " + ql(r["rolvaliduntil"]))
        await target.execute("CREATE ROLE " + qi(r["rolname"]) + " WITH " + " ".join(clauses))
        for setting in r["rolconfig"] or []:
            key, value = setting.split("=", 1)
            await target.execute("ALTER ROLE " + qi(r["rolname"]) + " SET " + qi(key) + " TO " + ql(value))
    target_major = int(await target.fetchval("SHOW server_version_num")) // 10000
    for m in source_inventory["memberships"]:
        current = await target.fetchrow(
            """SELECT admin_option,pg_get_userbyid(grantor) AS grantor
            FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1)
            AND member=(SELECT oid FROM pg_roles WHERE rolname=$2)""",
            m["role"],
            m["member"],
        )
        if current:
            if current["admin_option"] != m["admin_option"] or current["grantor"] != m["grantor"]:
                raise PreconditionError("MEMBERSHIP_CONFLICT", "Existing role membership differs")
            if target_major >= 16:
                options = await target.fetchrow(
                    """SELECT inherit_option,set_option FROM pg_auth_members
                    WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1)
                    AND member=(SELECT oid FROM pg_roles WHERE rolname=$2)
                    AND grantor=(SELECT oid FROM pg_roles WHERE rolname=$3)""",
                    m["role"],
                    m["member"],
                    m["grantor"],
                )
                if not options or any(options[k] != m[k] for k in ("inherit_option", "set_option")):
                    raise PreconditionError("MEMBERSHIP_CONFLICT", "Existing membership options differ")
            continue
        statement = "GRANT " + qi(m["role"]) + " TO " + qi(m["member"])
        if target_major >= 16:
            statement += (
                " WITH ADMIN "
                + str(m["admin_option"]).upper()
                + ", INHERIT "
                + str(m["inherit_option"]).upper()
                + ", SET "
                + str(m["set_option"]).upper()
            )
            statement += " GRANTED BY " + qi(m["grantor"])
        elif m["admin_option"]:
            statement += " WITH ADMIN OPTION"
        if target_major < 16:
            await target.execute("SET LOCAL ROLE " + qi(m["grantor"]))
        await target.execute(statement)
        await target.execute("RESET ROLE")
    db = await target.fetchval("SELECT current_database()")
    public = next((r for r in source_inventory["schemas"] if r["name"] == "public"), None)
    if public:
        await target.execute("ALTER SCHEMA public OWNER TO " + qi(public["owner"]))
    elif await target.fetchval("SELECT to_regnamespace('public') IS NOT NULL"):
        await target.execute("DROP SCHEMA public")
    meta = source_inventory["database"]
    await target.execute("ALTER DATABASE " + qi(db) + " OWNER TO " + qi(meta["owner"]))
    await target.execute("ALTER DATABASE " + qi(db) + " CONNECTION LIMIT " + str(meta["datconnlimit"]))
    await target.execute(
        "COMMENT ON DATABASE "
        + qi(db)
        + " IS "
        + ("NULL" if meta["comment"] is None else ql(meta["comment"]))
    )
    old_acl = await target.fetch("""SELECT DISTINCT a.grantee FROM pg_database d,
        LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a WHERE d.datname=current_database()""")
    for row in old_acl:
        grantee = (
            "PUBLIC"
            if row["grantee"] == 0
            else qi(await target.fetchval("SELECT pg_get_userbyid($1)", row["grantee"]))
        )
        await target.execute("REVOKE ALL ON DATABASE " + qi(db) + " FROM " + grantee)
    for row in meta["acl"]:
        if row["privilege"] not in {"CONNECT", "CREATE", "TEMPORARY"}:
            raise PreconditionError("DATABASE_ACL_UNKNOWN", "Unknown database privilege")
        await target.execute("SET LOCAL ROLE " + qi(row["grantor"]))
        await target.execute(
            "GRANT "
            + row["privilege"]
            + " ON DATABASE "
            + qi(db)
            + " TO "
            + ("PUBLIC" if row["grantee"] == "PUBLIC" else qi(row["grantee"]))
            + (" WITH GRANT OPTION" if row["grantable"] else "")
        )
        await target.execute("RESET ROLE")
    for r in source_inventory["role_settings"]:
        prefix = (
            "ALTER DATABASE " + qi(db)
            if r["role"] is None
            else "ALTER ROLE " + qi(r["role"]) + " IN DATABASE " + qi(db)
        )
        for setting in r["setconfig"]:
            key, value = setting.split("=", 1)
            await target.execute(prefix + " SET " + qi(key) + " TO " + ql(value))
