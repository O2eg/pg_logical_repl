"""Catalog, role and installed-resource checks without exporting password verifiers."""

import hmac
import json
from contextlib import asynccontextmanager

from .catalog import difference, inventory, schema_manifest
from .config import canonical, digest
from .errors import PreconditionError
from .state import now, write_private


def role_view(inv, token, executor):
    if not inv["password_access"]:
        raise PreconditionError("ROLE_PASSWORD_UNREADABLE", "Role verifiers must remain readable")
    roles = {}
    for role in inv["roles"]:
        if role["rolname"] == executor:
            continue
        value = dict(role)
        password = value.pop("rolpassword")
        value["password_hmac"] = hmac.digest(token.encode(), canonical(password), "sha256").hex()
        roles[role["rolname"]] = value
    memberships = [r for r in inv["memberships"] if executor not in (r["role"], r["member"], r["grantor"])]
    return dict(roles=roles, memberships=sorted(memberships, key=canonical))


async def resources(c, executor):
    namespace = dict(
        await c.fetchrow("""SELECT oid::int,pg_get_userbyid(nspowner) AS owner,nspacl::text
        FROM pg_namespace WHERE nspname='lc'""")
    )
    relations = [
        dict(r)
        for r in await c.fetch("""SELECT c.oid::int,c.relname,c.relkind::text,
        c.relacl::text,pg_get_userbyid(c.relowner) AS owner,c.relreplident::text,
        (SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
          a.attidentity::text,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
         FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
         WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
        (SELECT jsonb_agg(pg_get_constraintdef(co.oid) ORDER BY co.conname)
         FROM pg_constraint co WHERE co.conrelid=c.oid) AS constraints
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='lc' AND c.relkind IN ('r','S') ORDER BY c.relname""")
    ]
    functions = [
        dict(r)
        for r in await c.fetch("""SELECT p.oid::int,p.proname,
        pg_get_function_identity_arguments(p.oid) AS arguments,pg_get_functiondef(p.oid) AS definition,
        pg_get_userbyid(p.proowner) AS owner,p.proacl::text AS acl,p.proconfig,p.prosecdef
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='lc' ORDER BY 1,2""")
    ]
    triggers = [
        dict(r)
        for r in await c.fetch("""SELECT evtname,evtevent,evtenabled::text,
        evtfoid::regprocedure::text AS function,evttags,pg_get_userbyid(evtowner) AS owner
        FROM pg_event_trigger WHERE evtname IN ('lc_guard','lc_capture','lc_rewrite_guard') ORDER BY 1""")
    ]
    replay = [
        dict(r)
        for r in await c.fetch("""SELECT tgname,tgenabled::text,pg_get_triggerdef(oid) AS definition
        FROM pg_trigger WHERE tgrelid='lc.ddl_log'::regclass AND NOT tgisinternal ORDER BY 1""")
    ]
    control = dict(await c.fetchrow("SELECT ddl_mode,resource_version,settings FROM lc.control"))
    role = await c.fetchrow(
        """SELECT oid::int,rolsuper,rolcanlogin,rolcreaterole,rolcreatedb,rolinherit,
        rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1""",
        executor,
    )
    grants = [
        dict(r)
        for r in await c.fetch(
            """SELECT pg_get_userbyid(roleid) AS role,
        pg_get_userbyid(grantor) AS grantor,to_jsonb(m)->>'inherit_option' AS inherit_option,
        to_jsonb(m)->>'set_option' AS set_option,admin_option
        FROM pg_auth_members m WHERE member=(SELECT oid FROM pg_roles WHERE rolname=$1)
        ORDER BY 1,2""",
            executor,
        )
    ]
    return digest(
        dict(
            namespace=namespace,
            relations=relations,
            functions=functions,
            triggers=triggers,
            replay=replay,
            control=control,
            executor=dict(role) if role else None,
            grants=grants,
        )
    )


async def establish(service, sc, tc):
    state = service.store.read()
    executor = "lc_ddl_executor_" + service.config.migration_id
    token = state["ownership_token"]
    state["role_baseline"] = {
        side: role_view(await inventory(c, include_passwords=True), token, executor)
        for side, c in [("source", sc), ("target", tc)]
    }
    state["resource_baseline"] = {
        side: await resources(c, executor) for side, c in [("source", sc), ("target", tc)]
    }
    state["config_hash"] = service.config.control_hash
    service.store.save(state)


async def inspect(service, sc, tc, *, schema=True, barrier_passed=False):
    state = service.store.read()
    blockers, details = [], {}
    if not state.get("role_baseline") or not state.get("resource_baseline"):
        raise PreconditionError(
            "INTEGRITY_BASELINE_MISSING", "This migration has no integrity baseline; use a fresh migration"
        )
    if state.get("config_hash") != service.config.control_hash:
        blockers.append("CONFIGURATION_CHANGED")
    executor = "lc_ddl_executor_" + service.config.migration_id
    before = await sc.fetchval("SELECT COALESCE(max(id),0) FROM lc.ddl_log")
    inventories = {
        side: await inventory(c, include_passwords=True) for side, c in [("source", sc), ("target", tc)]
    }
    views = {}
    for side, c in [("source", sc), ("target", tc)]:
        views[side] = role_view(inventories[side], state["ownership_token"], executor)
        changes = difference(state["role_baseline"][side], views[side])
        if changes:
            blockers.append(side.upper() + "_ROLE_DRIFT")
            details[side + "_roles"] = changes
        if await resources(c, executor) != state["resource_baseline"][side]:
            blockers.append(side.upper() + "_RESOURCE_DRIFT")
    names = set(views["source"]["roles"])
    target_subset = dict(
        roles={name: views["target"]["roles"].get(name) for name in sorted(names)},
        memberships=[r for r in views["target"]["memberships"] if r["role"] in names or r["member"] in names],
    )
    source_subset = dict(
        views["source"],
        memberships=[r for r in views["source"]["memberships"] if r["role"] in names or r["member"] in names],
    )
    if changes := difference(source_subset, target_subset):
        blockers.append("ROLES_DIFFER")
        details["roles"] = changes
    source_log = [dict(r) for r in await sc.fetch("SELECT id,statement_sha256 FROM lc.ddl_log ORDER BY id")]
    applied = [dict(r) for r in await tc.fetch("SELECT id,statement_sha256 FROM lc.ddl_applied ORDER BY id")]
    pending = await tc.fetchval("""SELECT EXISTS(SELECT FROM lc.ddl_queue WHERE status NOT IN ('done','skipped'))
        OR EXISTS(SELECT FROM lc.table_sync WHERE state NOT IN ('ready','removed'))""")
    after = await sc.fetchval("SELECT COALESCE(max(id),0) FROM lc.ddl_log")
    settled = before == after and source_log == applied and not pending
    if barrier_passed and before == after and not pending and source_log != applied:
        blockers.append("DDL_JOURNAL_MISMATCH")
    # A DDL transaction or an unfinished queue is an expected temporary divergence.
    if schema and settled:
        changes = difference(schema_manifest(inventories["source"]), schema_manifest(inventories["target"]))
        if changes:
            blockers.append("SCHEMA_DRIFT")
            details["schema"] = changes
    result = dict(
        checked_at=now(), passed=not blockers, blockers=blockers, differences=details, settled=settled
    )
    write_private(service.store.directory / "reports/integrity.json", json.dumps(result, indent=2))
    return result


async def require(service, sc, tc):
    result = await inspect(service, sc, tc, barrier_passed=True)
    if result["blockers"] or not result["settled"]:
        raise PreconditionError(
            "INTEGRITY_BLOCKED", ", ".join(result["blockers"]) or "DDL journal is not settled"
        )
    return result


@asynccontextmanager
async def stable_catalogs(service, sc, tc):
    # Same lock as the captured DDL guard; an independent connection commits the barrier.
    async with sc.transaction():
        await sc.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('logical_connector/ddl/'||current_database(),0))"
        )
        async with service.source.connect() as marker:
            await service.barrier(marker, tc)
        yield
