"""DDL planning and source execution. Native apply replays the protected journal."""

import hashlib
import json
from pathlib import Path

from pglast import ast, parse_sql, parser
from pglast.enums import AlterTableType, DropBehavior, ObjectType

from . import ddl_queue
from .errors import PreconditionError
from .security import qualified as qn


def walk(node):
    if isinstance(node, ast.Node):
        yield node
        for name in node:
            yield from walk(getattr(node, name))
    elif isinstance(node, (tuple, list)):
        for child in node:
            yield from walk(child)


def scope(schema):
    if not schema or schema in {"lc", "information_schema"} or schema.startswith("pg_"):
        raise PreconditionError("DDL_SCOPE", "Use fully qualified application objects")


def classify(statement):
    try:
        parsed = parse_sql(statement)
    except parser.ParseError as exc:
        raise PreconditionError("DDL_PARSE", str(exc)) from None
    if len(parsed) != 1:
        raise PreconditionError("DDL_MULTIPLE", "Exactly one top-level DDL command is required")
    node = parsed[0].stmt
    allowed = {
        "AlterTableStmt",
        "RenameStmt",
        "CreateStmt",
        "CreateTableAsStmt",
        "IndexStmt",
        "DropStmt",
        "ViewStmt",
        "CreateFunctionStmt",
        "AlterFunctionStmt",
        "CreateTrigStmt",
        "CreateSeqStmt",
        "AlterSeqStmt",
        "CreateSchemaStmt",
        "AlterOwnerStmt",
        "AlterObjectSchemaStmt",
        "CommentStmt",
        "GrantStmt",
        "AlterDefaultPrivilegesStmt",
        "CreateEnumStmt",
        "AlterEnumStmt",
        "CreateDomainStmt",
        "AlterDomainStmt",
    }
    if type(node).__name__ not in allowed:
        raise PreconditionError("DDL_NOT_QUALIFIED", "This DDL class is not qualified for replay")
    classification = "in_stream"
    relation = getattr(node, "relation", None)
    if isinstance(node, ast.CreateTableAsStmt):
        relation = node.into.rel
    if isinstance(node, ast.CreateSchemaStmt):
        scope(node.schemaname)
        if node.schemaElts:
            raise PreconditionError("DDL_MULTIPLE", "CREATE SCHEMA must not contain nested commands")
    if isinstance(node, ast.CreateFunctionStmt):
        if len(node.funcname) != 2:
            raise PreconditionError("DDL_SCOPE", "Fully qualify the function name")
        scope(node.funcname[0].sval)
    for part in walk(node):
        if isinstance(part, ast.RangeVar):
            # References in view bodies may use the recorded caller search_path.
            if part is relation or part.schemaname:
                scope(part.schemaname)
            if part.relpersistence != "p":
                raise PreconditionError("DDL_PERSISTENCE", "Only logged application DDL is supported")
        if getattr(part, "behavior", None) == DropBehavior.DROP_CASCADE:
            raise PreconditionError(
                "DDL_CASCADE", "CASCADE requires a separately reviewed dependency migration"
            )
    if isinstance(node, ast.AlterTableStmt):
        if len(node.cmds) != 1:
            raise PreconditionError("DDL_MULTIPLE", "Use one ALTER TABLE action per command")
        cmd = node.cmds[0]
        supported = {
            "AT_AddColumn",
            "AT_DropColumn",
            "AT_ColumnDefault",
            "AT_SetNotNull",
            "AT_DropNotNull",
            "AT_AlterColumnType",
            "AT_SetStorage",
            "AT_SetStatistics",
            "AT_SetCompression",
            "AT_AddConstraint",
            "AT_DropConstraint",
            "AT_ValidateConstraint",
            "AT_ReplicaIdentity",
            "AT_AttachPartition",
            "AT_DetachPartition",
            "AT_ChangeOwner",
            "AT_SetRelOptions",
            "AT_ResetRelOptions",
            "AT_EnableTrig",
            "AT_DisableTrig",
            "AT_EnableTrigUser",
            "AT_DisableTrigUser",
        }
        if cmd.subtype.name not in supported:
            raise PreconditionError("DDL_NOT_QUALIFIED", "ALTER action is outside the DDL matrix")
        if cmd.subtype in {AlterTableType.AT_AddColumn, AlterTableType.AT_AlterColumnType}:
            if any(isinstance(x, (ast.FuncCall, ast.SQLValueFunction)) for x in walk(cmd.def_)):
                raise PreconditionError(
                    "DDL_NONDETERMINISTIC",
                    "Function calls in ADD/USING require a separate deterministic migration",
                )
        if cmd.subtype in {AlterTableType.AT_ValidateConstraint, AlterTableType.AT_AddConstraint}:
            classification = "in_stream_and_queue"
        if cmd.subtype in {AlterTableType.AT_AttachPartition, AlterTableType.AT_DetachPartition}:
            if cmd.def_.concurrent:
                raise PreconditionError("DDL_NOT_QUALIFIED", "Concurrent partition detach is not supported")
            classification = "in_stream_and_queue"
    if isinstance(node, (ast.CreateStmt, ast.CreateTableAsStmt)):
        classification = "in_stream_and_queue"
    if isinstance(node, ast.IndexStmt):
        if not node.idxname or node.if_not_exists:
            raise PreconditionError("DDL_INDEX_NAME", "Use an explicit index name without IF NOT EXISTS")
        classification = "out_of_stream"
    if isinstance(node, ast.DropStmt):
        allowed_drop = {
            "OBJECT_TABLE",
            "OBJECT_INDEX",
            "OBJECT_VIEW",
            "OBJECT_MATVIEW",
            "OBJECT_SEQUENCE",
            "OBJECT_FUNCTION",
            "OBJECT_PROCEDURE",
            "OBJECT_TRIGGER",
            "OBJECT_SCHEMA",
            "OBJECT_TYPE",
            "OBJECT_DOMAIN",
        }
        if node.removeType.name not in allowed_drop or len(node.objects) != 1:
            raise PreconditionError("DDL_NOT_QUALIFIED", "Drop one application object with RESTRICT")
        obj = node.objects[0]
        names = obj.objname if isinstance(obj, ast.ObjectWithArgs) else obj
        if len(names) < (1 if node.removeType == ObjectType.OBJECT_SCHEMA else 2):
            raise PreconditionError("DDL_SCOPE", "Fully qualify the object being dropped")
        scope(names[0].sval)
        if node.removeType == ObjectType.OBJECT_INDEX:
            classification = "out_of_stream"
        if node.removeType == ObjectType.OBJECT_TABLE:
            classification = "in_stream_and_queue"
    if isinstance(node, ast.AlterSeqStmt) and any(x.defname == "restart" for x in node.options):
        raise PreconditionError("DDL_SEQUENCE_RESTART", "Sequence values are synchronized at cutover")
    return dict(
        classification=classification,
        table=qn(relation.schemaname, relation.relname) if relation else None,
        statement=statement,
        statement_sha256=hashlib.sha256(statement.encode()).hexdigest(),
        source_autocommit=bool(getattr(node, "concurrent", False)),
    )


async def execute(service, path, plan_hash, sql_file):
    statement = Path(sql_file).read_text()
    classification = classify(statement)
    async with service.mutation("ddl-exec", path, plan_hash, states={"REPLICATING"}) as (sc, tc, plan):
        await service.owned(sc)
        await service.owned(tc)
        if service.config.ddl_mode == "frozen":
            raise PreconditionError("DDL_FROZEN", "DDL mode is frozen")
        if plan.get("extra", {}).get("statement_sha256") != classification["statement_sha256"]:
            raise PreconditionError("DDL_PLAN_MISMATCH", "Plan must bind the exact SQL file")
        await ddl_queue.process(service, sc, tc)
        archived = await sc.fetchval(
            "SELECT details FROM lc.operations WHERE name=$1", "ddl_archive:" + plan_hash
        )
        if archived:
            receipt = json.loads(archived)
            if receipt["statement_sha256"] != classification["statement_sha256"]:
                raise PreconditionError("DDL_PLAN_MISMATCH", "Archived operation differs from the SQL file")
            state = service.store.read()
            state["active_plan"] = None
            service.store.save(state)
            return dict(receipt, applied=True, archived=True)
        ddl_id = await sc.fetchval("SELECT id FROM lc.ddl_log WHERE operation_key=$1", plan_hash)
        if not ddl_id:
            await sc.fetchval("SELECT set_config('lc.managed','on',false)")
            await sc.fetchval("SELECT set_config('lc.operation_key',$1,false)", plan_hash)
            try:
                if classification["source_autocommit"]:
                    await concurrent_source(service, sc, statement, plan_hash)
                else:
                    async with sc.transaction():
                        await sc.execute(statement)
            finally:
                await sc.fetchval("SELECT set_config('lc.managed','',false)")
                await sc.fetchval("SELECT set_config('lc.operation_key','',false)")
            ddl_id = await sc.fetchval("SELECT id FROM lc.ddl_log WHERE operation_key=$1", plan_hash)
            if not ddl_id:
                raise PreconditionError(
                    "DDL_NOT_CAPTURED", "Source command has no transactional capture receipt"
                )
        await sc.execute(
            "UPDATE lc.operations SET details=details||'{\"complete\":true}'::jsonb WHERE name=$1",
            "ddl_concurrent:" + plan_hash,
        )
        await service.barrier(sc, tc, "ddl")
        # Drain any queue created by the last transaction before reporting success.
        await service.drain_ddl(sc, tc)
        if not await tc.fetchval("SELECT EXISTS(SELECT FROM lc.ddl_applied WHERE id=$1)", ddl_id):
            raise PreconditionError("DDL_NOT_APPLIED", "DDL journal row has no replay receipt")
        s = service.store.read()
        s["active_plan"] = None
        service.store.save(s)
        return dict(id=ddl_id, applied=True, statement_sha256=classification["statement_sha256"])


def index_signature(statement):
    """Normalize PG's index definition and a planned CREATE INDEX for recovery."""
    from pglast.stream import RawStream

    node = parse_sql(statement)[0].stmt
    if not isinstance(node, ast.IndexStmt):
        raise PreconditionError("DDL_INDEX_SIGNATURE", "Expected a CREATE INDEX definition")
    node.concurrent = False
    node.if_not_exists = False
    return RawStream()(node)


async def concurrent_source(service, sc, statement, plan_hash):
    """Reconcile a concurrent source operation across PostgreSQL's internal commits."""
    import json

    node = parse_sql(statement)[0].stmt
    creating = isinstance(node, ast.IndexStmt)
    if creating:
        schema, name = node.relation.schemaname, node.idxname
    else:
        schema, name = [x.sval for x in node.objects[0]]
    qualified = qn(schema, name)
    operation = "ddl_concurrent:" + plan_hash
    receipt = await sc.fetchval("SELECT details FROM lc.operations WHERE name=$1", operation)
    index = await sc.fetchrow(
        """SELECT c.oid::int,i.indisvalid,pg_get_indexdef(c.oid) AS definition
        FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)""",
        qualified,
    )
    if not receipt:
        if creating and index:
            raise PreconditionError("DDL_INDEX_EXISTS", "Concurrent CREATE requires an absent source index")
        if not creating and not index and not node.missing_ok:
            raise PreconditionError("DDL_INDEX_MISSING", "Concurrent DROP requires the planned source index")
        intent = dict(
            plan_hash=plan_hash,
            schema=schema,
            name=name,
            kind="create_index" if creating else "drop_index",
            oid=index["oid"] if index else None,
            definition=index["definition"] if index else None,
            complete=False,
        )
        await sc.execute(
            "INSERT INTO lc.operations(name,details) VALUES($1,$2::jsonb)", operation, json.dumps(intent)
        )
    else:
        intent = json.loads(receipt)
        if index and intent.get("oid") and index["oid"] != intent["oid"]:
            raise PreconditionError("DDL_INDEX_OWNERSHIP", "Source index OID changed after the intent")
        if creating and index:
            if index_signature(index["definition"]) != index_signature(statement):
                raise PreconditionError(
                    "DDL_INDEX_DRIFT", "Source index definition differs from the approved CREATE"
                )
            if not index["indisvalid"]:
                # The failed build belongs to the durable creation intent.
                await sc.fetchval("SELECT set_config('lc.managed','',false)")
                await sc.execute("DROP INDEX CONCURRENTLY " + qualified)
                await sc.fetchval("SELECT set_config('lc.managed','on',false)")
                index = None
    already_done = bool(creating and index and index["indisvalid"]) or (not creating and not index)
    if not already_done:
        try:
            await sc.execute(statement)
        finally:
            oid = await sc.fetchval("SELECT to_regclass($1)::oid::int", qualified)
            if oid:
                await sc.execute(
                    "UPDATE lc.operations SET details=details||jsonb_build_object('oid',$2::int) WHERE name=$1",
                    operation,
                    oid,
                )
    if not await sc.fetchval("SELECT EXISTS(SELECT FROM lc.ddl_log WHERE operation_key=$1)", plan_hash):
        # A concurrent operation can commit before its event-trigger transaction.
        # Verify the catalog then append the missing journal exactly once.
        if creating:
            definition = await sc.fetchval("SELECT pg_get_indexdef(to_regclass($1))", qualified)
            valid = await sc.fetchval(
                "SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)", qualified
            )
            if not valid or index_signature(definition) != index_signature(statement):
                raise PreconditionError(
                    "DDL_INDEX_POSTCONDITION", "Source concurrent index is not valid/matching"
                )
            import re

            target_statement = re.sub(
                r"^CREATE (UNIQUE )?INDEX ", r"CREATE \1INDEX CONCURRENTLY ", definition
            )
            jobs = [
                dict(
                    kind="create_index",
                    statement=target_statement,
                    schema=schema,
                    name=name,
                    definition=definition,
                )
            ]
        else:
            if await sc.fetchval("SELECT to_regclass($1) IS NOT NULL", qualified):
                raise PreconditionError("DDL_INDEX_POSTCONDITION", "Source concurrent DROP is incomplete")
            jobs = (
                [
                    dict(
                        kind="drop_index",
                        statement=statement,
                        schema=schema,
                        name=name,
                        definition=intent["definition"],
                    )
                ]
                if intent.get("definition")
                else []
            )
        await sc.fetchval("SELECT set_config('lc.caller_search_path',current_setting('search_path'),false)")
        await sc.fetchval(
            "SELECT lc.record_ddl($1,$2,$3::jsonb,$4::jsonb)",
            statement,
            "CREATE INDEX" if creating else "DROP INDEX",
            "[]",
            json.dumps(jobs),
        )
    await sc.execute(
        "UPDATE lc.operations SET details=details||'{\"complete\":true}'::jsonb WHERE name=$1", operation
    )
