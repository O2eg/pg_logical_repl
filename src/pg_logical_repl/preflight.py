"""Fail-closed preflight, including managed-provider capability checks."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from .catalog import inventory, public_inventory
from .db import Database


async def check(config, redactor, *, prepared=False):
    async with Database(config, "source", redactor).connect(readonly=True) as sc:
        source = await inventory(sc, include_passwords=True)
        user = await sc.fetchval("SELECT current_user")
        role = next((r for r in source["roles"] if r["rolname"] == user), {})
        repl = role.get("rolreplication") or role.get("rolsuper")
        if not repl:
            repl = await sc.fetchval(
                "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname='mdb_replication' AND pg_has_role(current_user,oid,'MEMBER'))"
            )
        versions = await sc.fetch("SELECT name,version FROM pg_available_extension_versions")
        source["available_extensions"] = [dict(x) for x in versions]
        source["wal_senders"] = await sc.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE backend_type='walsender'"
        )
    async with Database(config, "target", redactor).connect(readonly=True) as tc:
        target = await inventory(tc, include_passwords=True)
        target_role = dict(
            await tc.fetchrow("SELECT rolsuper,rolcreaterole FROM pg_roles WHERE rolname=current_user")
        )
        available = {
            (r["name"], r["version"])
            for r in await tc.fetch("SELECT name,version FROM pg_available_extension_versions")
        }
    checks = []

    def add(code, condition, message, fail="failed"):
        checks.append(dict(code=code, status="passed" if condition else fail, message=message))

    for side, inv in [("SOURCE", source), ("TARGET", target)]:
        for item in inv["unsupported_objects"]:
            add(
                side + "_OBJECT_PROFILE:" + item["category"] + ":" + item["identity"],
                False,
                "Object is not qualified for transfer and validation in the current profile",
            )

    profile = {
        ("publication", "mode"): "explicit_tables",
        ("publication", "include_schemas"): [],
        ("policies", "roles_scope"): "all",
        ("policies", "roles_passwords"): "required",
        ("policies", "existing_roles"): "fail_on_difference",
        ("policies", "extensions"): "exact_version",
        ("policies", "materialized_views"): "refresh_at_cutover",
        ("policies", "unlogged_tables"): "copy_at_cutover",
        ("policies", "tablespaces"): "preserve",
        ("policies", "target_access"): "closed",
        ("capacity", "mode"): "external_measurements",
        ("ddl", "drop_cascade"): "reject",
        ("ddl", "unknown"): "reject",
    }
    for (section, key), expected in profile.items():
        add(
            "QUALIFIED_POLICY:" + section + "." + key,
            config.get(section, key, expected) == expected,
            "Current qualified implementation requires " + section + "." + key + "=" + str(expected),
        )
    add("WAIVERS_QUALIFIED", not config.raw.get("waivers"), "Waiver application is not yet qualified")
    add(
        "REPLICATION_ROLE_CREATE",
        not config.raw["source"].get("replication", {}).get("create", False),
        "Use an existing replication account",
    )
    add(
        "STREAMING_PROFILE",
        config.get("subscription", "streaming", "auto") in ("auto", "on"),
        "Only streaming=on is qualified; auto selects on",
    )
    add(
        "BINARY_PROFILE",
        config.get("subscription", "binary", "auto") in ("auto", False),
        "Only text transfer is qualified; auto selects false",
    )
    add(
        "FAILOVER_PROFILE",
        config.get("subscription", "failover", "auto") in ("auto", False),
        "Automatic failover is not qualified; auto selects false",
    )
    add(
        "DISABLE_ON_ERROR_PROFILE",
        not config.get("subscription", "disable_on_error", False),
        "disable_on_error is not yet qualified",
    )

    si, ti = source["identity"], target["identity"]
    add(
        "SAME_MAJOR",
        si["major"] == ti["major"] and si["major"] in (15, 16, 17, 18),
        "Source and target must have the same major, 15–18",
    )
    add(
        "IDENTITY_KNOWN",
        bool(si["system_identifier"] and ti["system_identifier"]),
        "Both system identifiers must be readable",
    )
    add(
        "DISTINCT_INSTANCES",
        bool(si["system_identifier"] and si["system_identifier"] != ti["system_identifier"]),
        "Source and target must be distinct instances",
    )
    add(
        "PRIMARY_ENDPOINTS",
        not si["in_recovery"] and not ti["in_recovery"],
        "Both endpoints must be writable primaries",
    )
    add("SOURCE_REPLICATION_PRIVILEGE", bool(repl), "Replication role or provider capability is required")
    add(
        "SOURCE_WAL_LEVEL",
        source["settings"].get("wal_level") == "logical",
        "Source wal_level must be logical",
    )
    add(
        "TARGET_ADMIN_PRIVILEGE",
        target_role["rolsuper"],
        "Current qualified profile requires target superuser for roles and native subscription setup",
    )
    for key in ["encoding", "datcollate", "datctype", "datlocprovider", "datcollversion", "locale"]:
        add("COMPATIBLE_" + key.upper(), si[key] == ti[key], f"{key}: source={si[key]!r}, target={ti[key]!r}")
    if not prepared:
        objects = [r for r in target["relations"] if not r["extension"]]
        schemas = [
            s
            for s in target["schemas"]
            if s["name"] != "public" and not any(e["schema"] == s["name"] for e in target["extensions"])
        ]
        add(
            "TARGET_EMPTY",
            not objects
            and not target["functions"]
            and not target["large_objects"]
            and not schemas
            and not target["types"]
            and not target["default_acl"]
            and not target["unsupported_objects"],
            "Target must have no application objects or large objects",
        )
    add(
        "SOURCE_HASH_ACCESS",
        source["password_access"],
        "All password verifiers must be readable; pg_roles masked values are not valid exports",
    )
    add(
        "TARGET_HASH_ACCESS",
        target["password_access"],
        "Existing target role attributes and hashes must be comparable",
    )
    for e in source["extensions"]:
        installed = next((x for x in target["extensions"] if x["name"] == e["name"]), None)
        add(
            "EXTENSION_INSTALLED:" + e["name"],
            not installed or installed["version"] == e["version"],
            "An already installed target extension must have the exact source version",
        )
        add(
            "EXTENSION_AVAILABLE:" + e["name"],
            (e["name"], e["version"]) in available,
            f"Target needs extension {e['name']} version {e['version']}",
        )
        add(
            "EXTENSION_DATA:" + e["name"],
            not e["extconfig"],
            f"Extension {e['name']} configuration data needs a qualified handler",
        )
    add(
        "EVENT_TRIGGER_PRIVILEGE",
        role.get("rolsuper", False),
        "The current local profile enforces DDL freeze with event triggers in every mode",
    )
    for r in source["relations"]:
        if r["extension"]:
            continue
        name = r["schema"] + "." + r["name"]
        if r["kind"] in ("r", "p") and r["persistence"] == "p":
            valid = (
                (r["replica_identity"] == "d" and r["has_pk"])
                or (r["replica_identity"] == "i" and r["has_identity_index"])
                or r["replica_identity"] == "f"
            )
            add(
                "REPLICA_IDENTITY:" + name,
                valid
                or (
                    config.get("policies", "replica_identity", "fail") == "full_if_needed"
                    and r["replica_identity"] == "d"
                    and not r["has_pk"]
                ),
                f"Table {name} needs replica identity for UPDATE/DELETE",
            )
            if r["replica_identity"] == "f" or (
                not valid and config.get("policies", "replica_identity", "fail") == "full_if_needed"
            ):
                add(
                    "FULL_IDENTITY_TYPES:" + name,
                    all(c["full_identity_equality"] for c in r["columns"]),
                    "FULL requires qualified equality support for every column; polymorphic/custom types need separate qualification",
                )
        if r["kind"] == "f":
            add("FOREIGN_TABLE:" + name, False, "Foreign data dependencies require explicit qualification")
        for t in r["triggers"]:
            add(
                "REPLICA_TRIGGER:" + name + ":" + t["tgname"],
                t["tgenabled"] not in ("A", "R"),
                "ALWAYS/REPLICA application trigger needs controlled deferred restoration",
            )
    add(
        "SOURCE_TOPOLOGY",
        not source["subscriptions"] and not source["publications"] if not prepared else True,
        "Existing replication topology requires explicit qualification",
    )
    add(
        "TARGET_TOPOLOGY",
        not target["subscriptions"] if not prepared else True,
        "Target must not have unrelated subscriptions",
    )
    add(
        "APPLICATION_EVENT_TRIGGERS",
        not [
            t
            for t in source["event_triggers"]
            if not prepared or t["evtname"] not in {"lc_guard", "lc_capture", "lc_rewrite_guard"}
        ],
        "Application event triggers require qualified side-effect handling",
    )
    if not prepared:
        workers = 1 + int(target["settings"].get("max_sync_workers_per_subscription", "2"))
        add(
            "SOURCE_SLOT_CAPACITY",
            int(source["settings"].get("max_replication_slots", "0")) - len(source["slots"]) >= workers,
            f"Need capacity for one main slot and {workers - 1} tablesync slots",
        )
        add(
            "SOURCE_SENDER_CAPACITY",
            int(source["settings"].get("max_wal_senders", "0")) - source["wal_senders"] >= workers,
            "Need WAL sender capacity for main and tablesync connections",
        )
        add("TARGET_DURABILITY", target["settings"].get("fsync") == "on", "Target fsync must be on")
    target_roles = {r["rolname"]: r for r in target["roles"]}
    for r in source["roles"]:
        other = target_roles.get(r["rolname"])
        if other:
            same = all(other.get(k) == v for k, v in r.items())
            add(
                "ROLE_MATCH:" + r["rolname"],
                same,
                f"Existing role {r['rolname']} must match attributes and verifier",
            )
    evidence = config.get("capacity", "evidence_file")
    ok = False
    if evidence:
        try:
            data = json.loads((config.path.parent / evidence).read_text())
            age = (datetime.now(UTC) - datetime.fromisoformat(data["measured_at"])).total_seconds()
            ok = 0 <= age <= config.get("capacity", "max_age_seconds", 300) and all(
                data[s]["system_identifier"] == i["system_identifier"]
                and data[s]["free_bytes"] > data[s]["required_bytes"] >= 0
                for s, i in [("source", si), ("target", ti)]
            )
        except (OSError, ValueError, KeyError, TypeError):
            pass
    add(
        "CAPACITY_EVIDENCE",
        ok,
        "Fresh free/required bytes and matching identities are required for both hosts",
        fail="unknown",
    )
    add("TARGET_SOURCE_ROUTE", False, "Actual subscription route is probed during prepare", fail="deferred")
    blockers = [x for x in checks if x["status"] in ("failed", "unknown")]
    return (
        dict(
            checks=checks,
            blockers=blockers,
            passed=not blockers,
            source=public_inventory(source),
            target=public_inventory(target),
        ),
        source,
        target,
    )
