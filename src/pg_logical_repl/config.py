"""Strict, versioned YAML configuration with secret references only."""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import rfc8785
import yaml

from .errors import ConfigurationError


class UniqueLoader(yaml.SafeLoader):
    pass


# YAML 1.2 boolean spelling: replication settings on/off must stay strings.
UniqueLoader.yaml_implicit_resolvers = copy.deepcopy(yaml.SafeLoader.yaml_implicit_resolvers)
for first, resolvers in UniqueLoader.yaml_implicit_resolvers.items():
    UniqueLoader.yaml_implicit_resolvers[first] = [
        (tag, rx) for tag, rx in resolvers if tag != "tag:yaml.org,2002:bool"
    ]
UniqueLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(true|false|True|False|TRUE|FALSE)$"), list("tTfF")
)


def mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise ConfigurationError("Configuration keys must be strings")
        if key in result:
            raise ConfigurationError(f"Duplicate YAML key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)

ENDPOINT_KEYS = set(
    "host hostaddr port database user password_env password_file passfile sslmode sslrootcert sslcert sslkey connect_timeout_seconds sslrootcert_on_target create".split()
)
SECTIONS = {
    "source": {"admin", "replication"},
    "target": {"admin"},
    "client_tools": {"bin_dirs"},
    "publication": {"mode", "include_schemas", "extension_tables"},
    "subscription": {"streaming", "binary", "failover", "disable_on_error"},
    "ddl": {
        "mode",
        "new_tables_without_pk",
        "rewrite_max_table_bytes",
        "set_not_null_max_table_bytes",
        "drop_cascade",
        "unknown",
        "contact",
    },
    "policies": set(
        "roles_scope roles_passwords existing_roles extensions replica_identity materialized_views unlogged_tables tablespaces target_access".split()
    ),
    "limits": set(
        "lock_timeout_seconds query_timeout_seconds connect_timeout_seconds catchup_lag_seconds catchup_stable_seconds alert_lag_seconds alert_slot_retained_bytes alert_refresh_age_seconds alert_queue_age_seconds operation_timeout_seconds".split()
    ),
    "capacity": {"mode", "evidence_file", "max_age_seconds"},
    "sequences": {"presync_interval_seconds"},
    "validation": {
        "online",
        "chunk_rows",
        "max_parallel",
        "pause_between_chunks_ms",
        "max_retries",
        "cutover_level",
    },
    "cutover": {"share_locks", "analyze", "max_validation_age_seconds"},
    "daemon": {
        "poll_interval_seconds",
        "drift_interval_seconds",
        "max_parallel_index_builds",
        "queue_max_attempts",
        "metrics_textfile",
    },
}


def canonical(value) -> bytes:
    try:
        return rfc8785.dumps(value)
    except (ValueError, TypeError) as exc:
        raise ConfigurationError("Value is not canonical JSON: " + str(exc)) from None


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def keys(value, allowed, location):
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    unknown = set(value) - allowed
    if unknown:
        raise ConfigurationError(f"Unknown fields in {location}: {', '.join(sorted(unknown))}")


@dataclass(frozen=True)
class Config:
    path: Path
    raw: dict

    @property
    def migration_id(self):
        return self.raw["migration_id"]

    @property
    def state_dir(self):
        return (self.path.parent / self.raw["state_dir"]).absolute()

    @property
    def hash(self):
        return digest(self.raw)

    @property
    def control_hash(self):
        # Scheduling and validation limits may be tuned between daemon ticks.
        # Connection, publication and server-installed DDL policies may not.
        value = {
            k: v
            for k, v in self.raw.items()
            if k not in {"daemon", "limits", "validation", "sequences", "cutover"}
        }
        value["server_lock_timeout"] = self.get("limits", "lock_timeout_seconds", 5)
        return digest(value)

    @property
    def ddl_mode(self):
        return self.get("ddl", "mode", "frozen")

    @property
    def publication(self):
        return "lc_pub_" + self.migration_id

    @property
    def subscription(self):
        return "lc_sub_" + self.migration_id

    def get(self, section, key, default=None):
        return self.raw.get(section, {}).get(key, default)

    def endpoint(self, side: str, purpose: str = "admin") -> dict:
        base = dict(self.raw[side]["admin"])
        if purpose == "replication":
            base.update(self.raw[side].get("replication", {}))
            if any(
                k in self.raw[side].get("replication", {})
                for k in ("password_env", "password_file", "passfile")
            ):
                for k in ("password_env", "password_file", "passfile"):
                    if k not in self.raw[side]["replication"]:
                        base.pop(k, None)
        base.setdefault("port", 5432)
        base.setdefault("sslmode", "verify-full")
        return base


def load(path: str | Path) -> Config:
    path = Path(path).expanduser().resolve()
    try:
        raw = yaml.load(path.read_text(), Loader=UniqueLoader)
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"Cannot read YAML configuration ({type(exc).__name__})") from None
    keys(raw, set(SECTIONS) | {"schema_version", "migration_id", "mode", "state_dir", "waivers"}, "config")
    if raw.get("schema_version") != "pg_logical_connector/config-v2":
        raise ConfigurationError("Expected schema_version pg_logical_connector/config-v2")
    if not isinstance(raw.get("migration_id"), str) or not re.fullmatch(
        r"[a-z0-9_]{3,40}", raw["migration_id"]
    ):
        raise ConfigurationError("migration_id must match [a-z0-9_]{3,40}")
    if not isinstance(raw.get("state_dir"), str) or not raw["state_dir"]:
        raise ConfigurationError("state_dir is required")
    if raw.get("mode", "migration") not in {"migration", "replica"}:
        raise ConfigurationError("mode must be migration or replica")
    for section, allowed in SECTIONS.items():
        if section in raw:
            keys(raw[section], allowed, section)
    for side in ("source", "target"):
        if not isinstance(raw.get(side, {}).get("admin"), dict):
            raise ConfigurationError(f"{side}.admin is required")
        for purpose, endpoint in raw[side].items():
            keys(endpoint, ENDPOINT_KEYS, side + "." + purpose)
            for key, value in endpoint.items():
                if key == "create":
                    if type(value) is not bool:
                        raise ConfigurationError("create must be a boolean")
                elif key in {"port", "connect_timeout_seconds"}:
                    if type(value) is not int or value <= 0:
                        raise ConfigurationError(key + " must be a positive integer")
                elif not isinstance(value, str) or not value or "\x00" in value:
                    raise ConfigurationError(side + "." + purpose + "." + key + " must be a non-empty string")
            if purpose == "admin":
                for key in ("host", "database", "user"):
                    if not isinstance(endpoint.get(key), str) or not endpoint[key]:
                        raise ConfigurationError(f"{side}.admin.{key} is required")
            if "port" in endpoint and (
                type(endpoint["port"]) is not int or not 1 <= endpoint["port"] <= 65535
            ):
                raise ConfigurationError("port must be an integer from 1 to 65535")
            if endpoint.get("sslmode", "verify-full") not in {
                "verify-full",
                "verify-ca",
                "require",
                "disable",
            }:
                raise ConfigurationError("Unsupported sslmode")
            if sum(bool(endpoint.get(k)) for k in ("password_env", "password_file", "passfile")) > 1:
                raise ConfigurationError("Only one credential reference is allowed per endpoint")
    enums = {
        ("ddl", "mode"): {"frozen", "captured", "managed_only"},
        ("publication", "mode"): {"explicit_tables", "all_tables"},
        ("policies", "roles_scope"): {"all", "database_related"},
        ("policies", "roles_passwords"): {"required", "waiver"},
        ("policies", "replica_identity"): {"fail", "full_if_needed"},
        ("policies", "existing_roles"): {"fail_on_difference", "reconcile_existing"},
        ("validation", "cutover_level"): {"counts", "checksums", "full"},
    }
    for (section, key), allowed in enums.items():
        if key in raw.get(section, {}) and raw[section][key] not in allowed:
            raise ConfigurationError(f"Unsupported {section}.{key}")
    types = {
        "limits": {key: int for key in SECTIONS["limits"]},
        "capacity": {"mode": str, "evidence_file": str, "max_age_seconds": int},
        "sequences": {"presync_interval_seconds": int},
        "validation": {
            "online": bool,
            "chunk_rows": int,
            "max_parallel": int,
            "pause_between_chunks_ms": int,
            "max_retries": int,
            "cutover_level": str,
        },
        "cutover": {"share_locks": bool, "analyze": str, "max_validation_age_seconds": int},
        "daemon": {key: (str if key == "metrics_textfile" else int) for key in SECTIONS["daemon"]},
        "ddl": {key: (int if key.endswith("_bytes") else str) for key in SECTIONS["ddl"]},
        "policies": {key: str for key in SECTIONS["policies"]},
        "publication": {"mode": str, "include_schemas": list, "extension_tables": str},
    }
    for section, fields in types.items():
        for key, value in raw.get(section, {}).items():
            if type(value) is not fields[key] or (fields[key] is int and value < 0):
                raise ConfigurationError(f"Invalid type or range for {section}.{key}")
    for key in ("chunk_rows", "max_parallel"):
        if key in raw.get("validation", {}) and raw["validation"][key] == 0:
            raise ConfigurationError("validation." + key + " must be positive")
    for key, value in raw.get("subscription", {}).items():
        allowed = {
            "streaming": {"auto", "on", "off", "parallel"},
            "binary": {"auto", True, False},
            "failover": {"auto", True, False},
            "disable_on_error": {True, False},
        }[key]
        if type(value) not in {str, bool} or value not in allowed:
            raise ConfigurationError("Invalid subscription." + key)
    if "client_tools" in raw:
        directories = raw["client_tools"].get("bin_dirs", {})
        keys(directories, {"15", "16", "17", "18"}, "client_tools.bin_dirs")
        if any(not isinstance(v, str) or not v for v in directories.values()):
            raise ConfigurationError("Client tool paths must be strings")
    if any(not isinstance(v, str) or not v for v in raw.get("publication", {}).get("include_schemas", [])):
        raise ConfigurationError("include_schemas must contain non-empty strings")
    if not isinstance(raw.get("waivers", []), list):
        raise ConfigurationError("waivers must be a list")
    for w in raw.get("waivers", []):
        keys(w, {"object", "decision", "reason", "approved_by", "expires_at"}, "waiver")
        if not all(
            isinstance(w.get(k), str) and w[k]
            for k in ("object", "decision", "reason", "approved_by", "expires_at")
        ):
            raise ConfigurationError("Waivers require object/decision/reason/approved_by/expires_at")
    ddl = raw.get("ddl", {})
    if ddl.get("new_tables_without_pk", "reject") not in {"reject", "replica_identity_full"}:
        raise ConfigurationError("ddl.new_tables_without_pk must be reject or replica_identity_full")
    for key in ("rewrite_max_table_bytes", "set_not_null_max_table_bytes"):
        if key in ddl and (type(ddl[key]) is not int or ddl[key] < 0):
            raise ConfigurationError("ddl." + key + " must be a non-negative integer")
    for key in ("queue_max_attempts", "max_parallel_index_builds"):
        value = raw.get("daemon", {}).get(key, 1)
        if type(value) is not int or value < 1:
            raise ConfigurationError("daemon." + key + " must be a positive integer")
    validate_profile(raw)
    return Config(path, raw)


# Accepted configuration must describe executable behavior, including offline validate.
def validate_profile(raw):
    fixed = {
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
        ("daemon", "max_parallel_index_builds"): 1,
    }
    for (section, key), value in fixed.items():
        if raw.get(section, {}).get(key, value) != value:
            raise ConfigurationError(f"Implemented profile requires {section}.{key}={value!r}")
    for section, key in [("publication", "extension_tables"), ("ddl", "contact")]:
        if key in raw.get(section, {}):
            raise ConfigurationError(f"{section}.{key} is not implemented; omit it")
    if raw.get("mode", "migration") != "migration":
        raise ConfigurationError("mode=replica is not implemented")
    if raw.get("waivers"):
        raise ConfigurationError("Waivers are not implemented")
    for side in ("source", "target"):
        if "hostaddr" in raw[side]["admin"]:
            raise ConfigurationError("admin.hostaddr is not supported by the asyncpg transport; use host")
        if bool(raw[side]["admin"].get("sslcert")) != bool(raw[side]["admin"].get("sslkey")):
            raise ConfigurationError("Administrative sslcert and sslkey must be supplied together")
    if "sslrootcert_on_target" in raw["target"]["admin"]:
        raise ConfigurationError("target.admin requires sslrootcert, not sslrootcert_on_target")
    if "sslrootcert" in raw["source"].get("replication", {}):
        raise ConfigurationError("source.replication requires sslrootcert_on_target, not sslrootcert")
    if any(k in raw["source"].get("replication", {}) for k in ("sslcert", "sslkey")):
        raise ConfigurationError("Replication client certificates on target are not implemented")
    for side in ("source", "target"):
        for endpoint in raw[side].values():
            if endpoint.get("create"):
                raise ConfigurationError("Automatic account creation is not implemented")
    for key, allowed in {
        "streaming": ("auto", "on"),
        "binary": ("auto", False),
        "failover": ("auto", False),
        "disable_on_error": (False,),
    }.items():
        if key in raw.get("subscription", {}) and raw["subscription"][key] not in allowed:
            raise ConfigurationError(f"subscription.{key} is outside the implemented profile")
    if raw.get("cutover", {}).get("analyze", "never") not in {"never", "all"}:
        raise ConfigurationError("cutover.analyze must be never or all")
    for section, key in [
        ("daemon", "poll_interval_seconds"),
        ("limits", "operation_timeout_seconds"),
        ("limits", "connect_timeout_seconds"),
    ]:
        if raw.get(section, {}).get(key, 1) <= 0:
            raise ConfigurationError(f"{section}.{key} must be positive")
