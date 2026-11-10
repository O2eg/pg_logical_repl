import json

import pytest
import yaml

from pg_logical_repl.cli import parser
from pg_logical_repl.config import load
from pg_logical_repl.errors import ConfigurationError
from pg_logical_repl.integrity import role_view


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("daemon", "max_parallel_index_builds", 2),
        ("publication", "extension_tables", "skip_identical"),
        ("publication", "mode", "all_tables"),
        ("ddl", "contact", "operator"),
        ("policies", "roles_scope", "database_related"),
        ("policies", "existing_roles", "reconcile_existing"),
        ("policies", "target_access", "read_only"),
        ("subscription", "binary", True),
        ("subscription", "failover", True),
        ("subscription", "streaming", "parallel"),
        ("cutover", "analyze", "unknown"),
        ("limits", "connect_timeout_seconds", 0),
    ],
)
def test_unsupported_options_rejected_offline(tmp_path, section, key, value):
    endpoint = dict(host="localhost", database="db", user="user")
    config = dict(
        schema_version="pg_logical_connector/config-v2",
        migration_id="test",
        state_dir="state",
        source=dict(admin=endpoint),
        target=dict(admin=endpoint),
    )
    config[section] = {key: value}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    with pytest.raises(ConfigurationError):
        load(path)


def test_role_view_preserves_null_and_secrecy_and_stable_order():
    inv = dict(
        password_access=True,
        roles=[dict(rolname="app", rolpassword="SCRAM-secret", rolsuper=False)],
        memberships=[
            dict(role="second", member="app", grantor="admin"),
            dict(role="first", member="app", grantor="admin"),
        ],
    )
    value = role_view(inv, "private-token", "executor")
    assert "SCRAM-secret" not in json.dumps(value)
    inv["memberships"].reverse()
    assert value == role_view(inv, "private-token", "executor")
    inv["roles"][0]["rolpassword"] = None
    assert value != role_view(inv, "private-token", "executor")


def test_emergency_and_repair_cli_plans():
    args = parser().parse_args(["plan", "abort", "--source-unavailable", "--reason", "source lost"])
    assert args.source_unavailable and args.reason == "source lost"
    args = parser().parse_args(["plan", "resync-table", "--table", "public.app", "--reason", "repair"])
    assert args.table == "public.app"
    for command in ("pause", "resume", "resolve", "cleanup", "resync-table"):
        assert parser().parse_args([command, "--plan", "plan.json", "--plan-hash", "hash"]).command == command
