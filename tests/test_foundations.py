import json
import sys

import pytest
import yaml

from pg_logical_repl.cli import main
from pg_logical_repl.config import canonical, load
from pg_logical_repl.ddl import classify
from pg_logical_repl.errors import ConfigurationError, ConnectorError, OwnershipError, PreconditionError
from pg_logical_repl.process import execute
from pg_logical_repl.security import Redactor, private_file
from pg_logical_repl.state import Store, write_private


def config(tmp_path, **changes):
    endpoint = dict(
        host="127.0.0.1", database="test", user="postgres", password_env="LC_TEST_PASSWORD", sslmode="disable"
    )
    value = dict(
        schema_version="pg_logical_connector/config-v2",
        migration_id="example",
        state_dir="state",
        source=dict(admin=endpoint),
        target=dict(admin=endpoint),
    )
    value.update(changes)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_reject_duplicate_and_inline_secrets(tmp_path):
    p = config(tmp_path)
    p.write_text(p.read_text() + "migration_id: second\n")
    with pytest.raises(ConfigurationError, match="Duplicate"):
        load(p)
    p = config(tmp_path)
    p.write_text(p.read_text().replace("password_env: LC_TEST_PASSWORD", "password: secret"))
    with pytest.raises(ConfigurationError, match="Unknown"):
        load(p)


@pytest.mark.parametrize(
    "changes",
    [
        {"limits": {"query_timeout_seconds": True}},
        {"daemon": {"poll_interval_seconds": -1}},
        {"source": {"admin": {"host": 42, "database": "test", "user": "postgres"}}},
        {"subscription": {"streaming": "invalid"}},
        {"validation": {"online": "yes"}},
        {"validation": {"chunk_rows": 0}},
        {"validation": {"max_parallel": 0}},
        {"waivers": [{"object": "anything"}]},
    ],
)
def test_configuration_types(tmp_path, changes):
    with pytest.raises(ConfigurationError):
        load(config(tmp_path, **changes))


def test_yaml_on_is_string_and_canonical_unicode(tmp_path):
    p = config(tmp_path)
    p.write_text(p.read_text() + "subscription:\n  streaming: on\n")
    assert load(p).get("subscription", "streaming") == "on"
    assert canonical({"\ue000": 1, "😀": 2}).decode() == '{"😀":2,"\ue000":1}'
    assert canonical({"n": 1.0}) == b'{"n":1}'


def test_secret_files_and_redaction(tmp_path, monkeypatch):
    secret = "odd'\\:$VALUE\nsecond-line"
    r = Redactor()
    r.add(secret)
    assert secret not in r.text({"message": secret})
    assert r.clean({"password": secret, "ownership_token": "private"}) == {
        "password": "[REDACTED]",
        "ownership_token": "[REDACTED]",
    }
    p = tmp_path / "password"
    write_private(p, secret)
    assert private_file(p) == secret
    p.chmod(0o644)
    with pytest.raises(ConfigurationError):
        private_file(p)
    p.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(p)
    with pytest.raises(ConfigurationError):
        private_file(link)


def test_events_and_exclusion(tmp_path):
    directory = tmp_path / "private"
    s = Store(directory, "example", Redactor())
    s.initialize()
    for i in range(5):
        s.event("step", value=i)
    assert [x["sequence"] for x in s.events(after=2)] == [3, 4, 5]
    assert [x["sequence"] for x in s.events(after=0, limit=2)] == [1, 2]
    second = Store(directory, "example", Redactor())
    with s.lock(), pytest.raises(OwnershipError):
        with second.lock():
            pass
    with (directory / "events.jsonl").open("ab") as f:
        f.write(b'{"incomplete"')
    with pytest.raises(ConnectorError, match="incomplete"):
        s.event("next")


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE public.t ADD COLUMN x integer",
        "ALTER TABLE public.t ADD COLUMN x text",
        "ALTER TABLE public.t DROP COLUMN x",
        "ALTER TABLE public.t RENAME COLUMN a TO b",
    ],
)
def test_qualified_ddl(statement):
    assert classify(statement)["classification"] == "in_stream"


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE public.t ADD COLUMN x integer; DELETE FROM public.t",
        "ALTER TABLE public.t ADD COLUMN x int DEFAULT dangerous()",
        "ALTER TABLE public.t DROP COLUMN x CASCADE",
        "CREATE ROLE evil SUPERUSER",
        "ALTER TABLE t ADD COLUMN x integer",
        "ALTER TABLE lc.ddl_log DROP COLUMN statement",
        "DO $$ BEGIN EXECUTE 'ALTER TABLE public.t ADD COLUMN x text'; END $$",
    ],
)
def test_unqualified_ddl_rejected(statement):
    with pytest.raises(PreconditionError):
        classify(statement)


def test_machine_parse_failure(capsys):
    assert main(["--machine", "unknown-command"]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["tool"] == "pg-logical-repl"
    assert result["contract_version"] == "pg_logical_connector/result-v2"
    assert result["error"]["code"] == "CONFIGURATION_ERROR"


async def test_client_metadata_never_silently_truncated(tmp_path):
    command = [sys.executable, "-c", "print('x'*1100000)"]
    with pytest.raises(ConnectorError, match="truncated metadata"):
        await execute(command, redactor=Redactor())
    path = tmp_path / "large-output"
    await execute(command, stdout_path=path, redactor=Redactor())
    assert path.stat().st_size == 1100001
    assert path.stat().st_mode & 0o777 == 0o600
