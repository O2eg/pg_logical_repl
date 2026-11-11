"""The optional pg_play transport must not change the native CLI contract."""

import asyncio
import io
import json
import logging
from contextlib import redirect_stderr
from types import SimpleNamespace

import pytest
import yaml

from pg_logical_repl import cli, logging_config
from pg_logical_repl.component import CONTRACT, capabilities


@pytest.mark.parametrize(
    "args,contract",
    [
        (["--capabilities"], "pg_logical_connector/result-v2"),
        (["--component-capabilities"], CONTRACT),
        (["--contract", CONTRACT, "--capabilities"], CONTRACT),
        (["--contract=" + CONTRACT, "--capabilities"], CONTRACT),
    ],
)
def test_capability_transports(args, contract, capsys):
    assert cli.main(["--machine", "--request-id", "probe", *args]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["contract_version"] == contract
    assert value["request_id"] == "probe"
    if contract == CONTRACT:
        assert value["component"] == "pg_logical_repl"
        assert value["result"] == capabilities()
        assert "tool" not in value
    else:
        assert value["tool"] == "pg-logical-repl"


def test_parser_errors_use_selected_contract(capsys):
    assert cli.main(["--contract", CONTRACT, "--machine", "--bad-option"]) == 2
    value = json.loads(capsys.readouterr().out)
    assert value["contract_version"] == CONTRACT
    assert value["error"]["code"] == "CONFIGURATION_ERROR"
    assert value["command"] == "parse"


@pytest.mark.parametrize("option", [["--request-id", "probe"], ["--request-id=probe"]])
def test_parser_errors_echo_the_supplied_request_id(option, capsys):
    assert cli.main(["--contract", CONTRACT, "--machine", *option, "no-such-command"]) == 2
    assert json.loads(capsys.readouterr().out)["request_id"] == "probe"


@pytest.fixture
def configuration(tmp_path):
    endpoint = dict(host="localhost", database="test", user="postgres", password_env="TEST_SECRET")
    path = tmp_path / "migration.yaml"
    path.write_text(
        yaml.safe_dump(
            dict(
                schema_version="pg_logical_connector/config-v2",
                migration_id="transport_test",
                state_dir="state",
                source=dict(admin=endpoint),
                target=dict(admin=endpoint),
            )
        )
    )
    return str(path)


def test_config_description_never_resolves_credentials(configuration, capsys, monkeypatch):
    monkeypatch.delenv("TEST_SECRET", raising=False)
    assert cli.main(["--contract", CONTRACT, "--machine", "--config", configuration, "inspect-config"]) == 0
    value = json.loads(capsys.readouterr().out)["result"]
    assert value["migration_id"] == "transport_test"
    assert value["endpoints"]["source"]["port"] == 5432
    assert "password" not in json.dumps(value)


@pytest.mark.parametrize("cancel,code", [(False, 0), (True, 7)])
def test_inspection_state_and_signal_exit_code(configuration, capsys, monkeypatch, cancel, code):
    class Fake:
        def __init__(self, config, redactor):
            self.store = SimpleNamespace()
            redactor.add("super-secret-value")

        async def status(self):
            if cancel:
                raise asyncio.CancelledError
            return dict(state="BLOCKED", blocked_details="super-secret-value")

    monkeypatch.setattr(cli, "Service", Fake)
    assert cli.main(["--machine", "--contract", CONTRACT, "--config", configuration, "status"]) == code
    output = capsys.readouterr().out
    assert "super-secret-value" not in output
    value = json.loads(output)
    if cancel:
        assert value["status"] == "cancelled"
    else:
        assert value["status"] == "succeeded"
        assert value["result"]["state"] == "BLOCKED"


def test_logging_preserves_external_handlers_and_logger_settings(tmp_path):
    logger = logging.getLogger("pg_logical_repl")
    previous = logger.level, logger.propagate
    external_stream, stderr = io.StringIO(), io.StringIO()
    external = logging.StreamHandler(external_stream)
    logger.addHandler(external)
    try:
        with redirect_stderr(stderr):
            logging_config.configure("info", tmp_path / "events.log")
            logger.info("first event")
        # sys.stderr changed, but the configured stderr is still externally owned.
        logging_config.close()
        logging_config.close()
        assert not stderr.closed and not external_stream.closed
        assert external in logger.handlers
        assert (logger.level, logger.propagate) == previous
        assert "first event" in external_stream.getvalue()
        assert (tmp_path / "events.log").read_text().strip() == "first event"
    finally:
        logger.removeHandler(external)
        external.close()
