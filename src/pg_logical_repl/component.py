"""Optional pg_play transport; the native result and state formats stay unchanged."""

from . import __version__
from .capabilities import describe

CONTRACT = "pg_play/component/v2"
EXIT_CODES = dict(
    success=0,
    validation_error=2,
    precondition_failed=3,
    unsupported=4,
    partial=5,
    execution_error=6,
    cancelled=7,
    ownership_error=8,
)


def capabilities():
    native = describe()
    commands = {
        name: {
            **metadata,
            "accepts_plan_hash": metadata["requires_plan"] is True,
        }
        for name, metadata in native["commands"].items()
    }
    commands["capabilities"] = dict(
        mutates_target=False,
        machine_output=True,
        accepts_plan_hash=False,
    )
    return dict(
        capability_schema_version="pg_play/capabilities/v1",
        contract_version=CONTRACT,
        component="pg_logical_repl",
        component_version=__version__,
        commands=commands,
        exit_codes=EXIT_CODES,
        machine_interface=dict(
            machine_flag="--machine",
            request_id_option="--request-id",
            capabilities_option="--component-capabilities",
        ),
        secret_policy=dict(credentials="references_only", output="redacted"),
        contract_option=["--contract", CONTRACT],
        native=native,
    )


def envelope(value, code):
    result = dict(value)
    result["contract_version"] = CONTRACT
    result["component"] = "pg_logical_repl"
    result["component_version"] = result.pop("tool_version")
    result.pop("tool")
    result["command"] = result["command"] or "parse"
    if code in {130, 143}:
        code = EXIT_CODES["cancelled"]
    elif code == 3:
        result["status"] = "blocked"
    elif code == 0 and result["command"] == "plan":
        result["status"] = "planned"
    # A successful inspection is distinct from a healthy migration. The result
    # always retains the native state, including pending cleanup and BLOCKED.
    return result, code
