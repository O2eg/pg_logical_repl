"""Record local test evidence and check a freshly installed wheel without secrets."""

import argparse
import hashlib
import json
import os
import subprocess
import tomllib
import xml.etree.ElementTree as ET
import zipfile
from datetime import UTC, datetime
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit-prefix", required=True)
    parser.add_argument("--regression-glob", action="append", default=[])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    wheel = root / f"dist/pg_logical_repl-{version}-py3-none-any.whl"
    smoke = root / ".runtime/wheel-smoke-release/bin"
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "LC_CONFIG"}}
    suites = {}
    for major in [15, 16, 17, 18]:
        path = root / f"docs/{args.junit_prefix}_pg{major}.xml"
        result = ET.parse(path).getroot().find("testsuite")
        assert result is not None
        fields = {k: int(result.attrib[k]) for k in ["tests", "failures", "errors", "skipped"]}
        assert fields["tests"] and not any(fields[k] for k in ["failures", "errors", "skipped"])
        suites[str(major)] = dict(
            **fields,
            seconds=float(result.attrib["time"]),
            timestamp=result.attrib["timestamp"],
            junit=str(path.relative_to(root)),
            integration_tests=sum(
                "integration" in x.get("classname", "") for x in result.findall("testcase")
            ),
        )
    regressions = {}
    for path in sorted({p for pattern in args.regression_glob for p in (root / "docs").glob(pattern)}):
        result = ET.parse(path).getroot().find("testsuite")
        assert result is not None
        fields = {k: int(result.attrib[k]) for k in ("tests", "failures", "errors", "skipped")}
        assert fields["tests"] and not any(fields[k] for k in ("failures", "errors", "skipped"))
        regressions[path.stem] = dict(
            **fields, seconds=float(result.attrib["time"]), junit=str(path.relative_to(root))
        )
    output = subprocess.check_output(
        [str(smoke / "pg-logical-repl"), "--machine", "--capabilities"],
        text=True,
        cwd="/tmp",
        env=env,
    )
    capabilities = json.loads(output)
    assert capabilities["status"] == "succeeded"
    assert capabilities["tool_version"] == version
    assert capabilities["result"]["validation"]["payload_rows_returned"] is False
    subprocess.run(
        [
            str(smoke / "python"),
            "-c",
            "from importlib.resources import files; "
            "r=files('pg_logical_repl').joinpath('sql'); "
            "assert all(r.joinpath(n+'.sql').read_text() for n in ['base','ddl_support','freeze','capture','replay'])",
        ],
        check=True,
        cwd="/tmp",
        env=env,
    )
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert not any(".pg_stand" in n or n.endswith((".key", ".pgpass")) for n in names)
        # Assert installed code is the wheel's code, not an editable source checkout.
        site = next((root / ".runtime/wheel-smoke-release/lib").glob("python*/site-packages"))
        for name in names:
            if name.startswith("pg_logical_repl/") and not name.endswith("/"):
                assert (site / name).read_bytes() == archive.read(name), name
                assert (root / "src" / name).read_bytes() == archive.read(name), name
        package_bytes = b"\n".join(archive.read(n) for n in names if not n.endswith("/"))
    creds = json.loads((root / ".pg_stand/credentials/database/passwords.json").read_text())
    passwords = set()

    def collect(value):
        if isinstance(value, dict):
            for k, v in value.items():
                if k == "password" and isinstance(v, str) and v:
                    passwords.add(v.encode())
                collect(v)
        elif isinstance(value, list):
            for v in value:
                collect(v)

    collect(creds)
    assert passwords, "No stand passwords found for the secret scan"
    paths = [root / "README.md", root / "IMPLEMENTATION_STATUS.md", root / "pyproject.toml"]
    paths += [
        p
        for base in ["src", "tests", "infra", "docs"]
        for p in (root / base).rglob("*")
        if p.suffix in {".py", ".sql", ".md", ".json", ".xml"} and p.is_file()
    ]
    exposed = [str(p.relative_to(root)) for p in paths if any(s in p.read_bytes() for s in passwords)]
    assert not exposed, "Stand password found in " + ", ".join(exposed)
    assert not any(s in package_bytes for s in passwords), "Stand password found in wheel"
    code = hashlib.sha256()
    for p in sorted(
        [root / "pyproject.toml"] + [p for p in (root / "src").rglob("*") if p.suffix in {".py", ".sql"}]
    ):
        name, data = str(p.relative_to(root)).encode(), p.read_bytes()
        code.update(len(name).to_bytes(8, "big") + name + len(data).to_bytes(8, "big") + data)
    report = dict(
        recorded_at=datetime.now(UTC).isoformat(),
        version=version,
        maturity="development",
        full_spec_v2_complete=False,
        cloud_used=False,
        local_code_sha256=code.hexdigest(),
        suites=suites,
        additional_regressions=regressions,
        total_test_executions=sum(s["tests"] for s in suites.values())
        + sum(s["tests"] for s in regressions.values()),
        wheel=dict(
            path=str(wheel.relative_to(root)),
            sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
            clean_install=True,
            capabilities=True,
            sql_resources=True,
        ),
        privacy=dict(
            known_stand_password_matches=0, scanned_files=len(paths), private_credential_files_in_wheel=False
        ),
        active_stand_major=18,
        validation_benchmark="docs/validation_benchmark_pg18_2026-10-30.json",
        caveats=[
            "Small local datasets; no long duration performance or chaos qualification",
            "Same major 15–18; Python 3.12 / Linux ARM64 only",
            "Expanded DDL/queue is qualified for the documented profile; DO, compound/global/extension DDL and full SPEC object coverage remain incomplete",
            "Resync supports ordinary logged tables only; offline cleanup refuses unknown additional slots",
            "Integrity baseline requires a fresh migration; no adoption of state without a baseline",
            "Small local datasets; no production qualification",
            "Persistent apply error test accelerates observation time in state, not PostgreSQL errors",
        ],
    )
    dest = root / args.output
    dest.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(
        json.dumps(
            dict(
                report=str(dest),
                total_test_executions=report["total_test_executions"],
                secret_matches=0,
                wheel_smoke="passed",
            )
        )
    )


if __name__ == "__main__":
    main()
