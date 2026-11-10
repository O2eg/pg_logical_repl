"""Build an isolated wheel and run reproducible tests on owned pg-stand profiles."""

import argparse
import hashlib
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import venv
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("quick", "matrix", "soak", "replay"), default="quick")
    parser.add_argument("--pg-stand", required=True, help="Installed pg-stand CLI")
    parser.add_argument("--major", type=int, choices=(15, 16, 17, 18), action="append")
    parser.add_argument("--current-major", type=int, choices=(15, 16, 17, 18), default=18)
    parser.add_argument("--existing-containers", action="store_true")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--examples", type=int, default=6)
    parser.add_argument("--steps", type=int, default=20)
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--sequence", type=Path, help="Saved sequence.json, required for replay")
    args = parser.parse_args()
    if args.profile == "replay" and (not args.sequence or not args.sequence.is_file()):
        parser.error("--profile replay requires an existing --sequence")
    if min(args.examples, args.steps, args.rows, args.writers) < 1 or args.seconds < 5 or args.writers > 8:
        parser.error("Positive counts, 1..8 writers and at least 5 seconds required")
    os.chdir(ROOT)
    os.umask(0o077)
    # These tests change cluster-wide roles. Serialize all runs on the same stand.
    import fcntl

    parent = ROOT / ".runtime/quality-runs"
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (parent / "runner.lock").open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another quality runner owns this stand; parallel runs are unsupported")
        return run(args, parent)


def run(args, parent):
    started = time.monotonic()
    directory = Path(tempfile.mkdtemp(prefix=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-"), dir=parent))
    seed = args.seed if args.seed is not None else secrets.randbits(64)
    majors = args.major or ([15, 16, 17, 18] if args.profile == "matrix" else [18])
    report = dict(
        profile=args.profile,
        started_at=datetime.now(UTC).isoformat(),
        seed=seed,
        majors=majors,
        python=sys.version,
        platform=platform.platform(),
        generated_examples=args.examples,
        generated_steps=args.steps,
        requested_soak_seconds=args.seconds,
        passed=False,
    )
    print(f"QUALITY_ARTIFACTS {directory}\nHYPOTHESIS_SEED {seed}", flush=True)
    files = [ROOT / "pyproject.toml", ROOT / "README.md"]
    for name in ("src", "tests", "infra"):
        files += [
            p
            for p in (ROOT / name).rglob("*")
            if p.is_file() and "__pycache__" not in p.parts and ".egg-info" not in str(p)
        ]
    manifest = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}
    (directory / "source-manifest.json").write_text(json.dumps(manifest, indent=2))
    report["source_manifest_sha256"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()
    ).hexdigest()
    (directory / "report.json").write_text(json.dumps(report, indent=2))
    try:
        stage = directory / "build-source"
        stage.mkdir()
        for name in ("pyproject.toml", "README.md"):
            shutil.copy2(ROOT / name, stage / name)
        shutil.copytree(
            ROOT / "src/pg_logical_repl",
            stage / "src/pg_logical_repl",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        with (directory / "build.log").open("w") as output:
            subprocess.run(
                [sys.executable, "-m", "build", "--wheel", "--outdir", str(directory / "wheels"), str(stage)],
                check=True,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
        (wheel,) = (directory / "wheels").glob("*.whl")
        report["wheel"] = wheel.name
        report["wheel_sha256"] = hashlib.sha256(wheel.read_bytes()).hexdigest()
        venv.EnvBuilder(with_pip=True).create(directory / "venv")
        python = directory / "venv/bin/python"
        with (directory / "install.log").open("w") as output:
            subprocess.run(
                [str(python), "-m", "pip", "install", str(wheel) + "[test]"],
                check=True,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=300,
            )
        freeze = subprocess.check_output([str(python), "-m", "pip", "freeze"], text=True, timeout=30)
        (directory / "dependencies.txt").write_text(freeze)
        report["dependencies_file"] = "dependencies.txt"
        with (directory / "package-checks.log").open("w") as output:
            subprocess.run(
                [str(python), "-m", "pip", "check"],
                check=True,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=30,
            )
            subprocess.run(
                [str(directory / "venv/bin/pg-logical-repl"), "--version"],
                check=True,
                stdout=output,
                stderr=subprocess.STDOUT,
                cwd="/tmp",
                timeout=30,
            )
        env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTEST_ADDOPTS", "LC_CONFIG"}}
        probe = subprocess.check_output(
            [
                str(python),
                "-c",
                "import json,importlib.metadata,pg_logical_repl;print(json.dumps(dict(version=importlib.metadata.version('pg-logical-repl'),module=pg_logical_repl.__file__)))",
            ],
            cwd="/tmp",
            env=env,
            text=True,
        )
        report["installed"] = json.loads(probe)
        assert Path(report["installed"]["module"]).is_relative_to(directory / "venv"), (
            "Expected wheel installation"
        )
        env.update(
            LC_TEST_PYTHON=str(python),
            LC_QUALITY_ARTIFACTS=str(directory),
            LC_GENERATED_EXAMPLES=str(args.examples),
            LC_GENERATED_STEPS=str(args.steps),
            LC_SOAK="1" if args.profile == "soak" else "0",
            LC_SOAK_SECONDS=str(args.seconds),
            LC_SOAK_ROWS=str(args.rows),
            LC_SOAK_WRITERS=str(args.writers),
        )
        command = [
            str(python),
            "infra/test_matrix.py",
            "--pg-stand",
            args.pg_stand,
            "--current-major",
            str(args.current_major),
            "--artifacts-dir",
            str(directory),
            "--timeout",
            str(max(1200, args.seconds + 600)),
            "--pytest-arg=--hypothesis-seed=" + str(seed),
        ]
        if args.existing_containers:
            command.append("--existing-containers")
        for major in majors:
            command += ["--major", str(major)]
        if args.profile == "quick":
            command += ["--pytest-marker", "quality and not soak", "--pytest-path", "tests"]
        elif args.profile == "soak":
            command += ["--pytest-marker", "soak", "--pytest-path", "tests/integration/test_quality_cli.py"]
        elif args.profile == "replay":
            copied = directory / "replay-input.json"
            shutil.copy2(args.sequence, copied)
            env["LC_REPLAY_SEQUENCE"] = str(copied)
            command += [
                "--pytest-marker",
                "replay",
                "--pytest-path",
                "tests/integration/test_quality_generated.py",
            ]
        report["command"] = command
        (directory / "report.json").write_text(json.dumps(report, indent=2))
        # Keep a full log without buffering away the periodic workload progress.
        with (directory / "tests.log").open("w") as output:
            process = subprocess.Popen(
                command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            try:
                for line in process.stdout:
                    output.write(line)
                    output.flush()
                    print(line, end="", flush=True)
                report["returncode"] = process.wait()
            except BaseException:
                process.terminate()
                process.wait(timeout=120)
                raise
        report["junit"] = {}
        for path in directory.glob("test_pg*.xml"):
            suite = ET.parse(path).getroot().find("testsuite")
            report["junit"][path.name] = {
                key: int(suite.get(key, "0")) for key in ("tests", "failures", "errors", "skipped")
            }
        report["passed"] = (
            report["returncode"] == 0
            and len(report["junit"]) == len(set(majors))
            and all(
                s["tests"] > 0 and not (s["failures"] or s["errors"] or s["skipped"])
                for s in report["junit"].values()
            )
        )
        return 0 if report["passed"] else (report["returncode"] or 1)
    finally:
        report["elapsed_seconds"] = time.monotonic() - started
        report["finished_at"] = datetime.now(UTC).isoformat()
        (directory / "report.json").write_text(json.dumps(report, indent=2))
        print(f"QUALITY_REPORT {directory / 'report.json'}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
