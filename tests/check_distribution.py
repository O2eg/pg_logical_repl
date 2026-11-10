"""Verify public documentation, SQL resources and exclusions in wheel/sdist files."""

import argparse
import posixpath
import re
import tarfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
GUIDES = (
    "README.md",
    "IMPLEMENTATION_STATUS.md",
    "docs/DDL_IMPLEMENTATION.md",
    "docs/TESTING.md",
    "docs/RELEASING.md",
)
INTERNAL_NAMES = {"todo.md", "SPEC.md", "SPEC_REVIEW.md", "OPEN_SOURCE_REVIEW.md"}
RUNTIME_DIRECTORIES = {
    ".git",
    ".venv",
    ".runtime",
    ".pg_stand",
    ".pytest_cache",
    ".hypothesis",
    "__pycache__",
    "credentials",
    "secrets",
}


def read_archive(path):
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}
    with tarfile.open(path, "r:gz") as archive:
        return {
            member.name.partition("/")[2]: archive.extractfile(member).read()
            for member in archive.getmembers()
            if member.isfile()
        }


def check(path):
    files = read_archive(path)
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    wheel = path.suffix == ".whl"
    docs = f"pg_logical_repl-{version}.data/data/share/doc/pg-logical-repl/" if wheel else ""
    expected = {docs + name: ROOT / name for name in (*GUIDES, "examples/local.yaml")}
    for source in (ROOT / "src/pg_logical_repl/sql").glob("*.sql"):
        target = source.relative_to(ROOT / "src" if wheel else ROOT).as_posix()
        expected[target] = source
    errors = []
    for target, source in expected.items():
        if target not in files:
            errors.append(f"missing file: {target}")
        elif files[target] != source.read_bytes():
            errors.append(f"stale or changed file: {target}")

    metadata_path = f"pg_logical_repl-{version}.dist-info/METADATA" if wheel else "PKG-INFO"
    metadata = BytesParser().parsebytes(files.get(metadata_path, b""))
    if metadata.get("Name") != "pg-logical-repl" or metadata.get("Version") != version:
        errors.append("distribution metadata does not match package name/version")

    public_markdown = {docs + name for name in GUIDES}
    for name, content in files.items():
        parts = PurePosixPath(name).parts
        basename = parts[-1]
        if (
            basename in INTERNAL_NAMES
            or RUNTIME_DIRECTORIES.intersection(parts)
            or basename == ".env"
            or basename.endswith((".log", ".pid", ".pgpass", ".key", ".pem", ".dump", ".pyc"))
        ):
            errors.append(f"internal or runtime file: {name}")
        if not name.endswith(".md"):
            continue
        if name not in public_markdown:
            errors.append(f"Markdown document outside the public allowlist: {name}")
        for match in re.finditer(r"\]\(([^)]+)\)", content.decode("utf-8")):
            link = urlsplit(match[1])
            if link.scheme or link.netloc or not link.path:
                continue
            target = posixpath.normpath(posixpath.join(posixpath.dirname(name), unquote(link.path)))
            if target not in files:
                errors.append(f"broken local documentation link: {name} -> {match[1]}")

    for error in errors:
        print(f"{path.name}: {error}")
    if not errors:
        print(f"{path.name}: {len(GUIDES)} current public guides, example and SQL resources verified")
    return not errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", nargs="+", type=Path)
    args = parser.parse_args()
    results = [check(path) for path in args.archives]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
