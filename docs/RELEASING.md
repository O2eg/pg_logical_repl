# Packaging and release

The distribution and executable are `pg-logical-repl`. The Python package is
`pg_logical_repl`; Python 3.11+ is required. The current release is `0.2.0`.
Supported scenarios and remaining limitations are documented in
[IMPLEMENTATION_STATUS.md](../IMPLEMENTATION_STATUS.md).

## Public documentation

The wheel installs the README, implementation status, DDL/testing/release guides
and the example configuration under `share/doc/pg-logical-repl`, preserving
their relative paths. Locate this directory in the installation environment:

```sh
python -c 'import sysconfig; print(sysconfig.get_path("data") + "/share/doc/pg-logical-repl")'
```

The source distribution contains the same guides plus the test and infrastructure
sources needed to follow the testing guide. The wheel contains runtime code and
SQL resources; database test infrastructure is not installed as application code.

`pyproject.toml` and `MANIFEST.in` explicitly select public documents. Do not add
blanket Markdown globs that include local specifications, reviews or task lists.
`todo.md`, `docs/SPEC.md`, `docs/SPEC_REVIEW.md` and `docs/OPEN_SOURCE_REVIEW.md`
remain excluded. Runtime evidence, credentials, database storage and caches must
also remain outside both archives. Update `tests/check_distribution.py` when
adding a public guide so missing or stale documents fail the release check.

## Local checks

Run from the checkout in a Python 3.11+ virtual environment:

```sh
python -m pip install -e '.[test]' 'twine>=6,<7'
python -m ruff check .
python -m ruff format --check .
python -m pytest -q --ignore=tests/integration
python -m build
python -m twine check dist/*
python tests/check_distribution.py dist/*.whl dist/*.tar.gz
```

Use a clean build directory for each release. `python -m build` produces an sdist
and builds the wheel from that sdist, exercising source completeness. The archive
check compares public guides and SQL resources with the checkout, validates local
documentation links, and rejects internal files and runtime artifacts.

These checks do not exercise database migration, DDL replay or recovery against
PostgreSQL. Run the applicable tests from [TESTING.md](TESTING.md) on owned
laboratory endpoints before claiming those behaviors for a release. Keep their
results as release evidence; do not commit credentials or generated run directories.

## Trusted Publisher

The PyPI publisher registration must match:

| Field | Value |
| --- | --- |
| Project name | `pg-logical-repl` |
| Repository owner | `O2eg` |
| Repository name | `pg_logical_repl` |
| Workflow filename | `publish.yml` |
| Environment | `pypi` |

The workflow lives at `.github/workflows/publish.yml`. Only its publishing job
receives `id-token: write` and uses the `pypi` environment. Authentication uses
GitHub OIDC; no PyPI password or API token is required. A pending publisher creates
the PyPI project on the first successful upload. See the official
[Trusted Publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
and [pending publisher documentation](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).

## Release sequence

1. Commit the reviewed implementation, tests, public documentation and packaging.
   Keep `project.version` in `pyproject.toml` and `__version__` in
   `src/pg_logical_repl/__init__.py` equal. Use a new version for changed artifacts.
2. Push the release commit and let its workflow pass. Branch pushes, pull requests
   and manual dispatch run tests/build checks without uploading to PyPI.
3. After release approval, push the tag matching the version, for example
   `v0.2.0`. A tag push runs the checks again and publishes the verified artifacts.
   A mismatched tag fails before publication; do not move a published release tag.
4. Verify installation of the 0.2 release series from PyPI in a new environment:

   ```sh
   python -m pip install 'pg-logical-repl>=0.2.0,<0.3'
   python -m pip check
   pg-logical-repl --version
   pg-logical-repl --machine --component-capabilities
   ```

5. Publish a pg-play release depending on this contract only after the component
   is available from PyPI and its installation checks pass.
