# Contributing guide

All contributions, bug reports, bug fixes, documentation improvements, and enhancements are welcome.

All contributors and maintainers to this project should abide by the [Contributor Code of Conduct](./code-of-conduct.md).

Learn more about the contributors' roles in the [Roles](./contributor-roles.md) page.

This document describes how to contribute to the Great Expectations Airflow Provider, covering:

- Overview of how to contribute
- How to set up the local development environment
- Running tests
- Authoring the documentation

## Overview of how to contribute

To contribute to the Great Expectations Airflow Provider project:

1. Create a [GitHub Issue](https://github.com/great-expectations/airflow-provider-great-expectations/issues) describing a bug, enhancement, or feature request.
2. Fork the repository.
3. In your fork, open a branch off of the `main` branch.
4. Create a Pull Request into the `main` branch of the Provider repo from your forked feature branch.
5. Link your issue to the Pull Request.
6. After you complete development on your feature branch, request a review. A maintainer will merge your PR after all reviewers approve it.


## Set up a local development environment

Setting up a local development environment involves fulfilling requirements, getting a copy of the repository, and setting up a virtual environment.

### Requirements

- [Git](https://git-scm.com/)
- [Python](https://www.python.org/) version 3.11 or later, as your Airflow version allows
- [Great Expectations](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx) version 1.23.2+ (1.24.0+ on Python 3.14)
- [Apache Airflow®](https://airflow.apache.org/) version 2.11.0+ and below 4

See [Compatibility](../compatibility.md) for the tested versions and the support policy.

### Get a copy of the repository

1. Fork the [Provider repository](https://github.com/great-expectations/airflow-provider-great-expectations).

2. Clone your fork.
   ```bash
   git clone https://github.com/my-user/airflow-provider-great-expectations.git
   ```

### Set up a virtual environment

You can use any virtual environment tool. The following example uses the `venv` tool included in the Python standard library.

1. Create the virtual environment.
   ```bash
   $ python -m venv .venv
   ```

2. Activate the virtual environment.
   ```bash
   $ . .venv/bin/activate
   ```

3. Install the package and testing dependencies.
   ```bash
   (.venv) $ pip install -e '.[tests]'
   ```

## Run tests

Test with `pytest`:

1. Install `pytest` as a dependency.
   ```bash
   pip install "airflow-provider-great-expectations[tests]"
   ```
2. Run the following command, which will provide a concise output when all tests pass and minimum necessary details when they don't.
   ```bash
   pytest -p no:warnings
   ```
   The `no:warnings` flag filters out deprecation messages that may be issued by Airflow.

### Unit tests vs integration tests

Tests are organized into two categories:

- **Unit tests** (`tests/unit/`): Run without external dependencies and don't require credentials.
- **Integration tests** (`tests/integration/`): Need no credentials. Some selections need an external backend (Postgres, Spark), described below.

To run only unit tests:
```bash
pytest tests/unit
```

To run integration tests locally:
```bash
pytest -m integration tests/integration
```

### Running postgres tests

Postgres tests use a separate marker and require a running Postgres instance plus the `postgresql` optional dependency:

```bash
pip install -e '.[postgresql,tests]'
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=postgres
export POSTGRES_PORT=5432
pytest -m postgres tests/integration
```

### Running spark tests

Spark tests run in separate CI jobs and use different markers. To run them locally, install the `spark` optional dependency and start Spark via Docker Compose:

```bash
pip install -e '.[spark,tests]'
docker compose -f docker/spark/docker-compose.yml up -d
pytest -m spark_integration tests/integration
pytest -m spark_connect_integration tests/integration
```

### Type checking

The provider supports three groups of Airflow releases that have different type definitions: Airflow 2.11, Airflow 3.0, and Airflow 3.1 and later. `great_expectations_provider/version_compat.py` imports Airflow from a different place for each group and selects the imports with the `AIRFLOW_V_3_0_PLUS` and `AIRFLOW_V_3_1_PLUS` flags. CI runs `mypy` once for each group, and you can do the same locally:

1. Create a virtual environment for the Airflow release you want to check. The [compatibility page](../compatibility.md#supported-versions) lists the release that CI tests for each group.
2. Install the `lint` extra in that environment, together with the Airflow release and its constraints file. Replace `<Airflow version>` with that release and `<python version>` with the major and minor version of your Python, such as `3.11`. See [Install with constraints](../compatibility.md#install-with-constraints). The `lint` extra pins its own versions of `mypy`, `ruff`, `pytest` and `pytest-mock`, which differ from the versions in Airflow's constraints file, so first download the constraints file and remove those four pins. Keep the `apache-airflow` pin. The command writes `constraints.txt`; don't commit it.
   ```bash
   curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-<Airflow version>/constraints-<python version>.txt" | grep -v -E '^(mypy|ruff|pytest|pytest-mock)==' > constraints.txt
   pip install -e '.[lint]' "apache-airflow==<Airflow version>" --constraint constraints.txt
   ```

Then run the command for your Airflow release. The flags tell `mypy` which branches of `version_compat.py` to check.

| Airflow version | Command |
|-----------------|---------|
| 2.11 | `mypy --always-false AIRFLOW_V_3_0_PLUS --always-false AIRFLOW_V_3_1_PLUS .` |
| 3.0 | `mypy --always-true AIRFLOW_V_3_0_PLUS --always-false AIRFLOW_V_3_1_PLUS .` |
| 3.1 and later | `mypy --always-true AIRFLOW_V_3_0_PLUS --always-true AIRFLOW_V_3_1_PLUS .` |

Always pass the flags for the Airflow release you installed. If you run `mypy .` without them, `mypy` checks every branch of `version_compat.py` against a single Airflow release and reports errors from the branches that belong to other releases. Don't add the flags to `pyproject.toml`: `mypy` adds command-line flags to the values in the configuration file, and it exits with an error when a flag name is both always true and always false, so a default in the file would break the runs for the other releases.


## Write docs

We use Markdown to author Great Expectations Airflow Provider documentation. We use hatch to build and release the docs.

1. Update Markdown files in the [`docs/` folder](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/docs).
2. Build and serve the documentation locally to preview your changes.
   ```bash
   hatch run docs:dev
   ```
3. [Open an issue and PR](#overview-of-how-to-contribute) for your changes.
4. Once approved, release the documentation with the current project version and set it to the latest.
   ```
   hatch run docs:gh-release
   ```
