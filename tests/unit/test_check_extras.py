from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / ".github/scripts/check_extras.py"
FIXTURES = Path(__file__).parent / "fixtures/ci"
RECORDED_CONSTRAINTS = FIXTURES / "airflow-2.11.2-constraints-3.11.txt"
# The whole-run tests read this frozen matrix, not the checked-in one, so that a
# routine matrix update does not change the lanes and counts they assert.
FROZEN_MATRIX = FIXTURES / "support-matrix.toml"
# The tests that build a fake repository, and the one that lists the provider's extras,
# read this frozen copy of the package metadata. It declares the same extras as the
# frozen matrix, so adding or removing an extra in the real `pyproject.toml` does not
# change what they expect.
FROZEN_PYPROJECT = FIXTURES / "sample-pyproject.toml"

# Today's provider extras, other than the test and lint extras.
EXTRAS = (
    "athena",
    "azure",
    "bigquery",
    "gcp",
    "mssql",
    "postgresql",
    "s3",
    "snowflake",
    "spark",
    "trino",
)
# The eight lanes that the frozen matrix derives, as (Airflow release, Python).
LANES = (
    ("2.11.2", "3.11"),
    ("3.0.6", "3.11"),
    ("3.1.8", "3.11"),
    ("3.2.2", "3.11"),
    ("3.3.2", "3.11"),
    ("3.3.2", "3.12"),
    ("3.3.2", "3.13"),
    ("3.3.2", "3.14"),
)
EXEMPT_LINES = ("mypy==1.9.0", "pytest-mock==3.15.1", "pytest==8.2.1", "ruff==0.5.5")


def recorded(name: str) -> dict[str, Any]:
    """Load a recorded `uv pip compile` result: its exit status, stdout and stderr."""
    return json.loads((FIXTURES / f"uv-compile-{name}.json").read_text("utf-8"))


@pytest.fixture(autouse=True)
def no_job_summary(monkeypatch):
    """Keep the check's report out of the job summary of the CI job running these tests.

    Tests that check the summary set GITHUB_STEP_SUMMARY to a temporary file themselves.
    """
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


@pytest.fixture
def check_extras():
    spec = importlib.util.spec_from_file_location("check_extras_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def result_of(check_extras):
    """Build the script's result type from a recorded compile."""

    def build(name: str, **overrides: Any):
        data = {**recorded(name), **overrides}
        return check_extras.CommandResult(
            returncode=data["returncode"], stdout=data["stdout"], stderr=data["stderr"]
        )

    return build


# --------------------------------------------------------------------------
# Classifying one compile
# --------------------------------------------------------------------------


def test_a_resolution_with_its_driver_passes(check_extras, result_of):
    reasons = check_extras.classify(
        "postgresql", ("psycopg2-binary",), result_of("2.11.2-py3.11-postgresql")
    )
    assert reasons == ()


def test_a_missing_driver_fails_and_is_named(check_extras, result_of):
    reasons = check_extras.classify(
        "postgresql",
        ("psycopg2-binary", "pyodbc"),
        result_of("2.11.2-py3.11-postgresql"),
    )
    assert len(reasons) == 1
    assert "pyodbc" in reasons[0]
    assert "psycopg2-binary" not in reasons[0]


def test_every_missing_driver_is_named(check_extras, result_of):
    reasons = check_extras.classify(
        "postgresql",
        ("pyodbc", "ibm-db", "psycopg2-binary"),
        result_of("2.11.2-py3.11-postgresql"),
    )
    assert len(reasons) == 1
    assert "pyodbc" in reasons[0] and "ibm-db" in reasons[0]


@pytest.mark.parametrize("listed", ["Psycopg2_Binary", "psycopg2.binary"])
def test_drivers_match_by_normalized_name(check_extras, result_of, listed):
    assert (
        check_extras.classify(
            "postgresql", (listed,), result_of("2.11.2-py3.11-postgresql")
        )
        == ()
    )


def test_a_driver_must_be_a_resolved_package_not_a_substring(check_extras, result_of):
    # `grpcio` is resolved, but `grpc` is not a package in the output.
    reasons = check_extras.classify(
        "spark", ("grpc",), result_of("2.11.2-py3.11-postgresql")
    )
    assert len(reasons) == 1 and "grpc" in reasons[0]


def test_the_unknown_extra_warning_fails_and_names_both_extras(check_extras, result_of):
    reasons = check_extras.classify("mssql", (), result_of("2.11.2-py3.11-mssql"))
    assert len(reasons) == 1
    assert "provider extra `mssql`" in reasons[0]
    assert "GX extra `mssql`" in reasons[0]
    assert "great-expectations==1.24.1" in reasons[0]


def test_the_two_extras_are_told_apart_when_their_names_differ(check_extras, result_of):
    result = result_of(
        "2.11.2-py3.11-mssql",
        stderr=recorded("2.11.2-py3.11-mssql")["stderr"].replace(
            "named `mssql`", "named `sql-server`"
        ),
    )
    reasons = check_extras.classify("mssql", (), result)
    assert "provider extra `mssql`" in reasons[0]
    assert "GX extra `sql-server`" in reasons[0]


def test_a_repeated_warning_is_reported_once(check_extras, result_of):
    warning = recorded("2.11.2-py3.11-mssql")["stderr"]
    result = result_of("2.11.2-py3.11-mssql", stderr=warning + warning)
    assert len(check_extras.classify("mssql", (), result)) == 1


def test_an_unknown_extra_of_another_package_is_named_with_its_package(
    check_extras, result_of
):
    result = result_of(
        "2.11.2-py3.11-postgresql",
        stderr="warning: The package `apache-airflow==2.11.2` does not have an "
        "extra named `nope`\n",
    )
    reasons = check_extras.classify("postgresql", ("psycopg2-binary",), result)
    assert len(reasons) == 1
    assert "provider extra `postgresql`" in reasons[0]
    assert "`nope`" in reasons[0] and "apache-airflow==2.11.2" in reasons[0]
    assert "GX extra" not in reasons[0]


def test_an_unknown_extra_is_reported_alongside_a_failed_resolution(
    check_extras, result_of
):
    result = result_of(
        "3.3.2-py3.11-postgresql",
        stderr=recorded("2.11.2-py3.11-mssql")["stderr"]
        + recorded("3.3.2-py3.11-postgresql")["stderr"],
    )
    reasons = check_extras.classify("mssql", ("pyodbc",), result)
    assert len(reasons) == 2
    assert "GX extra `mssql`" in reasons[0]
    assert reasons[1].startswith("uv could not resolve")


def test_mssql_fails_twice_over_today_without_its_driver(check_extras, result_of):
    # The compile succeeds, so the failures are the warning and the absent driver.
    reasons = check_extras.classify(
        "mssql", ("pyodbc",), result_of("2.11.2-py3.11-mssql")
    )
    assert len(reasons) == 2
    assert "GX extra `mssql`" in reasons[0]
    assert "pyodbc" in reasons[1]


def test_a_resolution_conflict_fails_with_uvs_explanation(check_extras, result_of):
    reasons = check_extras.classify(
        "postgresql", ("psycopg2-binary",), result_of("3.3.2-py3.11-postgresql")
    )
    assert len(reasons) == 1
    assert reasons[0].startswith("uv could not resolve")
    assert "structlog<25.5.0 and structlog==26.1.0" in reasons[0]
    assert "\n" not in reasons[0]


def test_a_failed_resolution_does_not_also_report_drivers(check_extras, result_of):
    reasons = check_extras.classify(
        "postgresql", ("psycopg2-binary",), result_of("3.3.2-py3.11-postgresql")
    )
    assert not any("driver" in reason for reason in reasons)


def test_a_long_resolver_explanation_reports_its_first_and_last_steps(
    check_extras, result_of
):
    reasons = check_extras.classify(
        "spark", ("pyspark",), result_of("3.0.6-py3.11-spark")
    )
    assert len(reasons) == 1
    # The first step is about an old GX release; the last is about the newest.
    assert "pyspark>=2.3.2,<4.0 and pyspark==4.0.0" in reasons[0]
    assert "Last step: " in reasons[0]
    last = reasons[0].split("Last step: ", 1)[1]
    assert "zstandard>=0.25.0 and zstandard==0.24.0" in last
    assert "great-expectations[spark-connect]==1.24.1" in last
    assert "1.7.1" not in reasons[0]
    assert "unsatisfiable" not in reasons[0]
    assert len(reasons[0]) < 700


def test_a_one_step_explanation_is_reported_once(check_extras, result_of):
    reasons = check_extras.classify(
        "postgresql", ("psycopg2-binary",), result_of("3.3.2-py3.11-postgresql")
    )
    assert "Last step" not in reasons[0]
    assert reasons[0].endswith("cannot be used.")
    assert "unsatisfiable" not in reasons[0]


def test_a_failure_without_a_resolver_explanation_still_names_uvs_error(
    check_extras, result_of
):
    result = check_extras.CommandResult(
        returncode=2, stdout="", stderr="error: Failed to fetch: https://pypi.org/x\n"
    )
    reasons = check_extras.classify("s3", ("boto3",), result)
    assert len(reasons) == 1
    assert "Failed to fetch" in reasons[0]


# --------------------------------------------------------------------------
# The extras the provider defines
# --------------------------------------------------------------------------


def test_the_test_and_lint_extras_are_not_checked(check_extras):
    assert check_extras.provider_extras(FROZEN_PYPROJECT) == EXTRAS


def test_the_checked_in_matrix_maps_every_provider_extra(check_extras):
    matrix = check_extras.load_matrix(ROOT / ".github/support-matrix.toml")
    extras = check_extras.provider_extras(ROOT / "pyproject.toml")
    assert check_extras.unmapped_extras(extras, matrix.extras) == ()


def test_an_extra_without_a_matrix_entry_is_unmapped(check_extras):
    assert check_extras.unmapped_extras(
        ("mssql", "sql-server", "Fresh_Backend"), ("mssql",)
    ) == ("Fresh_Backend", "sql-server")


def test_extras_are_matched_to_the_matrix_by_normalized_name(check_extras):
    assert (
        check_extras.unmapped_extras(("sql_server", "SQL.Server"), ("sql-server",))
        == ()
    )


# --------------------------------------------------------------------------
# Compile command composition
# --------------------------------------------------------------------------


def test_the_compile_command_targets_the_lane(check_extras):
    command = check_extras.compile_command(
        "/bin/uv", Path("in.txt"), Path("constraints.txt"), "3.12"
    )
    assert command[:4] == ["/bin/uv", "pip", "compile", "in.txt"]
    assert command[command.index("--constraint") + 1] == "constraints.txt"
    assert command[command.index("--python-version") + 1] == "3.12"
    assert command[command.index("--python-platform") + 1] == "x86_64-manylinux_2_28"


def test_the_requirements_pin_airflow_and_the_provider_with_its_extra(
    check_extras, tmp_path
):
    text = check_extras.requirements_text(
        "airflow-provider-great-expectations", "mssql", "2.11.2", tmp_path
    )
    assert text.splitlines() == [
        "apache-airflow==2.11.2",
        f"airflow-provider-great-expectations[mssql] @ {tmp_path.as_uri()}",
    ]


def test_a_command_that_times_out_is_a_failure_not_a_crash(check_extras, monkeypatch):
    def hang(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(check_extras.subprocess, "run", hang)
    result = check_extras.run_command(["uv", "pip", "compile"], timeout=5)
    assert result.returncode != 0
    assert "timed out after 5 seconds" in result.stderr
    reasons = check_extras.classify("s3", ("boto3",), result)
    assert len(reasons) == 1 and "timed out" in reasons[0]


def test_a_command_that_cannot_start_is_a_failure_not_a_crash(
    check_extras, monkeypatch
):
    def missing(command, **kwargs):
        raise FileNotFoundError("no such file")

    monkeypatch.setattr(check_extras.subprocess, "run", missing)
    result = check_extras.run_command(["uv", "--version"])
    assert result.returncode != 0 and "cannot run uv" in result.stderr


# --------------------------------------------------------------------------
# The whole run, with the network and uv replaced
# --------------------------------------------------------------------------


class FakeUv:
    """Stand in for uv: answer from recorded results, and remember what was asked."""

    def __init__(self, check_extras, matrix_extras, recordings=None):
        self.module = check_extras
        self.matrix_extras = matrix_extras
        self.recordings = recordings or {}
        self.compiles: list[SimpleNamespace] = []

    def __call__(self, command, *, timeout=None, cwd=None):
        if list(command[1:]) == ["--version"]:
            return self.module.CommandResult(0, "uv 0.11.24 (fake)\n", "")
        requirements = Path(command[3]).read_text("utf-8")
        found_release = re.search(r"apache-airflow==(\S+)", requirements)
        found_extra = re.search(r"\[([^\]]+)\] @ ", requirements)
        assert found_release is not None and found_extra is not None
        release, extra = found_release.group(1), found_extra.group(1)
        python = command[command.index("--python-version") + 1]
        self.compiles.append(
            SimpleNamespace(
                command=list(command),
                release=release,
                python=python,
                extra=extra,
                constraints=Path(command[command.index("--constraint") + 1]).read_text(
                    "utf-8"
                ),
            )
        )
        recorded_result = self.recordings.get((release, python, extra))
        if recorded_result is not None:
            return recorded_result
        drivers = self.matrix_extras[extra]
        stdout = "".join(f"{driver}==1.0\n" for driver in drivers)
        return self.module.CommandResult(0, stdout, "Resolved 1 package in 1ms\n")


@pytest.fixture
def make_repo(tmp_path):
    def make(extra_extras: str = "") -> Path:
        root = tmp_path / "repo"
        (root / ".github").mkdir(parents=True, exist_ok=True)
        (root / ".github/support-matrix.toml").write_text(
            FROZEN_MATRIX.read_text("utf-8"), encoding="utf-8"
        )
        pyproject = FROZEN_PYPROJECT.read_text("utf-8")
        marker = "\ntests = ["
        assert marker in pyproject
        (root / "pyproject.toml").write_text(
            pyproject.replace(marker, f"\n{extra_extras}{marker}", 1), encoding="utf-8"
        )
        return root

    return make


@pytest.fixture
def run(check_extras, monkeypatch, tmp_path, capsys):
    """Run the check against a fake repository and return (status, stdout, summary, uv)."""

    def go(root: Path, *, recordings=None, constraints=None, uv_on_path=True):
        matrix = check_extras.load_matrix(root / ".github/support-matrix.toml")
        fake = FakeUv(check_extras, matrix.extras, recordings)
        downloads: list[tuple[str, str, str]] = []

        def download(url: str, airflow: str, python: str) -> str:
            downloads.append((url, airflow, python))
            if constraints is not None:
                return constraints(url, airflow, python)
            return RECORDED_CONSTRAINTS.read_text("utf-8")

        monkeypatch.setattr(check_extras, "run_command", fake)
        monkeypatch.setattr(check_extras, "download_constraints", download)
        monkeypatch.setattr(
            check_extras.shutil, "which", lambda name: "/bin/uv" if uv_on_path else None
        )
        summary = tmp_path / "summary.md"
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
        status = check_extras.main([], repo_root=root)
        out = capsys.readouterr()
        text = summary.read_text("utf-8") if summary.exists() else ""
        return SimpleNamespace(
            status=status,
            stdout=out.out,
            stderr=out.err,
            summary=text,
            uv=fake,
            downloads=downloads,
        )

    return go


def test_an_extra_with_no_matrix_entry_fails_first_by_name(make_repo, run):
    root = make_repo('newbackend = ["great-expectations[newbackend]>=1.7.0, <2"]\n')
    outcome = run(root)
    assert outcome.status == 1
    assert "newbackend" in outcome.stdout + outcome.stderr
    assert "newbackend" in outcome.summary
    # Nothing was downloaded or compiled: the missing entry fails the run first.
    assert outcome.uv.compiles == []
    assert outcome.downloads == []


def test_every_unmapped_extra_is_named(make_repo, run):
    root = make_repo(
        'newbackend = ["great-expectations[newbackend]>=1.7.0, <2"]\n'
        'other-one = ["great-expectations[other]>=1.7.0, <2"]\n'
    )
    outcome = run(root)
    assert "newbackend" in outcome.stdout + outcome.stderr
    assert "other-one" in outcome.stdout + outcome.stderr


def test_every_lane_is_compiled_with_every_extra(make_repo, run):
    outcome = run(make_repo())
    assert outcome.status == 0
    seen = {(c.release, c.python, c.extra) for c in outcome.uv.compiles}
    assert seen == {(r, p, e) for r, p in LANES for e in EXTRAS}
    assert len(outcome.uv.compiles) == len(LANES) * len(EXTRAS)


def test_each_lanes_constraints_are_downloaded_once(make_repo, run):
    outcome = run(make_repo())
    urls = [url for url, _, _ in outcome.downloads]
    assert len(urls) == len(LANES) == len(set(urls))
    assert (
        "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/"
        "constraints-3.11.txt"
    ) in urls


def test_constraints_lose_only_the_tool_pins_the_installer_exempts(make_repo, run):
    full = RECORDED_CONSTRAINTS.read_text("utf-8")
    for line in EXEMPT_LINES:
        assert line in full
    outcome = run(make_repo())
    constraints = outcome.uv.compiles[0].constraints
    for line in EXEMPT_LINES:
        assert line not in constraints.splitlines()
    kept = [line for line in full.splitlines() if line not in EXEMPT_LINES]
    assert constraints.splitlines() == kept
    assert "pytest" in outcome.stdout and "mypy" in outcome.stdout


def test_a_passing_run_prints_the_table_to_stdout_and_the_job_summary(make_repo, run):
    outcome = run(make_repo())
    assert outcome.status == 0
    for text in (outcome.stdout, outcome.summary):
        header = next(line for line in text.splitlines() if line.startswith("| Lane"))
        assert [cell.strip() for cell in header.strip("|").split("|")] == [
            "Lane",
            *EXTRAS,
        ]
        rows = [line for line in text.splitlines() if line.startswith("| Airflow ")]
        assert len(rows) == len(LANES)
        assert all(row.count("pass") == len(EXTRAS) for row in rows)
        assert "FAIL" not in text
    assert "Airflow 3.3.2, Python 3.14" in outcome.stdout


def test_the_report_records_the_uv_and_python_versions(make_repo, run):
    outcome = run(make_repo())
    assert "uv 0.11.24 (the matrix pins 0.11.24)" in outcome.stdout
    assert "x86_64-manylinux_2_28" in outcome.stdout
    assert f"Python {sys.version_info.major}.{sys.version_info.minor}" in outcome.stdout


def test_a_failing_extra_is_marked_and_explained_on_its_lane(
    make_repo, run, check_extras
):
    mssql = check_extras.CommandResult(
        returncode=0,
        stdout=recorded("2.11.2-py3.11-mssql")["stdout"],
        stderr=recorded("2.11.2-py3.11-mssql")["stderr"],
    )
    outcome = run(make_repo(), recordings={("2.11.2", "3.11", "mssql"): mssql})
    assert outcome.status == 1
    for text in (outcome.stdout, outcome.summary):
        floor_row = next(
            line for line in text.splitlines() if line.startswith("| Airflow 2.11.2")
        )
        cells = [cell.strip() for cell in floor_row.strip("|").split("|")][1:]
        assert dict(zip(EXTRAS, cells, strict=True)) == {
            extra: ("FAIL" if extra == "mssql" else "pass") for extra in EXTRAS
        }
        other_rows = [
            line
            for line in text.splitlines()
            if line.startswith("| Airflow ") and "2.11.2" not in line
        ]
        assert all("FAIL" not in row for row in other_rows)
        failure = next(
            line for line in text.splitlines() if line.startswith("- Airflow 2.11.2")
        )
        assert "Python 3.11" in failure and "`mssql`" in failure
        assert "GX extra `mssql`" in failure and "pyodbc" in failure
        assert text.count("\n- Airflow ") == 1


def test_a_resolution_conflict_fails_every_extra_of_that_lane(
    make_repo, run, check_extras
):
    conflict = check_extras.CommandResult(
        returncode=1, stdout="", stderr=recorded("3.3.2-py3.11-postgresql")["stderr"]
    )
    recordings = {("3.3.2", "3.11", extra): conflict for extra in EXTRAS}
    outcome = run(make_repo(), recordings=recordings)
    assert outcome.status == 1
    failures = [
        line for line in outcome.stdout.splitlines() if line.startswith("- Airflow ")
    ]
    # Ten identical failures on one lane make one line.
    assert len(failures) == 1
    assert "Airflow 3.3.2, Python 3.11, all 10 extras" in failures[0]
    assert "structlog==26.1.0" in failures[0]
    table = [
        line
        for line in outcome.stdout.splitlines()
        if line.startswith("| Airflow 3.3.2, Python 3.11")
    ]
    assert table[0].count("FAIL") == len(EXTRAS)
    assert "10 of 80 resolutions fail." in outcome.stdout


def test_a_lane_without_a_constraints_file_fails_all_its_extras_with_the_404(
    make_repo, run, check_extras
):
    def constraints(url: str, airflow: str, python: str) -> str:
        if (airflow, python) == ("3.0.6", "3.11"):
            raise check_extras.installer().ConstraintsNotFound(
                f"no constraints file for Airflow {airflow} on Python {python} "
                f"(HTTP 404): {url}"
            )
        return RECORDED_CONSTRAINTS.read_text("utf-8")

    outcome = run(make_repo(), constraints=constraints)
    assert outcome.status == 1
    failures = [
        line for line in outcome.stdout.splitlines() if line.startswith("- Airflow ")
    ]
    assert len(failures) == 1
    assert "HTTP 404" in failures[0] and "Airflow 3.0.6, Python 3.11" in failures[0]
    # The other lanes still ran, and the broken lane was never compiled.
    assert {c.release for c in outcome.uv.compiles} == {
        release for release, _ in LANES if release != "3.0.6"
    }


def test_extras_that_fail_alike_on_a_lane_share_one_line(make_repo, run, check_extras):
    conflict = check_extras.CommandResult(
        returncode=1, stdout="", stderr=recorded("3.3.2-py3.11-postgresql")["stderr"]
    )
    recordings = {("3.0.6", "3.11", extra): conflict for extra in ("s3", "trino")}
    outcome = run(make_repo(), recordings=recordings)
    failures = [
        line for line in outcome.stdout.splitlines() if line.startswith("- Airflow ")
    ]
    assert len(failures) == 1
    assert failures[0].startswith("- Airflow 3.0.6, Python 3.11, `s3`, `trino`: ")
    assert "2 of 80 resolutions fail." in outcome.stdout


def test_each_lane_compiles_for_its_own_python(make_repo, run):
    outcome = run(make_repo())
    pythons = {(c.release, c.python) for c in outcome.uv.compiles}
    assert pythons == set(LANES)
    for compile_ in outcome.uv.compiles:
        command = compile_.command
        assert command[command.index("--python-version") + 1] == compile_.python


def test_a_missing_uv_is_invalid_input(make_repo, run):
    outcome = run(make_repo(), uv_on_path=False)
    assert outcome.status == 2
    assert "uv" in outcome.stderr
    assert outcome.uv.compiles == []


def test_an_invalid_matrix_is_invalid_input(make_repo, check_extras, capsys):
    root = make_repo()
    matrix = root / ".github/support-matrix.toml"
    matrix.write_text(
        matrix.read_text("utf-8").replace("schema_version = 1", "schema_version = 7"),
        encoding="utf-8",
    )
    assert check_extras.main([], repo_root=root) == 2
    assert "schema_version" in capsys.readouterr().err
