from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import urllib.error
from email.message import Message
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / ".github/scripts/lane_install.py"
MATRIX = ROOT / ".github/support-matrix.toml"
RECORDED_CONSTRAINTS = (
    Path(__file__).parent / "fixtures/ci/airflow-2.11.2-constraints-3.11.txt"
)
RECORDED_PYPI_RC2 = Path(__file__).parent / "fixtures/ci/airflow-3.4.0rc2.json"
# What the recorded 3.4.0rc2 metadata pins exactly, as pre-releases.
RC2_PINS = ("apache-airflow-core==3.4.0rc2", "apache-airflow-task-sdk==1.4.0rc2")

# The tests never reach the network, so they use whichever Python runs them.
PY = f"{sys.version_info.major}.{sys.version_info.minor}"
MATRIX_TEMPLATE = (
    "https://raw.githubusercontent.com/apache/airflow/"
    "constraints-{airflow}/constraints-{python}.txt"
)
TEST_TEMPLATE = "https://constraints.test/{airflow}/py{python}.txt"

PYPROJECT = """\
[project]
name = "demo"
dependencies = [
    "great-expectations>=1.7.0, <2",
    "apache-airflow AIRFLOW_SPEC",
]

[project.optional-dependencies]
tests = ["pytest==8.3.4", "pytest-mock==3.14.0"]
lint = [
    "mypy==1.14.1",
    "ruff==0.8.3",
    "pytest==8.3.4",
    "pytest-mock==3.14.0",
    "great-expectations[spark, spark-connect]>=1.7.0, <2",
]
spark = ["pyarrow>=4.0.0"]
"""


@pytest.fixture
def lane_install():
    spec = importlib.util.spec_from_file_location("lane_install_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def make_repo(tmp_path):
    """Build a minimal repository root: pyproject, matrix and script requirements."""

    def make(airflow_spec: str = ">=2.11.0, <4") -> Path:
        root = tmp_path / "repo"
        (root / ".github/scripts").mkdir(parents=True, exist_ok=True)
        matrix = MATRIX.read_text(encoding="utf-8")
        assert MATRIX_TEMPLATE in matrix
        (root / ".github/support-matrix.toml").write_text(
            matrix.replace(MATRIX_TEMPLATE, TEST_TEMPLATE), encoding="utf-8"
        )
        (root / ".github/scripts/requirements.txt").write_text("packaging==26.3\n")
        (root / "pyproject.toml").write_text(
            PYPROJECT.replace("AIRFLOW_SPEC", airflow_spec), encoding="utf-8"
        )
        return root

    return make


def url_for(airflow: str, python: str = PY) -> str:
    return TEST_TEMPLATE.format(airflow=airflow, python=python)


def pypi_url(airflow: str) -> str:
    return f"https://pypi.org/pypi/apache-airflow/{airflow}/json"


def http_error(url: str, code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url, code, "error", Message(), None)


# --------------------------------------------------------------------------
# Exemption rule
# --------------------------------------------------------------------------


def test_exempt_set_comes_from_the_pinned_test_and_lint_requirements(lane_install):
    assert lane_install.exempt_names(ROOT / "pyproject.toml") == frozenset(
        {"pytest", "pytest-mock", "mypy", "ruff"}
    )


def test_exempt_set_follows_the_extras_it_is_derived_from(lane_install, tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """\
[project]
name = "demo"

[project.optional-dependencies]
tests = ["Pytest_Mock==3.14.0; python_version >= '3.9'", "black>=24"]
lint = [
    "mypy == 1.14.1",
    "RUFF==0.8.3",
    "great-expectations[spark]>=1.7.0, <2",
    "wildcard==1.*",
]
docs = ["mkdocs==1.6.0"]
""",
        encoding="utf-8",
    )

    assert lane_install.exempt_names(pyproject) == frozenset(
        {"pytest-mock", "mypy", "ruff"}
    )


def test_pinned_requirements_lists_only_the_equality_pins_of_the_given_extras(
    lane_install, make_repo
):
    pyproject = make_repo() / "pyproject.toml"

    assert lane_install.pinned_requirements(pyproject, ("lint",)) == (
        "mypy==1.14.1",
        "ruff==0.8.3",
        "pytest==8.3.4",
        "pytest-mock==3.14.0",
    )
    assert lane_install.pinned_requirements(pyproject, ("tests",)) == (
        "pytest==8.3.4",
        "pytest-mock==3.14.0",
    )


def test_an_extra_that_is_not_defined_is_an_error_rather_than_an_empty_set(
    lane_install, tmp_path
):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "demo"\n\n'
        '[project.optional-dependencies]\ntests = ["pytest==8.3.4"]\n',
        encoding="utf-8",
    )

    with pytest.raises(lane_install.LaneInstallError) as excinfo:
        lane_install.exempt_names(pyproject)

    assert excinfo.value.exit_code == 2
    assert "'lint'" in str(excinfo.value)


# --------------------------------------------------------------------------
# Constraints filtering
# --------------------------------------------------------------------------


def test_filtering_recorded_constraints_removes_exactly_the_four_tool_pins(
    lane_install,
):
    text = RECORDED_CONSTRAINTS.read_text(encoding="utf-8")
    exempt = lane_install.exempt_names(ROOT / "pyproject.toml")

    filtered, removed = lane_install.filter_constraints(text, exempt)

    assert removed == (
        "mypy==1.9.0",
        "pytest-mock==3.15.1",
        "pytest==8.2.1",
        "ruff==0.5.5",
    )
    assert filtered.splitlines() == [
        line for line in text.splitlines() if line not in removed
    ]
    assert filtered.endswith("\n")
    kept = filtered.splitlines()
    for look_alike in (
        "mypy-boto3-s3==1.42.37",
        "mypy_extensions==1.1.0",
        "pytest-asyncio==1.3.0",
        "pytest-xdist==3.8.0",
    ):
        assert look_alike in kept


def test_filtering_matches_normalised_names_extras_and_inline_comments(lane_install):
    text = "\n".join(
        [
            "Pytest_Mock==3.15.1",
            "PYTEST[testing]==8.2.1  # pinned by the constraints",
            'Ruff==0.5.5 ; python_version >= "3.9"',
            "mypy==1.9.0",
            "mypy-extensions==1.1.0",
            "# ruff==0.0.1 stays because it is a comment",
            "",
            "https://example.test/not-a-requirement.whl",
            "",
        ]
    )

    filtered, removed = lane_install.filter_constraints(
        text, frozenset({"pytest", "pytest-mock", "mypy", "ruff"})
    )

    assert len(removed) == 4
    assert filtered.splitlines() == [
        "mypy-extensions==1.1.0",
        "# ruff==0.0.1 stays because it is a comment",
        "",
        "https://example.test/not-a-requirement.whl",
    ]


def test_filtering_with_nothing_exempt_changes_nothing(lane_install):
    text = RECORDED_CONSTRAINTS.read_text(encoding="utf-8")

    assert lane_install.filter_constraints(text, frozenset()) == (text, ())


# --------------------------------------------------------------------------
# Floor parsing
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("requirement", "floor"),
    [
        ("apache-airflow>=2.11.0,<4", "2.11.0"),
        ("apache-airflow>=2.1, <4", "2.1.0"),
        ("apache-airflow >= 3, <4", "3.0.0"),
        ("apache-airflow[celery]>=2.11.2,<4", "2.11.2"),
        ('apache-airflow>=2.1,<4; python_version >= "3.8"', "2.1.0"),
        ("apache-airflow>=2.1,>=2.11,<4", "2.11.0"),
    ],
)
def test_floor_is_the_lower_bound_normalised_to_three_parts(
    lane_install, requirement, floor
):
    assert lane_install.parse_floor(requirement) == floor


@pytest.mark.parametrize(
    "requirement",
    [
        "apache-airflow<4",
        "apache-airflow>2.1,<4",
        "apache-airflow",
        "apache-airflow>=2.1.0rc1,<4",
    ],
)
def test_floor_without_a_usable_lower_bound_exits_2_and_names_the_requirement(
    lane_install, requirement
):
    with pytest.raises(lane_install.LaneInstallError) as excinfo:
        lane_install.parse_floor(requirement)

    assert excinfo.value.exit_code == 2
    assert requirement in str(excinfo.value)


def test_floor_is_read_from_the_apache_airflow_dependency(lane_install, make_repo):
    pyproject = make_repo(">=2.1, <4") / "pyproject.toml"

    assert lane_install.airflow_floor(pyproject) == "2.1.0"


def test_floor_requires_an_apache_airflow_dependency(lane_install, tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "demo"\ndependencies = ["great-expectations>=1.7.0"]\n',
        encoding="utf-8",
    )

    with pytest.raises(lane_install.LaneInstallError) as excinfo:
        lane_install.airflow_floor(pyproject)

    assert excinfo.value.exit_code == 2
    assert "apache-airflow" in str(excinfo.value)


def test_the_real_pyproject_declares_a_floor_the_installer_can_read(lane_install):
    floor = lane_install.airflow_floor(ROOT / "pyproject.toml")

    assert len(floor.split(".")) == 3
    assert all(part.isdigit() for part in floor.split("."))


# --------------------------------------------------------------------------
# Downloading constraints
# --------------------------------------------------------------------------


class Response:
    def __init__(self, body: str) -> None:
        self._body = body.encode()

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_fetch_returns_the_constraints_text(lane_install, monkeypatch):
    monkeypatch.setattr(
        lane_install.urllib.request,
        "urlopen",
        lambda url, timeout=None: Response("pandas==2.1.4\n"),
    )

    text = lane_install.fetch_constraints(url_for("2.11.2"), "2.11.2", "3.11")

    assert text == "pandas==2.1.4\n"


def test_missing_constraints_fail_with_an_explicit_404_message(
    lane_install, monkeypatch
):
    def urlopen(url, timeout=None):
        raise http_error(url, 404)

    monkeypatch.setattr(lane_install.urllib.request, "urlopen", urlopen)

    with pytest.raises(lane_install.ConstraintsNotFound) as excinfo:
        lane_install.fetch_constraints(url_for("2.1.0", "3.11"), "2.1.0", "3.11")

    assert str(excinfo.value).startswith(
        "no constraints file for Airflow 2.1.0 on Python 3.11 (HTTP 404)"
    )
    assert url_for("2.1.0", "3.11") in str(excinfo.value)
    assert excinfo.value.exit_code == 1


@pytest.mark.parametrize(
    "failure",
    [http_error("https://constraints.test/x", 503), urllib.error.URLError("offline")],
)
def test_other_download_failures_name_the_url_and_the_reason(
    lane_install, monkeypatch, failure
):
    def urlopen(url, timeout=None):
        raise failure

    monkeypatch.setattr(lane_install.urllib.request, "urlopen", urlopen)

    with pytest.raises(lane_install.LaneInstallError) as excinfo:
        lane_install.fetch_constraints(url_for("2.11.2"), "2.11.2", "3.11")

    assert not isinstance(excinfo.value, lane_install.ConstraintsNotFound)
    assert excinfo.value.exit_code == 1
    assert url_for("2.11.2") in str(excinfo.value)


# --------------------------------------------------------------------------
# Pre-release requirements pinned by the target release
# --------------------------------------------------------------------------


def test_prerelease_pins_are_the_exact_prerelease_pins_of_the_recorded_metadata(
    lane_install,
):
    requires_dist = json.loads(RECORDED_PYPI_RC2.read_text(encoding="utf-8"))["info"][
        "requires_dist"
    ]

    assert lane_install.prerelease_pins(requires_dist) == RC2_PINS


@pytest.mark.parametrize(
    ("requires_dist", "expected"),
    [
        # Each pre-release kind: a, b, rc and dev.
        (
            ["w==1.0a1", "x==2.0b2", "y==3.0rc3", "z==4.0.dev5"],
            ("w==1.0a1", "x==2.0b2", "y==3.0rc3", "z==4.0.dev5"),
        ),
        # Optional-dependency entries are not installed unless the extra is asked for.
        (['x==1.0rc1; extra == "all"'], ()),
        (['x==1.0rc1; python_version != "3.14" and extra == "all"'], ()),
        (["x==1.0rc1; (python_version >= '3.10') and extra=='all'"], ()),
        # Other markers stay on the requirement, so uv decides whether they apply.
        (
            ['x==1.0rc1; python_version >= "3.10"'],
            ('x==1.0rc1; python_version >= "3.10"',),
        ),
        (["x[y]==1.0rc1"], ("x[y]==1.0rc1",)),
        # Only exact pins of pre-releases count.
        (["x==2.0.0", "x==2.0.0.post1", "x==2.0"], ()),
        (["x>=1.0rc1", "x~=1.0rc1", "x!=1.0rc1", "x<=1.0rc1", "x<1.0rc1"], ()),
        (["x==1.0rc1,!=1.0rc2", "x==1.*", "x"], ()),
        (["not a requirement", "", "x @ https://example.test/x.whl"], ()),
        # Repeats collapse to one, in order of appearance.
        (["b==1.0rc1", "a==2.0b1", "b==1.0rc1"], ("b==1.0rc1", "a==2.0b1")),
    ],
)
def test_prerelease_pins_select_exact_prerelease_pins_outside_extras(
    lane_install, requires_dist, expected
):
    assert lane_install.prerelease_pins(requires_dist) == expected


def metadata_json(requires_dist: object) -> str:
    return json.dumps(
        {"info": {"name": "apache-airflow", "requires_dist": requires_dist}}
    )


def test_fetching_metadata_reads_the_requirements_of_that_release(
    lane_install, monkeypatch
):
    seen: list[str] = []

    def urlopen(url, timeout=None):
        seen.append(url)
        return Response(RECORDED_PYPI_RC2.read_text(encoding="utf-8"))

    monkeypatch.setattr(lane_install.urllib.request, "urlopen", urlopen)

    requires_dist = lane_install.fetch_requires_dist("apache-airflow", "3.4.0rc2")

    assert seen == [pypi_url("3.4.0rc2")]
    assert list(RC2_PINS) == requires_dist[:2]


def test_a_release_without_requirements_has_none(lane_install, monkeypatch):
    monkeypatch.setattr(
        lane_install.urllib.request,
        "urlopen",
        lambda url, timeout=None: Response(metadata_json(None)),
    )

    assert lane_install.fetch_requires_dist("apache-airflow", "3.4.0rc2") == []


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (http_error(pypi_url("3.4.0rc2"), 404), "HTTP 404"),
        (http_error(pypi_url("3.4.0rc2"), 503), "HTTP 503"),
        (urllib.error.URLError("offline"), "offline"),
        ("not json", "JSON"),
        ("{}", "info"),
        ('{"info": {"requires_dist": "oops"}}', "requires_dist"),
    ],
)
def test_unreadable_metadata_fails_naming_the_release_and_the_url(
    lane_install, monkeypatch, failure, reason
):
    def urlopen(url, timeout=None):
        if isinstance(failure, Exception):
            raise failure
        return Response(failure)

    monkeypatch.setattr(lane_install.urllib.request, "urlopen", urlopen)

    with pytest.raises(lane_install.LaneInstallError) as excinfo:
        lane_install.fetch_requires_dist("apache-airflow", "3.4.0rc2")

    assert excinfo.value.exit_code == 1
    message = str(excinfo.value)
    assert "apache-airflow 3.4.0rc2" in message
    assert pypi_url("3.4.0rc2") in message
    assert reason in message


# --------------------------------------------------------------------------
# Command composition (no network, no uv)
# --------------------------------------------------------------------------

REPO = Path("/work/provider")
BASE = ["uv", "pip", "install", "--python", "/venv/bin/python"]


def compose(lane_install, mode, **kwargs):
    return lane_install.build_command(
        mode, uv="uv", python_executable="/venv/bin/python", repo_root=REPO, **kwargs
    )


def test_constrained_pins_airflow_and_applies_the_filtered_constraints(lane_install):
    command = compose(
        lane_install,
        "constrained",
        airflow="2.11.2",
        constraints_file=Path("/tmp/c.txt"),
        extras=("tests",),
    )

    assert command == [
        *BASE,
        "--resolution",
        "highest",
        "--constraint",
        "/tmp/c.txt",
        "apache-airflow==2.11.2",
        "--editable",
        "/work/provider[tests]",
    ]


def test_latest_pins_nothing_and_uses_no_constraints(lane_install):
    command = compose(lane_install, "latest", extras=("tests",))

    assert command == [
        *BASE,
        "--resolution",
        "highest",
        "--editable",
        "/work/provider[tests]",
    ]


def test_lowest_direct_resolves_direct_dependencies_at_their_lowest(lane_install):
    command = compose(
        lane_install,
        "lowest-direct",
        airflow="2.11.0",
        constraints_file=Path("/tmp/c.txt"),
        extras=("tests",),
    )

    assert command == [
        *BASE,
        "--resolution",
        "lowest-direct",
        "--constraint",
        "/tmp/c.txt",
        "apache-airflow==2.11.0",
        "--editable",
        "/work/provider[tests]",
    ]


def test_prerelease_pins_the_target_and_each_of_its_prerelease_requirements(
    lane_install,
):
    command = compose(
        lane_install,
        "prerelease",
        airflow="3.4.0rc2",
        constraints_file=Path("/tmp/c.txt"),
        extras=("tests",),
        prerelease_pins=RC2_PINS,
        with_requirements=("great-expectations==1.25.0rc1",),
    )

    assert command == [
        *BASE,
        "--resolution",
        "highest",
        "--constraint",
        "/tmp/c.txt",
        "apache-airflow==3.4.0rc2",
        "apache-airflow-core==3.4.0rc2",
        "apache-airflow-task-sdk==1.4.0rc2",
        "--editable",
        "/work/provider[tests]",
        "great-expectations==1.25.0rc1",
    ]
    # Pre-releases are allowed only where they are pinned, never wholesale.
    assert not any(argument.startswith("--prerelease") for argument in command)


def test_prerelease_without_published_constraints_keeps_the_prerelease_pins(
    lane_install,
):
    command = compose(
        lane_install,
        "prerelease",
        airflow="3.4.0rc2",
        extras=("tests",),
        prerelease_pins=RC2_PINS,
    )

    assert command == [
        *BASE,
        "--resolution",
        "highest",
        "apache-airflow==3.4.0rc2",
        "apache-airflow-core==3.4.0rc2",
        "apache-airflow-task-sdk==1.4.0rc2",
        "--editable",
        "/work/provider[tests]",
    ]


def test_next_python_installs_like_latest(lane_install):
    assert compose(lane_install, "next-python", extras=("tests",)) == compose(
        lane_install, "latest", extras=("tests",)
    )


def test_tools_installs_the_lint_pins_and_the_script_requirements_only(lane_install):
    command = compose(
        lane_install,
        "tools",
        tool_pins=("mypy==1.14.1", "ruff==0.8.3"),
        scripts_requirements=REPO / ".github/scripts/requirements.txt",
    )

    assert command == [
        *BASE,
        "mypy==1.14.1",
        "ruff==0.8.3",
        "--requirement",
        "/work/provider/.github/scripts/requirements.txt",
    ]


def test_several_extras_and_no_extras(lane_install):
    several = compose(lane_install, "latest", extras=("postgresql", "tests"))
    none = compose(lane_install, "latest")

    assert several[-1] == "/work/provider[postgresql,tests]"
    assert none[-1] == "/work/provider"


# --------------------------------------------------------------------------
# Argument handling
# --------------------------------------------------------------------------


def test_every_mode_parses(lane_install):
    modes = {
        "constrained",
        "latest",
        "lowest-direct",
        "prerelease",
        "next-python",
        "tools",
    }

    assert set(lane_install.MODES) == modes


def test_a_constrained_request_carries_its_inputs(lane_install):
    request = lane_install.parse_request(
        [
            "--mode",
            "constrained",
            "--airflow",
            "3.1.8",
            "--python",
            "3.12",
            "--extras",
            "postgresql,tests",
            "--with",
            "pyspark==4.1.3",
            "--with",
            "pandas==2.1.4",
            "other==1",
        ]
    )

    assert request.mode == "constrained"
    assert request.airflow == "3.1.8"
    assert request.python == "3.12"
    assert request.extras == ("postgresql", "tests")
    assert request.with_requirements == ("pyspark==4.1.3", "pandas==2.1.4", "other==1")
    assert request.constraints_url is None
    assert request.constraints_template is None


def test_empty_url_template_and_requirement_values_count_as_absent(lane_install):
    request = lane_install.parse_request(
        [
            "--mode",
            "lowest-direct",
            "--airflow",
            "floor",
            "--constraints-url",
            "",
            "--constraints-template",
            "https://constraints.test/{airflow}/{python}.txt",
            "--with",
            "",
        ]
    )

    assert request.constraints_url is None
    assert request.constraints_template == (
        "https://constraints.test/{airflow}/{python}.txt"
    )
    assert request.with_requirements == ()


@pytest.mark.parametrize(
    ("argv", "fragment"),
    [
        (["--mode", "constrained"], "--airflow is required"),
        (["--mode", "lowest-direct"], "--airflow is required"),
        (["--mode", "prerelease"], "--airflow is required"),
        (["--mode", "latest", "--airflow", "2.11.2"], "--airflow is not used"),
        (["--mode", "next-python", "--airflow", "2.11.2"], "--airflow is not used"),
        (["--mode", "tools", "--airflow", "2.11.2"], "--airflow is not used"),
        (["--mode", "tools", "--extras", "tests"], "--extras is not used"),
        (["--mode", "tools", "--with", "x==1"], "--with is not used"),
        (
            ["--mode", "latest", "--constraints-url", "https://x.test/c.txt"],
            "--constraints-url is not used",
        ),
        (
            ["--mode", "latest", "--constraints-template", "https://x.test/{airflow}"],
            "--constraints-template is not used",
        ),
        (
            ["--mode", "constrained", "--airflow", "floor"],
            "'floor' is only valid with --mode lowest-direct",
        ),
        (
            [
                "--mode",
                "constrained",
                "--airflow",
                "2.11.2",
                "--constraints-url",
                "https://x.test/c.txt",
                "--constraints-template",
                "https://x.test/{airflow}/{python}.txt",
            ],
            "only one of --constraints-url and --constraints-template",
        ),
        (
            ["--mode", "constrained", "--airflow", "2.11.2; echo hi"],
            "invalid --airflow",
        ),
        (
            ["--mode", "constrained", "--airflow", "2.11.2", "--python", "three"],
            "invalid --python",
        ),
        (
            ["--mode", "constrained", "--airflow", "2.11.2", "--extras", "tests;x"],
            "invalid --extras",
        ),
        (
            ["--mode", "latest", "--with=--index-url=https://evil.test"],
            "--with requirements must not start with '-'",
        ),
    ],
)
def test_invalid_requests_exit_2_and_say_why(lane_install, argv, fragment):
    with pytest.raises(lane_install.LaneInstallError) as excinfo:
        lane_install.parse_request(argv)

    assert excinfo.value.exit_code == 2
    assert fragment in str(excinfo.value)


def test_pre_release_airflow_versions_are_accepted_for_the_prerelease_mode(
    lane_install,
):
    request = lane_install.parse_request(
        ["--mode", "prerelease", "--airflow", "3.4.0rc1"]
    )

    assert request.airflow == "3.4.0rc1"


# --------------------------------------------------------------------------
# The whole run, with the network and uv replaced
# --------------------------------------------------------------------------


class Harness:
    """Replace the network, uv and the environment's installed versions."""

    def __init__(self, lane_install: Any, monkeypatch: pytest.MonkeyPatch, repo: Path):
        self.lane_install = lane_install
        self.monkeypatch = monkeypatch
        self.repo = repo
        self.served: dict[str, str] = {}
        self.failures: dict[str, Exception] = {}
        self.fetched: list[str] = []
        self.commands: list[list[str]] = []
        self.constraint_files: list[str] = []
        self.install_returncode = 0
        self.versions = {
            "apache-airflow": "2.11.2",
            "great-expectations": "1.24.1",
            "pytest": "8.3.4",
        }

    def urlopen(self, url: str, timeout: float | None = None) -> Response:
        self.fetched.append(url)
        if url in self.failures:
            raise self.failures[url]
        if url not in self.served:
            raise http_error(url, 404)
        return Response(self.served[url])

    def run(
        self, command: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        if command[1:] == ["--version"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "uv 0.11.21 (Homebrew 2026-06-11 aarch64-apple-darwin)\n",
                "",
            )
        assert command[1:3] == ["pip", "install"], command
        self.commands.append(command)
        if "--constraint" in command:
            path = Path(command[command.index("--constraint") + 1])
            self.constraint_files.append(path.read_text(encoding="utf-8"))
        return subprocess.CompletedProcess(command, self.install_returncode, "", "")

    def main(self, *argv: str) -> int:
        with self.monkeypatch.context() as patch:
            patch.setattr(self.lane_install.urllib.request, "urlopen", self.urlopen)
            patch.setattr(self.lane_install.subprocess, "run", self.run)
            patch.setattr(self.lane_install.shutil, "which", lambda name: "/bin/uv")
            patch.setattr(
                self.lane_install, "installed_versions", lambda names: self.versions
            )
            return self.lane_install.main(list(argv), repo_root=self.repo)


@pytest.fixture
def harness(lane_install, monkeypatch, make_repo):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    return Harness(lane_install, monkeypatch, make_repo())


def test_constrained_install_filters_the_constraints_installs_and_reports(
    harness, monkeypatch, capsys, tmp_path
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    recorded = RECORDED_CONSTRAINTS.read_text(encoding="utf-8")
    harness.served[url_for("2.11.2")] = recorded

    status = harness.main(
        "--mode", "constrained", "--airflow", "2.11.2", "--extras", "tests"
    )

    assert status == 0
    assert harness.fetched == [url_for("2.11.2")]
    (command,) = harness.commands
    assert command[:5] == ["uv", "pip", "install", "--python", sys.executable]
    assert "apache-airflow==2.11.2" in command
    (installed_constraints,) = harness.constraint_files
    assert installed_constraints.splitlines() == [
        line
        for line in recorded.splitlines()
        if line
        not in {
            "mypy==1.9.0",
            "pytest-mock==3.15.1",
            "pytest==8.2.1",
            "ruff==0.5.5",
        }
    ]
    out = capsys.readouterr().out
    for removed in (
        "mypy==1.9.0",
        "pytest-mock==3.15.1",
        "pytest==8.2.1",
        "ruff==0.5.5",
    ):
        assert removed in out
    for row in (
        f"| Python | {sys.version.split()[0]} |",
        "| uv | 0.11.21 |",
        "| apache-airflow | 2.11.2 |",
        "| great-expectations | 1.24.1 |",
        "| pytest | 8.3.4 |",
    ):
        assert row in out
        assert row in summary.read_text(encoding="utf-8")
    assert "| pandas |" not in out


def test_each_install_appends_to_the_job_summary(harness, monkeypatch, tmp_path):
    summary = tmp_path / "summary.md"
    summary.write_text("earlier step\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    assert harness.main("--mode", "latest") == 0
    assert harness.main("--mode", "next-python") == 0

    text = summary.read_text(encoding="utf-8")
    assert text.startswith("earlier step\n")
    assert text.count("### Lane install: latest") == 1
    assert text.count("### Lane install: next-python") == 1


def test_a_template_that_does_not_fit_exits_2(harness, capsys):
    status = harness.main(
        "--mode",
        "constrained",
        "--airflow",
        "2.11.2",
        "--constraints-template",
        "https://constraints.test/{release}/{python}.txt",
    )

    assert status == 2
    assert harness.fetched == []
    assert "invalid constraints template" in capsys.readouterr().err


def test_an_unreadable_matrix_exits_2_naming_the_file(harness, capsys):
    (harness.repo / ".github/support-matrix.toml").write_text(
        "schema_version = 2\n", encoding="utf-8"
    )

    status = harness.main("--mode", "constrained", "--airflow", "2.11.2")

    assert status == 2
    assert "support-matrix.toml" in capsys.readouterr().err
    assert harness.commands == []


def test_the_job_summary_is_optional(harness, capsys):
    harness.served[url_for("2.11.2")] = "pandas==2.1.4\n"

    status = harness.main("--mode", "constrained", "--airflow", "2.11.2")

    assert status == 0
    assert "| apache-airflow | 2.11.2 |" in capsys.readouterr().out


def test_constraints_come_from_the_matrix_template_unless_overridden(harness):
    harness.served[url_for("2.11.2")] = "pandas==2.1.4\n"
    harness.served[f"https://mirror.test/2.11.2/py{PY}.txt"] = "pandas==2.1.4\n"
    harness.served["https://mirror.test/exact.txt"] = "pandas==2.1.4\n"
    template = "https://mirror.test/{airflow}/py{python}.txt"

    assert harness.main("--mode", "constrained", "--airflow", "2.11.2") == 0
    assert (
        harness.main(
            "--mode",
            "constrained",
            "--airflow",
            "2.11.2",
            "--constraints-template",
            template,
        )
        == 0
    )
    assert (
        harness.main(
            "--mode",
            "constrained",
            "--airflow",
            "2.11.2",
            "--constraints-url",
            "https://mirror.test/exact.txt",
        )
        == 0
    )

    assert harness.fetched == [
        url_for("2.11.2"),
        f"https://mirror.test/2.11.2/py{PY}.txt",
        "https://mirror.test/exact.txt",
    ]


def test_missing_floor_constraints_fail_without_falling_back(
    make_repo, lane_install, monkeypatch, capsys
):
    harness = Harness(lane_install, monkeypatch, make_repo(">=2.1, <4"))
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    status = harness.main(
        "--mode", "lowest-direct", "--airflow", "floor", "--extras", "tests"
    )

    assert status == 1
    assert harness.fetched == [url_for("2.1.0")]
    assert harness.commands == []
    err = capsys.readouterr().err
    assert f"no constraints file for Airflow 2.1.0 on Python {PY} (HTTP 404)" in err


def test_lowest_direct_installs_the_floor_under_its_own_constraints(harness, capsys):
    harness.served[url_for("2.11.0")] = "pandas==2.1.4\nruff==0.5.5\n"

    status = harness.main(
        "--mode", "lowest-direct", "--airflow", "floor", "--extras", "tests"
    )

    assert status == 0
    (command,) = harness.commands
    assert command[command.index("--resolution") + 1] == "lowest-direct"
    assert "apache-airflow==2.11.0" in command
    assert harness.constraint_files == ["pandas==2.1.4\n"]


def test_prerelease_pins_its_prerelease_requirements_under_published_constraints(
    harness, monkeypatch, capsys, tmp_path
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    harness.served[url_for("3.4.0rc2")] = "pandas==2.1.4\nruff==0.5.5\n"
    harness.served[pypi_url("3.4.0rc2")] = RECORDED_PYPI_RC2.read_text(encoding="utf-8")

    status = harness.main(
        "--mode", "prerelease", "--airflow", "3.4.0rc2", "--extras", "tests"
    )

    assert status == 0
    assert harness.fetched == [url_for("3.4.0rc2"), pypi_url("3.4.0rc2")]
    (command,) = harness.commands
    assert harness.constraint_files == ["pandas==2.1.4\n"]
    pinned = command[command.index("apache-airflow==3.4.0rc2") :][:3]
    assert pinned == ["apache-airflow==3.4.0rc2", *RC2_PINS]
    assert not any(argument.startswith("--prerelease") for argument in command)
    for text in (capsys.readouterr().out, summary.read_text(encoding="utf-8")):
        for pin in RC2_PINS:
            assert f"`{pin}`" in text


def test_prerelease_without_published_constraints_installs_and_says_so(
    harness, monkeypatch, capsys, tmp_path
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    harness.served[pypi_url("3.4.0rc2")] = RECORDED_PYPI_RC2.read_text(encoding="utf-8")

    status = harness.main(
        "--mode",
        "prerelease",
        "--airflow",
        "3.4.0rc2",
        "--constraints-url",
        "",
        "--with",
        "great-expectations==1.25.0rc1",
        "--extras",
        "tests",
    )

    assert status == 0
    assert harness.fetched == [url_for("3.4.0rc2"), pypi_url("3.4.0rc2")]
    (command,) = harness.commands
    assert "--constraint" not in command
    assert command[command.index("apache-airflow==3.4.0rc2") :][:3] == [
        "apache-airflow==3.4.0rc2",
        *RC2_PINS,
    ]
    assert "great-expectations==1.25.0rc1" in command
    note = f"no constraints file for Airflow 3.4.0rc2 on Python {PY} (HTTP 404)"
    assert note in capsys.readouterr().out
    assert note in summary.read_text(encoding="utf-8")
    assert "without constraints" in summary.read_text(encoding="utf-8")


def test_a_prerelease_without_prerelease_requirements_adds_none(harness):
    harness.served[url_for("3.4.0b1")] = "pandas==2.1.4\n"
    harness.served[pypi_url("3.4.0b1")] = metadata_json(["pendulum>=3.0"])

    status = harness.main("--mode", "prerelease", "--airflow", "3.4.0b1")

    assert status == 0
    (command,) = harness.commands
    assert command[command.index("apache-airflow==3.4.0b1") + 1] == "--editable"


def test_a_final_release_target_needs_no_prerelease_metadata(harness):
    harness.served[url_for("3.3.2")] = "pandas==2.1.4\n"

    status = harness.main("--mode", "prerelease", "--airflow", "3.3.2")

    assert status == 0
    assert harness.fetched == [url_for("3.3.2")]
    (command,) = harness.commands
    assert command[command.index("apache-airflow==3.3.2") + 1] == "--editable"


@pytest.mark.parametrize(
    "failure",
    [http_error(pypi_url("3.4.0rc2"), 404), urllib.error.URLError("offline")],
)
def test_unreadable_prerelease_metadata_fails_the_lane_without_a_fallback(
    harness, monkeypatch, capsys, tmp_path, failure
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    harness.served[url_for("3.4.0rc2")] = "pandas==2.1.4\n"
    harness.failures[pypi_url("3.4.0rc2")] = failure

    status = harness.main("--mode", "prerelease", "--airflow", "3.4.0rc2")

    assert status == 1
    assert harness.commands == []
    err = capsys.readouterr().err
    assert "apache-airflow 3.4.0rc2" in err and pypi_url("3.4.0rc2") in err
    assert pypi_url("3.4.0rc2") in summary.read_text(encoding="utf-8")


def test_other_modes_never_read_pypi_metadata(harness):
    harness.served[url_for("2.11.2")] = "pandas==2.1.4\n"
    assert harness.main("--mode", "latest") == 0
    assert harness.main("--mode", "constrained", "--airflow", "2.11.2") == 0
    assert all("pypi.org" not in url for url in harness.fetched)


def test_unreachable_constraints_fail_the_lane(harness, capsys):
    harness.failures[url_for("2.11.2")] = urllib.error.URLError("offline")

    status = harness.main("--mode", "constrained", "--airflow", "2.11.2")

    assert status == 1
    assert harness.commands == []
    assert url_for("2.11.2") in capsys.readouterr().err


def test_a_failed_install_fails_the_lane_and_skips_the_report(
    harness, monkeypatch, capsys, tmp_path
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    harness.served[url_for("3.3.2")] = "structlog==26.1.0\n"
    harness.install_returncode = 1

    status = harness.main("--mode", "constrained", "--airflow", "3.3.2")

    assert status == 1
    assert "| apache-airflow |" not in capsys.readouterr().out
    text = summary.read_text(encoding="utf-8")
    assert "uv exited with status 1" in text
    assert "| apache-airflow |" not in text


@pytest.mark.parametrize("mode", ["latest", "next-python"])
def test_unconstrained_modes_download_nothing(harness, mode):
    status = harness.main("--mode", mode, "--extras", "tests")

    assert status == 0
    assert harness.fetched == []
    (command,) = harness.commands
    assert "--constraint" not in command
    assert not any(argument.startswith("apache-airflow") for argument in command)


def test_tools_installs_the_lint_pins_and_the_script_requirements(harness, capsys):
    status = harness.main("--mode", "tools")

    assert status == 0
    assert harness.fetched == []
    (command,) = harness.commands
    assert command[5:] == [
        "mypy==1.14.1",
        "ruff==0.8.3",
        "pytest==8.3.4",
        "pytest-mock==3.14.0",
        "--requirement",
        str(harness.repo / ".github/scripts/requirements.txt"),
    ]
    assert "| pytest | 8.3.4 |" in capsys.readouterr().out


def test_an_interpreter_that_is_not_the_lane_python_is_rejected(harness, capsys):
    status = harness.main(
        "--mode", "constrained", "--airflow", "2.11.2", "--python", "3.9"
    )

    assert status == 2
    assert harness.fetched == []
    assert harness.commands == []
    err = capsys.readouterr().err
    assert "3.9" in err and PY in err


def test_an_invalid_request_exits_2_before_doing_anything(harness, capsys):
    status = harness.main("--mode", "constrained")

    assert status == 2
    assert harness.fetched == []
    assert harness.commands == []
    assert "--airflow is required" in capsys.readouterr().err


def test_a_missing_uv_exits_2(harness, lane_install, monkeypatch, capsys):
    monkeypatch.setattr(lane_install.shutil, "which", lambda name: None)
    monkeypatch.setattr(lane_install.subprocess, "run", harness.run)

    status = lane_install.main(["--mode", "latest"], repo_root=harness.repo)

    assert status == 2
    assert harness.commands == []
    assert "uv" in capsys.readouterr().err


def test_the_report_lists_only_what_is_installed(lane_install):
    report = lane_install.render_report(
        heading="Lane install: constrained, Airflow 2.11.2, Python 3.11",
        rows=[("Python", "3.11.15"), ("uv", "0.11.21"), ("pytest", "8.3.4")],
        notes=["Exempt pins removed: pytest==8.2.1"],
    )

    assert report.splitlines()[:2] == [
        "### Lane install: constrained, Airflow 2.11.2, Python 3.11",
        "",
    ]
    assert "| Component | Version |" in report
    assert "| pytest | 8.3.4 |" in report
    assert "| pandas |" not in report
    assert "Exempt pins removed: pytest==8.2.1" in report


def test_installed_versions_reports_installed_distributions_and_skips_the_rest(
    lane_install,
):
    versions = lane_install.installed_versions(
        ("pytest", "definitely-not-an-installed-distribution")
    )

    assert set(versions) == {"pytest"}
    assert versions["pytest"] == pytest.__version__


def test_the_installer_runs_on_an_interpreter_without_packaging():
    """Lane jobs run the installer on a bare interpreter before installing anything."""
    program = f"""
import importlib.util, sys
from pathlib import Path
sys.modules["packaging"] = None  # any import of packaging now fails
spec = importlib.util.spec_from_file_location("lane_install_bare", {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
assert module.parse_floor("apache-airflow>=2.1, <4") == "2.1.0"
filtered, removed = module.filter_constraints("ruff==0.5.5\\npandas==2.1.4\\n", frozenset({{"ruff"}}))
assert (filtered, removed) == ("pandas==2.1.4\\n", ("ruff==0.5.5",))
assert module.load_matrix(Path({str(MATRIX)!r})).constraints_url.startswith("https://")
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program], capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
