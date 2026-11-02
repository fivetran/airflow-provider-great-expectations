from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

MATRIX_PATH = Path(__file__).parents[2] / ".github" / "support-matrix.toml"
SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "support_matrix.py"
# A frozen snapshot of the matrix. Tests of the scripts' behaviour read it, not the
# checked-in matrix, so a routine matrix update does not change what they expect.
FROZEN_MATRIX_PATH = Path(__file__).parent / "fixtures" / "ci" / "support-matrix.toml"
# A frozen copy of the package metadata. Tests of the consistency check assert its
# findings against this, not the repository's own pyproject.toml, so changing the
# package's declared support does not change what they expect.
FROZEN_PYPROJECT_PATH = (
    Path(__file__).parent / "fixtures" / "ci" / "sample-pyproject.toml"
)
RENDER_HEADER = (
    "<!-- Generated from `.github/support-matrix.toml`; run "
    "`python .github/scripts/support_matrix.py render --write`. -->\n"
    "| Airflow | Tested release | Python tested in CI | "
    "Python supported by this Airflow line |\n"
    "| --- | --- | --- | --- |\n"
)


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def _overlap_tier_flags(text: str) -> str:
    return text.replace("always_true = []", 'always_true = ["AIRFLOW_V_3_0_PLUS"]')


@pytest.fixture
def support_matrix_module():
    spec = importlib.util.spec_from_file_location(
        "support_matrix_under_test", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def frozen_matrix(support_matrix_module):
    return support_matrix_module.load(FROZEN_MATRIX_PATH)


@pytest.fixture
def checked_in_matrix(support_matrix_module):
    return support_matrix_module.load(MATRIX_PATH)


@pytest.fixture
def valid_matrix_text() -> str:
    return """schema_version = 1

[policy]
reference_provider = "apache-airflow-providers-common-sql"

[python]
supported = ["3.11", "3.12"]
next = "3.13"

[[airflow]]
line = "2.11"
release = "2.11.2"
tier = "2.11"
python = ["3.11"]

[[airflow]]
line = "3.0"
release = "3.0.6"
tier = "3.0"
python = ["3.11"]

[[airflow]]
line = "3.1"
release = "3.1.8"
tier = "3.1+"
python = ["3.11", "3.12"]

[tiers."2.11"]
always_true = []
always_false = ["AIRFLOW_V_3_0_PLUS", "AIRFLOW_V_3_1_PLUS"]

[tiers."3.0"]
always_true = ["AIRFLOW_V_3_0_PLUS"]
always_false = ["AIRFLOW_V_3_1_PLUS"]

[tiers."3.1+"]
always_true = ["AIRFLOW_V_3_0_PLUS", "AIRFLOW_V_3_1_PLUS"]
always_false = []

[install]
constraints_url = "https://example.test/constraints-{airflow}/constraints-{python}.txt"
uv = "0.11.24"

[services.postgres]
image = "postgres:17.11-bookworm@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
lanes = ["floor", "newest"]

[services.spark]
image = "apache/spark:4.1.3-java17@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
pyspark = "4.1.3"
lanes = ["newest"]

[extras]
athena = ["pyathena"]
"""


@pytest.fixture
def current_matrix_text(valid_matrix_text: str) -> str:
    """A fixture copy of today's lane and tier policy for workflow output tests."""
    text = valid_matrix_text.replace(
        'supported = ["3.11", "3.12"]',
        'supported = ["3.11", "3.12", "3.13", "3.14"]',
    ).replace('next = "3.13"', 'next = "3.15"')
    return text.replace(
        '[tiers."2.11"]',
        '[[airflow]]\nline = "3.2"\nrelease = "3.2.2"\ntier = "3.1+"\npython = ["3.11", "3.12", "3.13", "3.14"]\n\n[[airflow]]\nline = "3.3"\nrelease = "3.3.2"\ntier = "3.1+"\npython = ["3.11", "3.12", "3.13", "3.14"]\n\n[tiers."2.11"]',
    )


@pytest.mark.parametrize(
    ("mutate", "entry"),
    [
        (
            lambda s: s.replace("schema_version = 1", "schema_version = 2"),
            "schema_version",
        ),
        (
            lambda s: s.replace("schema_version = 1", "schema_version = 1.0"),
            "schema_version",
        ),
        (
            lambda s: s.replace('["3.11", "3.12"]', '["3.12", "3.11"]', 1),
            "python.supported",
        ),
        (lambda s: s.replace('next = "3.13"', 'next = "3.12"'), "python.next"),
        (
            lambda s: s.replace('release = "2.11.2"', 'release = "2.12.0"'),
            "airflow[0].release",
        ),
        (
            lambda s: s.replace('python = ["3.11"]', 'python = ["3.12"]', 1),
            "airflow[0].python",
        ),
        (
            lambda s: s.replace('python = ["3.11", "3.12"]', 'python = ["3.11"]'),
            "airflow[2].python",
        ),
        (_overlap_tier_flags, "tiers."),
        (
            lambda s: s.replace("constraints-{python}.txt", "constraints-python.txt"),
            "install.constraints_url",
        ),
        (
            lambda s: s.replace("@sha256:aaaaaaaa", "@sha256:invalid"),
            "services.postgres.image",
        ),
        (lambda s: s.replace('athena = ["pyathena"]', "athena = []"), "extras.athena"),
    ],
)
def test_rejects_invalid_matrix_and_names_entry(
    support_matrix_module, valid_matrix_text, tmp_path, mutate, entry
):
    path = tmp_path / "matrix.toml"
    path.write_text(mutate(valid_matrix_text))

    with pytest.raises(support_matrix_module.MatrixError) as error:
        support_matrix_module.load(path)

    assert entry in str(error.value)


def test_loads_checked_in_matrix(checked_in_matrix):
    """The checked-in matrix changes with every routine update, so assert its shape."""
    matrix = checked_in_matrix

    assert matrix.reference_provider
    assert matrix.python_supported
    assert list(matrix.python_supported) == sorted(
        set(matrix.python_supported), key=_version
    )
    assert _version(matrix.python_next) > _version(matrix.python_supported[-1])
    assert matrix.airflow
    lines = [line.line for line in matrix.airflow]
    assert lines == sorted(set(lines), key=_version)
    for airflow in matrix.airflow:
        assert re.fullmatch(r"\d+\.\d+\.\d+", airflow.release)
        assert airflow.release.rsplit(".", 1)[0] == airflow.line
    assert matrix.airflow[-1].python == matrix.python_supported
    assert all(matrix.extras.values())


def test_checked_in_matrix_derives_a_floor_a_newest_and_a_lane_per_python(
    support_matrix_module, checked_in_matrix
):
    matrix = checked_in_matrix
    lowest_python = matrix.python_supported[0]

    lanes = support_matrix_module.derive_lanes(matrix)
    floor = support_matrix_module.floor_lane(matrix)
    newest = support_matrix_module.newest_lane(matrix)

    assert len(lanes) == len(matrix.airflow) + len(matrix.python_supported) - 1
    assert (floor.airflow, floor.python) == (matrix.airflow[0].release, lowest_python)
    assert (newest.airflow, newest.python) == (
        matrix.airflow[-1].release,
        lowest_python,
    )
    assert {lane.python for lane in lanes} == set(matrix.python_supported)


def test_loads_frozen_matrix_fixture(frozen_matrix):
    matrix = frozen_matrix

    assert matrix.reference_provider == "apache-airflow-providers-common-sql"
    assert matrix.python_supported == ("3.11", "3.12", "3.13", "3.14")
    assert matrix.python_next == "3.15"
    assert [line.release for line in matrix.airflow] == [
        "2.11.2",
        "3.0.6",
        "3.1.8",
        "3.2.2",
        "3.3.2",
    ]


def test_frozen_matrix_derives_eight_lanes_with_floor_and_newest_roles(
    support_matrix_module, frozen_matrix
):
    lanes = support_matrix_module.derive_lanes(frozen_matrix)

    assert [(lane.airflow, lane.python, lane.roles) for lane in lanes] == [
        ("2.11.2", "3.11", ("floor", "line")),
        ("3.0.6", "3.11", ("line",)),
        ("3.1.8", "3.11", ("line",)),
        ("3.2.2", "3.11", ("line",)),
        ("3.3.2", "3.11", ("newest", "line")),
        ("3.3.2", "3.12", ("python-range",)),
        ("3.3.2", "3.13", ("python-range",)),
        ("3.3.2", "3.14", ("python-range",)),
    ]
    assert support_matrix_module.floor_lane(frozen_matrix) == lanes[0]
    assert support_matrix_module.newest_lane(frozen_matrix) == lanes[4]


def test_cli_validates_real_matrix(support_matrix_module):
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--matrix", str(MATRIX_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_cli_reports_invalid_entry_and_exits_two(valid_matrix_text, tmp_path):
    path = tmp_path / "invalid.toml"
    path.write_text(
        valid_matrix_text.replace("schema_version = 1", "schema_version = 2")
    )

    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--matrix", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "schema_version" in result.stderr


def test_cli_rejects_non_integer_schema_version(valid_matrix_text, tmp_path):
    path = tmp_path / "invalid.toml"
    path.write_text(
        valid_matrix_text.replace("schema_version = 1", "schema_version = 1.0")
    )

    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--matrix", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "schema_version" in result.stderr


def test_emit_rejects_unknown_constraints_template_field(valid_matrix_text, tmp_path):
    matrix_path = tmp_path / "invalid-template.toml"
    matrix_path.write_text(
        valid_matrix_text.replace(
            "constraints-{airflow}/constraints-{python}.txt",
            "constraints-{airflow}-{unknown}/constraints-{python}.txt",
        )
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "emit", "--matrix", str(matrix_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "install.constraints_url" in result.stderr
    assert "Traceback" not in result.stderr


def test_lane_derivation_tracks_matrix_fixture(
    support_matrix_module, valid_matrix_text, tmp_path
):
    path = tmp_path / "matrix.toml"
    path.write_text(valid_matrix_text)
    matrix = support_matrix_module.load(path)

    lanes = support_matrix_module.derive_lanes(matrix)
    assert [(lane.airflow, lane.python, lane.roles) for lane in lanes] == [
        ("2.11.2", "3.11", ("floor", "line")),
        ("3.0.6", "3.11", ("line",)),
        ("3.1.8", "3.11", ("newest", "line")),
        ("3.1.8", "3.12", ("python-range",)),
    ]
    assert support_matrix_module.floor_lane(matrix) == lanes[0]
    assert support_matrix_module.newest_lane(matrix) == lanes[2]

    changed = valid_matrix_text.replace(
        'supported = ["3.11", "3.12"]', 'supported = ["3.11", "3.12", "3.13"]'
    )
    changed = changed.replace('next = "3.13"', 'next = "3.14"')
    changed = changed.replace(
        'python = ["3.11", "3.12"]', 'python = ["3.11", "3.12", "3.13"]'
    )
    path.write_text(changed)
    assert (
        len(support_matrix_module.derive_lanes(support_matrix_module.load(path))) == 5
    )


def test_typecheck_targets_repeat_mypy_flag_per_name(
    support_matrix_module, valid_matrix_text, tmp_path
):
    path = tmp_path / "matrix.toml"
    path.write_text(valid_matrix_text)
    targets = support_matrix_module.typecheck_targets(support_matrix_module.load(path))

    assert [
        (target.tier, target.lane.airflow, target.lane.python) for target in targets
    ] == [
        ("2.11", "2.11.2", "3.11"),
        ("3.0", "3.0.6", "3.11"),
        ("3.1+", "3.1.8", "3.11"),
    ]
    assert targets[0].mypy_args == (
        "--always-false=AIRFLOW_V_3_0_PLUS",
        "--always-false=AIRFLOW_V_3_1_PLUS",
    )
    assert targets[1].mypy_args == (
        "--always-true=AIRFLOW_V_3_0_PLUS",
        "--always-false=AIRFLOW_V_3_1_PLUS",
    )


def test_emit_writes_all_matrix_outputs(
    support_matrix_module, current_matrix_text, tmp_path
):
    output = tmp_path / "github-output"
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(current_matrix_text)
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "emit",
            "--matrix",
            str(matrix_path),
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert set(values) == {
        "lanes",
        "typecheck",
        "postgres",
        "spark",
        "postgres-image",
        "spark-image",
        "pyspark-version",
        "canary-latest",
        "canary-lowest-direct",
        "lowest-python",
        "next-python",
        "uv-version",
    }
    lanes = json.loads(values["lanes"])
    assert len(lanes) == 8
    assert lanes[0]["roles"] == ["floor", "line"]
    assert json.loads(values["typecheck"])[0]["mypy_args"] == [
        "--always-false=AIRFLOW_V_3_0_PLUS",
        "--always-false=AIRFLOW_V_3_1_PLUS",
    ]
    targets = json.loads(values["typecheck"])
    assert [
        (target["tier"], target["lane"]["airflow"], target["lane"]["python"])
        for target in targets
    ] == [
        ("2.11", "2.11.2", "3.11"),
        ("3.0", "3.0.6", "3.11"),
        ("3.1+", "3.3.2", "3.11"),
    ]
    postgres_lanes = json.loads(values["postgres"])
    assert [
        (lane["airflow"], lane["python"], lane["roles"]) for lane in postgres_lanes
    ] == [
        ("2.11.2", "3.11", ["floor", "line"]),
        ("3.3.2", "3.11", ["newest", "line"]),
    ]
    spark_lanes = json.loads(values["spark"])
    assert [
        (lane["airflow"], lane["python"], lane["roles"]) for lane in spark_lanes
    ] == [
        ("3.3.2", "3.11", ["newest", "line"]),
    ]
    assert json.loads(values["canary-latest"]) == {"python": "3.14"}
    lowest_direct = json.loads(values["canary-lowest-direct"])
    assert lowest_direct[0] == {
        "airflow": "floor",
        "python": "3.11",
        "constraints_template": "https://example.test/constraints-{airflow}/constraints-{python}.txt",
    }
    assert lowest_direct[1]["airflow"] == "3.2.2"
    assert lowest_direct[1]["python"] == "3.14"


def test_render_table_has_four_columns_and_newest_line_first(
    support_matrix_module, frozen_matrix
):
    rendered = support_matrix_module.render_table(frozen_matrix)

    assert rendered.startswith(RENDER_HEADER)
    assert "| 3.3 | 3.3.2 | 3.11, 3.12, 3.13, 3.14 | 3.11–3.14 |" in rendered
    assert "| 3.0 | 3.0.6 | 3.11 | 3.11–3.12 |" in rendered
    assert rendered.index("| 3.3 |") < rendered.index("| 3.2 |")
    assert rendered.index("| 3.2 |") < rendered.index("| 3.1 |")


def test_checked_in_matrix_renders_one_four_column_row_per_line_newest_first(
    support_matrix_module, checked_in_matrix
):
    rendered = support_matrix_module.render_table(checked_in_matrix)

    assert rendered.startswith(RENDER_HEADER)
    rows = [
        [cell.strip() for cell in row.strip("|").split("|")]
        for row in rendered.removeprefix(RENDER_HEADER).splitlines()
    ]
    assert all(len(row) == 4 for row in rows)
    assert [(row[0], row[1]) for row in rows] == [
        (line.line, line.release) for line in reversed(checked_in_matrix.airflow)
    ]


@pytest.mark.parametrize(
    "path", [MATRIX_PATH, FROZEN_MATRIX_PATH], ids=["checked-in", "frozen"]
)
def test_render_table_is_stable(support_matrix_module, path):
    matrix = support_matrix_module.load(path)

    assert support_matrix_module.render_table(
        matrix
    ) == support_matrix_module.render_table(matrix)


def _consistency_repo(tmp_path: Path, pyproject: str, compose: str = "") -> Path:
    root = tmp_path / "repo"
    (root / ".github").mkdir(parents=True)
    (root / "docker/spark").mkdir(parents=True)
    (root / "pyproject.toml").write_text(pyproject)
    (root / "docker/spark/docker-compose.yml").write_text(compose)
    return root


def _frozen_repo(tmp_path: Path, support_matrix_module) -> Path:
    """A scratch repository holding the frozen package metadata and frozen matrix.

    Everything the check reads other than the metadata agrees with the matrix: the
    rendered fragment is tracked and the compose default is the matrix's image.
    """
    matrix = support_matrix_module.load(FROZEN_MATRIX_PATH)
    image = matrix.services["spark"].image
    root = _consistency_repo(
        tmp_path,
        FROZEN_PYPROJECT_PATH.read_text(encoding="utf-8"),
        f"services:\n  spark:\n    image: ${{SPARK_IMAGE:-{image}}}\n",
    )
    (root / ".github/support-matrix.toml").write_bytes(FROZEN_MATRIX_PATH.read_bytes())
    (root / ".github/support-matrix.md").write_text(
        support_matrix_module.render_table(matrix), encoding="utf-8"
    )
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(
        ["git", "-C", str(root), "add", ".github/support-matrix.md"], check=True
    )
    return root


def test_consistency_reports_the_frozen_metadata_disagreements(
    support_matrix_module, frozen_matrix, tmp_path
):
    root = _frozen_repo(tmp_path, support_matrix_module)

    findings = support_matrix_module.check_consistency(frozen_matrix, root)

    assert {(item.rule, item.field) for item in findings} == {
        ("C1", "apache-airflow floor"),
        ("C3", "requires-python floor"),
        ("C3", "requires-python upper bound"),
        ("C4", "Python classifiers"),
    }
    assert not any(item.rule == "C2" for item in findings)
    assert all(item.source for item in findings)
    by_field = {item.field: item for item in findings}
    assert (
        by_field["apache-airflow floor"].expected,
        by_field["apache-airflow floor"].actual,
    ) == ("2.11", "2.1")
    assert (
        by_field["requires-python floor"].expected,
        by_field["requires-python floor"].actual,
    ) == ("3.11", "3.9")
    assert by_field["requires-python upper bound"].actual == "<3.14"
    classifiers = by_field["Python classifiers"]
    assert classifiers.expected == ", ".join(
        f"Programming Language :: Python :: {version}"
        for version in ("3.11", "3.12", "3.13", "3.14")
    )
    assert classifiers.actual == ", ".join(
        f"Programming Language :: Python :: {version}"
        for version in ("3.10", "3.11", "3.12", "3.13")
    )


def test_consistency_accepts_2_0_shaped_metadata(
    support_matrix_module, current_matrix_text, tmp_path
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(current_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    classifiers = "\n".join(
        f'    "Programming Language :: Python :: {version}",'
        for version in ("3.11", "3.12", "3.13", "3.14")
    )
    pyproject = f"""[project]\nrequires-python = ">=3.11"\nclassifiers = [\n{classifiers}\n]\ndependencies = ["apache-airflow>=2.11.0,<4"]\n"""
    fragment = support_matrix_module.render_table(matrix)
    root = _consistency_repo(
        tmp_path,
        pyproject,
        f"services:\n  spark:\n    image: ${{SPARK_IMAGE:-apache/spark:4.1.3-java17@sha256:{'b' * 64}}}\n",
    )
    (root / ".github/support-matrix.md").write_text(fragment)
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(
        ["git", "-C", str(root), "add", ".github/support-matrix.md"], check=True
    )

    assert support_matrix_module.check_consistency(matrix, root) == []


@pytest.mark.parametrize(
    ("specifier", "expected_field"),
    [
        (">=2.11.0,!=3.1.0", "apache-airflow exclusions"),
        (">=2.11.0,<3.3", "apache-airflow tested releases"),
    ],
)
def test_consistency_detects_airflow_exclusion_and_release_outside_range(
    support_matrix_module, current_matrix_text, tmp_path, specifier, expected_field
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(current_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    root = _consistency_repo(
        tmp_path,
        f'[project]\ndependencies = ["apache-airflow{specifier}"]\n',
        "services:\n  spark:\n    image: ${SPARK_IMAGE:-x}\n",
    )

    findings = support_matrix_module.check_consistency(matrix, root)

    assert any(item.rule == "C2" and item.field == expected_field for item in findings)


def test_consistency_checks_all_marked_copies_and_compose_default(
    support_matrix_module, valid_matrix_text, tmp_path
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(valid_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    root = _consistency_repo(
        tmp_path,
        '[project]\nrequires-python = ">=3.11"\nclassifiers = ["Programming Language :: Python :: 3.11"]\ndependencies = ["apache-airflow>=2.11.0,<4"]\n',
        "services:\n  spark:\n    image: apache/spark:wrong\n",
    )
    tracked = subprocess.run(
        ["git", "init", "-q", str(root)], check=False, capture_output=True
    )
    assert tracked.returncode == 0
    expected = support_matrix_module.render_table(matrix)
    (root / ".github/support-matrix.md").write_text(expected)
    copy = root / "README.md"
    copy.write_text(
        f"intro\n<!-- support-matrix:begin -->\n{expected}<!-- support-matrix:end -->\n"
    )
    subprocess.run(
        ["git", "-C", str(root), "add", "README.md", ".github/support-matrix.md"],
        check=True,
    )
    copy.write_text(copy.read_text().replace("| 3.1 |", "| stale |"))

    findings = support_matrix_module.check_consistency(matrix, root)

    stale = [item for item in findings if item.rule == "C6"]
    assert len(stale) == 1 and stale[0].source == "README.md"
    assert "support_matrix.py render --write" in stale[0].hint
    assert stale[0].expected == expected
    assert stale[0].actual != expected and "| stale |" in stale[0].actual
    compose = [item for item in findings if item.rule == "C7"]
    assert len(compose) == 1


def test_consistency_reports_stale_fragment_values_and_regeneration_hint(
    support_matrix_module, valid_matrix_text, tmp_path
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(valid_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    root = _consistency_repo(
        tmp_path,
        '[project]\nrequires-python = ">=3.11"\nclassifiers = ["Programming Language :: Python :: 3.11"]\ndependencies = ["apache-airflow>=2.11.0,<4"]\n',
    )
    expected = support_matrix_module.render_table(matrix)
    actual = expected.replace("| 3.1 |", "| stale |")
    (root / ".github/support-matrix.md").write_text(actual)

    finding = next(
        item
        for item in support_matrix_module.check_consistency(matrix, root)
        if item.source == ".github/support-matrix.md"
    )

    assert finding.rule == "C6"
    assert finding.expected == expected
    assert finding.actual == actual
    assert "support_matrix.py render --write" in finding.hint


def test_consistency_reports_markdown_enumeration_and_read_errors(
    support_matrix_module, valid_matrix_text, tmp_path, monkeypatch
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(valid_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    root = _consistency_repo(
        tmp_path,
        '[project]\nrequires-python = ">=3.11"\nclassifiers = ["Programming Language :: Python :: 3.11"]\ndependencies = ["apache-airflow>=2.11.0,<4"]\n',
    )
    expected = support_matrix_module.render_table(matrix)
    (root / ".github/support-matrix.md").write_text(expected)
    readme = root / "README.md"
    readme.write_text(
        f"<!-- support-matrix:begin -->\n{expected}<!-- support-matrix:end -->"
    )
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(
        ["git", "-C", str(root), "add", "README.md", ".github/support-matrix.md"],
        check=True,
    )
    original_run = support_matrix_module.subprocess.run

    def fail_ls_files(command, *args, **kwargs):
        if "ls-files" in command:
            raise FileNotFoundError("git unavailable")
        return original_run(command, *args, **kwargs)

    monkeypatch.setattr(support_matrix_module.subprocess, "run", fail_ls_files)
    enumeration = support_matrix_module.check_consistency(matrix, root)
    failure = next(item for item in enumeration if item.source == "git ls-files")
    assert failure.rule == "C6" and "git unavailable" in failure.actual
    assert failure.expected == "tracked Markdown files enumerated"

    monkeypatch.setattr(support_matrix_module.subprocess, "run", original_run)
    readme.unlink()
    unreadable = support_matrix_module.check_consistency(matrix, root)
    read_failure = next(item for item in unreadable if item.source == "README.md")
    assert read_failure.rule == "C6" and read_failure.actual.startswith("<unreadable:")


def test_consistency_reports_present_but_wrong_compose_default(
    support_matrix_module, valid_matrix_text, tmp_path
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(valid_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    root = _consistency_repo(
        tmp_path,
        '[project]\nrequires-python = ">=3.11"\nclassifiers = ["Programming Language :: Python :: 3.11"]\ndependencies = ["apache-airflow>=2.11.0,<4"]\n',
        "services:\n  spark:\n    image: ${SPARK_IMAGE:-apache/spark:wrong}\n",
    )

    finding = next(
        item
        for item in support_matrix_module.check_consistency(matrix, root)
        if item.rule == "C7"
    )

    assert finding.expected == matrix.services["spark"].image
    assert finding.actual == "apache/spark:wrong"


def test_consistency_reports_invalid_requires_python(
    support_matrix_module, valid_matrix_text, tmp_path
):
    matrix_path = tmp_path / "matrix.toml"
    matrix_path.write_text(valid_matrix_text)
    matrix = support_matrix_module.load(matrix_path)
    root = _consistency_repo(
        tmp_path,
        '[project]\nrequires-python = ">=not-a-version"\nclassifiers = ["Programming Language :: Python :: 3.11"]\ndependencies = ["apache-airflow>=2.11.0,<4"]\n',
    )

    findings = support_matrix_module.check_consistency(matrix, root)

    assert any(
        item.source == "pyproject.toml"
        and item.field == "requires-python"
        and item.rule == "C5"
        for item in findings
    )


def test_check_command_prints_each_disagreement_and_returns_one(
    support_matrix_module, tmp_path
):
    root = _frozen_repo(tmp_path, support_matrix_module)
    script = root / ".github" / "scripts" / "support_matrix.py"
    script.parent.mkdir(parents=True)
    script.write_bytes(SCRIPT_PATH.read_bytes())

    result = subprocess.run(
        [sys.executable, str(script), "check"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stdout + result.stderr
    printed = [line.split(" ", 1)[0] for line in result.stdout.splitlines()]
    assert printed == ["C1", "C3", "C3", "C4"], result.stdout
    assert "C7" not in result.stdout


def test_check_command_on_the_repository_exits_cleanly_with_well_formed_findings(
    support_matrix_module, checked_in_matrix
):
    root = SCRIPT_PATH.parents[2]

    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "check"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    findings = support_matrix_module.check_consistency(checked_in_matrix, root)

    # Which rules fire depends on what the repository declares at the time; the
    # frozen copies above pin the specific findings. This only checks the wiring.
    assert result.returncode in (0, 1), result.stdout + result.stderr
    assert result.stderr == ""
    assert (result.returncode == 1) == bool(findings)
    for item in findings:
        assert re.fullmatch(r"C[1-7]", item.rule)
        assert item.source and item.field and item.hint
        assert f"{item.rule} {item.source}: {item.field}:" in result.stdout


def test_render_write_and_check_cli_and_stale_detection(tmp_path):
    scratch_github = tmp_path / ".github"
    scratch_scripts = scratch_github / "scripts"
    scratch_scripts.mkdir(parents=True)
    scratch_script = scratch_scripts / "support_matrix.py"
    scratch_script.write_bytes(SCRIPT_PATH.read_bytes())
    matrix_path = scratch_github / "support-matrix.toml"
    fragment_path = scratch_github / "support-matrix.md"
    matrix_path.write_bytes(FROZEN_MATRIX_PATH.read_bytes())

    write_result = subprocess.run(
        [
            sys.executable,
            str(scratch_script),
            "render",
            "--matrix",
            str(matrix_path),
            "--write",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert write_result.returncode == 0, write_result.stderr

    check_command = [
        sys.executable,
        str(scratch_script),
        "render",
        "--matrix",
        str(matrix_path),
        "--check",
    ]
    current_result = subprocess.run(
        check_command, check=False, capture_output=True, text=True
    )
    assert current_result.returncode == 0, current_result.stderr

    matrix_path.write_text(
        matrix_path.read_text().replace('release = "3.3.2"', 'release = "3.3.3"')
    )
    stale_result = subprocess.run(
        check_command, check=False, capture_output=True, text=True
    )
    assert stale_result.returncode == 1
    assert (
        "python .github/scripts/support_matrix.py render --write" in stale_result.stderr
    )
