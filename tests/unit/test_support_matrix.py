from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

MATRIX_PATH = Path(__file__).parents[2] / ".github" / "support-matrix.toml"
SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "support_matrix.py"


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


def test_loads_checked_in_matrix(support_matrix_module):
    matrix = support_matrix_module.load(MATRIX_PATH)

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
