from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from packaging.version import Version

pytestmark = pytest.mark.unit
ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / ".github/scripts/pypi_upstream.py"
FIXTURES = Path(__file__).parent / "fixtures/ci"


@pytest.fixture
def upstream():
    spec = importlib.util.spec_from_file_location("pypi_upstream_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def data(upstream):
    projects = {
        name: upstream._project_from_json(
            json.loads((FIXTURES / f"{name}.json").read_text())
        )
        for name in ("airflow", "gx", "reference")
    }
    assert projects["gx"].latest_requires_dist == ()
    return projects


def airflow_scenario(project):
    return replace(
        project,
        releases=project.releases
        + (
            project.releases[0].__class__(Version("3.3.3"), None, False),
            project.releases[0].__class__(Version("3.3.4"), None, True),
            project.releases[0].__class__(Version("3.4.0b1"), None, False),
            project.releases[0].__class__(Version("3.4.0"), None, False),
        ),
    )


def test_reference_drift_reports_both_floors_and_reference_version(upstream, data):
    matrix = upstream._matrix()
    findings = upstream.reference_drift(
        matrix, ROOT / "pyproject.toml", data["reference"]
    )
    assert len(findings) == 2
    assert all(
        item.rule == "D1" and str(data["reference"].latest_version) in item.message
        for item in findings
    )
    assert any("2.1" in item.message and "2.11.0" in item.message for item in findings)
    assert any(">3.9" in item.message and ">=3.11" in item.message for item in findings)


def test_reference_identity_uses_info_version_not_newer_prerelease_or_yanked(
    upstream, data, tmp_path
):
    raw = json.loads((FIXTURES / "reference.json").read_text())
    raw["releases"]["9.0.0b1"] = [{"requires_python": ">=3.15", "yanked": False}]
    raw["releases"]["10.0.0"] = [{"requires_python": ">=3.15", "yanked": True}]
    raw["releases"]["2.4.0"] = [{"requires_python": ">=3.11", "yanked": False}]
    reference = upstream._project_from_json(raw)
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nrequires-python = ">=3.11"\ndependencies = ["apache-airflow>=2.11.0,<4"]\n'
    )

    assert reference.latest_version == Version("2.4.0")
    assert upstream.reference_drift(upstream._matrix(), pyproject, reference) == []


@pytest.mark.parametrize("file_metadata", [None, "missing"])
def test_reference_python_floor_uses_info_when_latest_file_floor_is_absent(
    upstream, tmp_path, file_metadata
):
    raw = json.loads((FIXTURES / "reference.json").read_text())
    raw["releases"]["2.4.0"] = [
        {"yanked": False}
        if file_metadata == "missing"
        else {"requires_python": file_metadata, "yanked": False}
    ]
    reference = upstream._project_from_json(raw)
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nrequires-python = ">=3.11"\ndependencies = ["apache-airflow>=2.11.0,<4"]\n'
    )

    assert upstream.reference_drift(upstream._matrix(), pyproject, reference) == []


def test_matrix_freshness_flags_newer_finals_but_ignores_yanked_and_prerelease(
    upstream, data
):
    findings = upstream.matrix_freshness(
        upstream._matrix(), ROOT / "pyproject.toml", airflow_scenario(data["airflow"])
    )
    assert any("3.3.3" in item.message for item in findings)
    assert any("3.4.0" in item.message for item in findings)
    assert all("3.4.0b1" not in item.message for item in findings)
    assert all("3.3.4" not in item.message for item in findings)


def test_next_python_gate_requires_both_classifiers(upstream, data):
    open_gate, reasons = upstream.next_python_gate(
        upstream._matrix(), data["airflow"], data["gx"]
    )
    assert not open_gate
    assert any("great-expectations" in item for item in reasons)
    classifier = "Programming Language :: Python :: 3.15"
    gx = replace(
        data["gx"], latest_classifiers=data["gx"].latest_classifiers + (classifier,)
    )
    airflow = replace(
        data["airflow"],
        latest_classifiers=data["airflow"].latest_classifiers + (classifier,),
    )
    assert upstream.next_python_gate(upstream._matrix(), airflow, gx) == (True, ())


def test_prerelease_target_selects_new_airflow_prerelease_and_stable_gx(
    upstream, data, monkeypatch
):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(
        upstream.urllib.request, "urlopen", lambda *args, **kwargs: Response()
    )
    target = upstream.prerelease_targets(
        upstream._matrix(), data["airflow"], data["gx"]
    )
    assert target.airflow == "3.4.0rc1"
    assert target.airflow_is_prerelease
    assert target.gx is None


def test_cli_emits_target_and_closed_gate_outputs(
    upstream, data, monkeypatch, tmp_path
):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(
        upstream.urllib.request, "urlopen", lambda *args, **kwargs: Response()
    )
    monkeypatch.setattr(
        upstream,
        "fetch",
        lambda project: {
            "apache-airflow": data["airflow"],
            "great-expectations": data["gx"],
        }[project],
    )
    output = tmp_path / "github-output"
    assert upstream.main(["prerelease", "--github-output", str(output)]) == 0
    contents = output.read_text()
    assert "airflow=3.4.0rc1" in contents
    assert "airflow_is_prerelease=true" in contents
    assert "gx=" in contents
    output.write_text("")
    assert upstream.main(["next-python-gate", "--github-output", str(output)]) == 0
    contents = output.read_text()
    assert "next_python_gate=false" in contents
    assert "apache-airflow" in contents and "great-expectations" in contents


def test_drift_cli_returns_findings_and_fetch_failure_exit_two(
    upstream, data, monkeypatch, capsys
):
    projects = {
        data["reference"].name: data["reference"],
        "apache-airflow": data["airflow"],
        "great-expectations": data["gx"],
    }
    monkeypatch.setattr(upstream, "fetch", lambda project: projects[project])
    assert upstream.main(["drift"]) == 1
    output = capsys.readouterr().out
    assert "D1:" in output and "2.4.0" in output
    assert ">3.9" in output and ">=3.11" in output

    def fail(project):
        raise upstream.UpstreamUnavailable(
            project, f"https://pypi.org/pypi/{project}/json", "offline"
        )

    monkeypatch.setattr(upstream, "fetch", fail)
    assert upstream.main(["drift"]) == 2
    error = capsys.readouterr().err
    assert "apache-airflow-providers-common-sql" in error
    assert "https://pypi.org/pypi/apache-airflow-providers-common-sql/json" in error


def test_fetch_failure_names_project_url(upstream, monkeypatch):
    def fail(url, timeout):
        raise OSError("offline")

    monkeypatch.setattr(upstream.urllib.request, "urlopen", fail)
    with pytest.raises(upstream.UpstreamUnavailable) as error:
        upstream.fetch("apache-airflow")
    assert error.value.project == "apache-airflow"
    assert "https://pypi.org/pypi/apache-airflow/json" == error.value.url
