"""Read upstream package metadata and compare it with the supported matrix."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version

if TYPE_CHECKING:
    from support_matrix import SupportMatrix


class UpstreamUnavailable(Exception):
    """An upstream project could not be read."""

    def __init__(self, project: str, url: str, reason: str) -> None:
        self.project = project
        self.url = url
        self.reason = reason
        super().__init__(f"{project}: unable to fetch {url}: {reason}")


@dataclass(frozen=True)
class Release:
    version: Version
    requires_python: str | None
    yanked: bool


@dataclass(frozen=True)
class Project:
    name: str
    releases: tuple[Release, ...]
    latest_version: Version
    latest_classifiers: tuple[str, ...]
    latest_requires_dist: tuple[str, ...]


@dataclass(frozen=True)
class Finding:
    rule: str
    message: str


@dataclass(frozen=True)
class PrereleaseTarget:
    airflow: str
    airflow_is_prerelease: bool
    constraints_url: str | None
    gx: str | None


def _project_from_json(data: dict) -> Project:
    info = data["info"]
    releases: list[Release] = []
    for raw_version, files in data.get("releases", {}).items():
        try:
            version = Version(raw_version)
        except Exception:
            continue
        if not files:
            continue
        usable = [file for file in files if not file.get("yanked", False)]
        # Keep the most permissive non-yanked file metadata for the release.
        requires_python = next(
            (
                file.get("requires_python")
                for file in usable
                if file.get("requires_python")
            ),
            None,
        )
        releases.append(Release(version, requires_python, not bool(usable)))
    latest_version = Version(info["version"])
    classifiers = tuple(info.get("classifiers") or ())
    requires_dist = tuple(info.get("requires_dist") or ())
    # The project-level info record is authoritative for its version's metadata.
    latest_index = next(
        (
            index
            for index, item in enumerate(releases)
            if item.version == latest_version
        ),
        None,
    )
    if latest_index is None:
        releases.append(Release(latest_version, info.get("requires_python"), False))
    else:
        latest_release = releases[latest_index]
        releases[latest_index] = Release(
            latest_version, info.get("requires_python"), latest_release.yanked
        )
    return Project(
        info["name"],
        tuple(sorted(releases, key=lambda item: item.version)),
        latest_version,
        classifiers,
        requires_dist,
    )


def fetch(project: str) -> Project:
    url = f"https://pypi.org/pypi/{project}/json"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise UpstreamUnavailable(project, url, str(exc)) from exc
    return _project_from_json(data)


def _load_support_matrix(path: Path):
    # Lazy import keeps Python 3.10 collection working (tomllib starts in 3.11).
    import importlib.util

    module_path = Path(__file__).with_name("support_matrix.py")
    spec = importlib.util.spec_from_file_location(
        "support_matrix_for_upstream", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load support matrix module at {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.load(path)


def _matrix():
    return _load_support_matrix(Path(__file__).parents[1] / "support-matrix.toml")


def _lower_bound(text: str | None) -> Version | None:
    if not text:
        return None
    try:
        specifiers = SpecifierSet(text)
    except Exception:
        return None
    bounds = [
        Version(item.version) for item in specifiers if item.operator in (">", ">=")
    ]
    return max(bounds) if bounds else None


def _display_floor(text: str | None) -> str:
    if not text:
        return "<none>"
    matches = re.findall(r"(>=|>)([^,\s]+)", text)
    if not matches:
        return "<none>"
    return (
        max(matches, key=lambda item: Version(item[1]))[0]
        + max(matches, key=lambda item: Version(item[1]))[1]
    )


def _airflow_requirement(requirements: tuple[str, ...]) -> Requirement | None:
    for text in requirements:
        try:
            requirement = Requirement(text)
        except Exception:
            continue
        if requirement.name.lower().replace("_", "-") == "apache-airflow":
            return requirement
    return None


def reference_drift(
    m: SupportMatrix, pyproject: Path, reference: Project
) -> list[Finding]:
    import tomllib

    with pyproject.open("rb") as handle:
        project = tomllib.load(handle)["project"]
    current_python = _lower_bound(project.get("requires-python"))
    reference_python = _lower_bound(
        next(
            (
                r.requires_python
                for r in reference.releases
                if r.version == reference.latest_version
            ),
            None,
        )
    )
    findings: list[Finding] = []
    ref_requirement = _airflow_requirement(reference.latest_requires_dist)
    reference_airflow = (
        _lower_bound(str(ref_requirement.specifier)) if ref_requirement else None
    )
    current_requirement = _airflow_requirement(tuple(project.get("dependencies", ())))
    current_airflow = (
        _lower_bound(str(current_requirement.specifier))
        if current_requirement
        else None
    )
    for label, actual, expected, actual_text, expected_text in (
        (
            "apache-airflow",
            current_airflow,
            reference_airflow,
            str(current_requirement.specifier) if current_requirement else None,
            str(ref_requirement.specifier) if ref_requirement else None,
        ),
        (
            "requires-python",
            current_python,
            reference_python,
            project.get("requires-python"),
            next(
                (
                    r.requires_python
                    for r in reference.releases
                    if r.version == reference.latest_version
                ),
                None,
            ),
        ),
    ):
        if actual != expected:
            findings.append(
                Finding(
                    "D1",
                    f"{label} floor differs from reference {reference.name} {reference.latest_version}: provider {_display_floor(actual_text)}, reference {_display_floor(expected_text)}; update the provider floor to match the reference.",
                )
            )
    return findings


def matrix_freshness(
    m: SupportMatrix, pyproject: Path, airflow: Project
) -> list[Finding]:
    import tomllib

    with pyproject.open("rb") as handle:
        project = tomllib.load(handle)["project"]
    requirement = _airflow_requirement(tuple(project.get("dependencies", ())))
    cap = requirement.specifier if requirement else SpecifierSet()
    finals = [
        release.version
        for release in airflow.releases
        if not release.version.is_prerelease
        and not release.version.is_devrelease
        and not release.yanked
    ]
    findings: list[Finding] = []
    for line in m.airflow:
        baseline = Version(line.release)
        newer = [
            version
            for version in finals
            if version.release[:2] == baseline.release[:2] and version > baseline
        ]
        if newer:
            latest = max(newer)
            findings.append(
                Finding(
                    "D2",
                    f"Airflow line {line.line} matrix release {baseline} is behind final PyPI release {latest}; update the matrix release.",
                )
            )
    latest_line_minor = max(
        (tuple(int(part) for part in line.line.split(".")) for line in m.airflow),
        default=(0, 0),
    )
    capped = [version for version in finals if version in cap]
    if capped:
        latest_capped = max(capped)
        if latest_capped.release[:2] > latest_line_minor:
            findings.append(
                Finding(
                    "D2",
                    f"Airflow matrix newest line is {latest_line_minor[0]}.{latest_line_minor[1]}, but latest final release under provider cap {latest_capped} is on a newer minor; add the line to the matrix.",
                )
            )
    return findings


def next_python_gate(
    m: SupportMatrix, airflow: Project, gx: Project
) -> tuple[bool, tuple[str, ...]]:
    classifier = f"Programming Language :: Python :: {m.python_next}"
    missing = tuple(
        name
        for project in (airflow, gx)
        if classifier not in project.latest_classifiers
        for name in (project.name,)
    )
    return not missing, tuple(
        f"{name} does not declare {classifier}" for name in missing
    )


def prerelease_targets(
    m: SupportMatrix, airflow: Project, gx: Project
) -> PrereleaseTarget:
    baseline = Version(m.airflow[-1].release)
    newer_airflow = [
        release.version
        for release in airflow.releases
        if not release.yanked and release.version > baseline
    ]
    newest_airflow = max(newer_airflow, default=baseline)
    if newest_airflow.is_prerelease:
        airflow_version = newest_airflow
        is_pre = True
    else:
        airflow_version = baseline
        is_pre = False
    constraints_url = m.constraints_url.format(
        airflow=str(airflow_version), python=m.python_supported[0]
    )
    constraints: str | None = constraints_url
    try:
        with urllib.request.urlopen(constraints_url, timeout=30):
            pass
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            constraints = None
        else:
            raise UpstreamUnavailable(
                "apache-airflow constraints", constraints_url, str(exc)
            ) from exc
    except OSError as exc:
        raise UpstreamUnavailable(
            "apache-airflow constraints", constraints_url, str(exc)
        ) from exc
    gx_final = max(
        (
            release.version
            for release in gx.releases
            if not release.yanked
            and not release.version.is_prerelease
            and not release.version.is_devrelease
        ),
        default=Version("0"),
    )
    gx_pre = [
        release.version
        for release in gx.releases
        if not release.yanked
        and release.version.is_prerelease
        and release.version > gx_final
    ]
    return PrereleaseTarget(
        str(airflow_version), is_pre, constraints, str(max(gx_pre)) if gx_pre else None
    )


def _append_output(path: Path | None, values: dict[str, str]) -> None:
    rendered = "".join(f"{key}={value}\n" for key, value in values.items())
    if path is None:
        sys.stdout.write(rendered)
    else:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(rendered)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("drift", "prerelease", "next-python-gate"))
    parser.add_argument(
        "--matrix", type=Path, default=Path(__file__).parents[1] / "support-matrix.toml"
    )
    parser.add_argument(
        "--pyproject", type=Path, default=Path(__file__).parents[2] / "pyproject.toml"
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        default=Path(os.environ["GITHUB_OUTPUT"])
        if os.getenv("GITHUB_OUTPUT")
        else None,
    )
    args = parser.parse_args(argv)
    try:
        matrix = _load_support_matrix(args.matrix)
        if args.command == "drift":
            reference = fetch(matrix.reference_provider)
            airflow = fetch("apache-airflow")
            gx = fetch("great-expectations")
            findings = reference_drift(matrix, args.pyproject, reference)
            findings += matrix_freshness(matrix, args.pyproject, airflow)
            gate_open, _ = next_python_gate(matrix, airflow, gx)
            if gate_open:
                findings.append(
                    Finding(
                        "D3",
                        f"Airflow and {gx.name} declare Python {matrix.python_next}, but the support matrix does not; add it to the supported Python set and matrix lanes.",
                    )
                )
            for finding in findings:
                print(f"{finding.rule}: {finding.message}")
            return 1 if findings else 0
        airflow = fetch("apache-airflow")
        gx = fetch("great-expectations")
        if args.command == "prerelease":
            target = prerelease_targets(matrix, airflow, gx)
            values = {
                "airflow": target.airflow,
                "airflow_is_prerelease": str(target.airflow_is_prerelease).lower(),
                "constraints_url": target.constraints_url or "",
                "gx": target.gx or "",
            }
            _append_output(args.github_output, values)
        else:
            opened, missing = next_python_gate(matrix, airflow, gx)
            values = {
                "next_python_gate": str(opened).lower(),
                "missing": ",".join(missing),
            }
            _append_output(args.github_output, values)
            for item in missing:
                print(item)
        return 0
    except UpstreamUnavailable as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
