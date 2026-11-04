"""Load and validate the checked-in support matrix."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from collections.abc import Mapping


class MatrixError(Exception):
    """A support-matrix validation failure associated with a field or entry."""

    def __init__(self, entry: str, message: str) -> None:
        self.entry = entry
        self.message = message
        super().__init__(f"{entry}: {message}")


@dataclass(frozen=True)
class AirflowLine:
    line: str
    release: str
    tier: str
    python: tuple[str, ...]


@dataclass(frozen=True)
class Tier:
    name: str
    always_true: tuple[str, ...]
    always_false: tuple[str, ...]


@dataclass(frozen=True)
class Service:
    image: str
    lanes: tuple[str, ...]
    pyspark: str | None


@dataclass(frozen=True)
class SupportMatrix:
    reference_provider: str
    python_supported: tuple[str, ...]
    python_next: str
    airflow: tuple[AirflowLine, ...]
    tiers: Mapping[str, Tier]
    constraints_url: str
    uv_version: str
    services: Mapping[str, Service]
    extras: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class Lane:
    id: str
    airflow: str
    line: str
    python: str
    tier: str
    constraints_url: str
    roles: tuple[str, ...]


@dataclass(frozen=True)
class TypecheckTarget:
    tier: str
    lane: Lane
    mypy_args: tuple[str, ...]


def _fail(entry: str, message: str) -> NoReturn:
    raise MatrixError(entry, message)


def _table(value: object, entry: str) -> dict:
    if not isinstance(value, dict):
        _fail(entry, "must be a table")
    return value


def _string(value: object, entry: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(entry, "must be a non-empty string")
    return value


def _string_list(value: object, entry: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        _fail(entry, "must be a list of non-empty strings")
    return tuple(value)


def _python_key(version: str, entry: str) -> tuple[int, ...]:
    if not re.fullmatch(r"\d+\.\d+", version):
        _fail(entry, f"invalid Python version {version!r}; expected major.minor")
    return tuple(int(part) for part in version.split("."))


def load(path: Path) -> SupportMatrix:
    """Parse a schema-v1 matrix and reject invalid entries with their field names."""
    import tomllib

    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        _fail(str(path), f"cannot read valid TOML: {exc}")

    raw = _table(raw, "matrix")
    schema_version = raw.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        _fail("schema_version", "must be 1")

    policy = _table(raw.get("policy"), "policy")
    reference_provider = _string(
        policy.get("reference_provider"), "policy.reference_provider"
    )
    python_table = _table(raw.get("python"), "python")
    python_supported = _string_list(python_table.get("supported"), "python.supported")
    python_next = _string(python_table.get("next"), "python.next")

    supported_keys = tuple(_python_key(v, "python.supported") for v in python_supported)
    if not supported_keys or tuple(sorted(set(supported_keys))) != supported_keys:
        _fail("python.supported", "versions must be listed once in ascending order")
    next_key = _python_key(python_next, "python.next")
    if next_key <= max(supported_keys):
        _fail("python.next", "must be greater than every supported Python version")

    raw_airflow = raw.get("airflow")
    if not isinstance(raw_airflow, list) or not raw_airflow:
        _fail("airflow", "must contain at least one Airflow line")
    airflow: list[AirflowLine] = []
    for index, item in enumerate(raw_airflow):
        prefix = f"airflow[{index}]"
        item = _table(item, prefix)
        line = _string(item.get("line"), f"{prefix}.line")
        release = _string(item.get("release"), f"{prefix}.release")
        tier = _string(item.get("tier"), f"{prefix}.tier")
        pythons = _string_list(item.get("python"), f"{prefix}.python")
        if not re.fullmatch(r"\d+\.\d+", line):
            _fail(f"{prefix}.line", "must be major.minor")
        if not re.fullmatch(r"\d+\.\d+\.\d+", release):
            _fail(f"{prefix}.release", "must be a final major.minor.patch release")
        if ".".join(release.split(".")[:2]) != line:
            _fail(f"{prefix}.release", f"release must match line {line}")
        airflow.append(AirflowLine(line, release, tier, pythons))
    airflow_keys = [
        tuple(int(part) for part in line.line.split(".")) for line in airflow
    ]
    if airflow_keys != sorted(set(airflow_keys)):
        _fail("airflow", "lines must be listed once in ascending order")

    tiers_raw = _table(raw.get("tiers"), "tiers")
    tiers: dict[str, Tier] = {}
    for name, value in tiers_raw.items():
        entry = f"tiers.{name!r}"
        value = _table(value, entry)
        true_flags = _string_list(value.get("always_true"), f"{entry}.always_true")
        false_flags = _string_list(value.get("always_false"), f"{entry}.always_false")
        if set(true_flags) & set(false_flags):
            _fail(entry, "always_true and always_false flags must be disjoint")
        tiers[name] = Tier(name, true_flags, false_flags)
    used_tiers = {line.tier for line in airflow}
    if used_tiers - tiers.keys():
        missing_index = next(
            i for i, line in enumerate(airflow) if line.tier not in tiers
        )
        _fail(f"airflow[{missing_index}].tier", "tier is not defined")
    if tiers.keys() - used_tiers:
        name = sorted(tiers.keys() - used_tiers)[0]
        _fail(f"tiers.{name!r}", "tier is not used by an Airflow line")
    flag_sets = [
        set(tier.always_true) | set(tier.always_false) for tier in tiers.values()
    ]
    if flag_sets and any(flags != flag_sets[0] for flags in flag_sets[1:]):
        _fail("tiers", "every tier must define the same compatibility flags")

    supported_set = set(python_supported)
    for index, airflow_line in enumerate(airflow):
        if airflow_line.tier not in tiers:
            _fail(f"airflow[{index}].tier", f"unknown tier {airflow_line.tier!r}")
        if not set(airflow_line.python) <= supported_set:
            _fail(f"airflow[{index}].python", "must be a subset of python.supported")
        if not python_supported[0] in airflow_line.python:
            _fail(
                f"airflow[{index}].python", "must include the lowest supported Python"
            )
    if airflow[-1].python != python_supported:
        newest_index = len(airflow) - 1
        _fail(
            f"airflow[{newest_index}].python",
            "newest Airflow line must support every supported Python",
        )

    install = _table(raw.get("install"), "install")
    constraints_url = _string(install.get("constraints_url"), "install.constraints_url")
    try:
        fields = {name for _, name, _, _ in Formatter().parse(constraints_url) if name}
    except ValueError as exc:
        _fail("install.constraints_url", f"invalid template: {exc}")
    if not {"airflow", "python"} <= fields:
        _fail("install.constraints_url", "template must contain {airflow} and {python}")
    try:
        constraints_url.format(airflow="2.11.2", python="3.11")
    except (AttributeError, IndexError, KeyError, TypeError, ValueError) as exc:
        _fail("install.constraints_url", f"invalid template: {exc}")
    uv_version = _string(install.get("uv"), "install.uv")
    if not re.fullmatch(r"\d+\.\d+\.\d+", uv_version):
        _fail("install.uv", "must be an exact major.minor.patch version")

    services_raw = _table(raw.get("services"), "services")
    services: dict[str, Service] = {}
    image_pattern = re.compile(
        r"^[A-Za-z0-9._/-]+:[A-Za-z0-9._-]+@sha256:[a-f0-9]{64}$"
    )
    for name in ("postgres", "spark"):
        entry = f"services.{name}"
        service_raw = _table(services_raw.get(name), entry)
        image = _string(service_raw.get("image"), f"{entry}.image")
        lanes = _string_list(service_raw.get("lanes"), f"{entry}.lanes")
        if not image_pattern.fullmatch(image):
            _fail(
                f"{entry}.image", "must include a tag and a 64-character sha256 digest"
            )
        if not lanes or set(lanes) - {"floor", "newest"}:
            _fail(f"{entry}.lanes", "must contain only floor and/or newest")
        pyspark = None
        if name == "spark":
            pyspark = _string(service_raw.get("pyspark"), f"{entry}.pyspark")
            tag = image.split("@", 1)[0].rsplit(":", 1)[1]
            if not tag.startswith(pyspark):
                _fail(
                    f"{entry}.image",
                    "Spark image tag must start with the PySpark version",
                )
        elif "pyspark" in service_raw:
            _fail(f"{entry}.pyspark", "is only valid for the Spark service")
        services[name] = Service(image, lanes, pyspark)

    extras_raw = _table(raw.get("extras"), "extras")
    extras = {
        name: _string_list(value, f"extras.{name}")
        for name, value in extras_raw.items()
    }
    if any(not distributions for distributions in extras.values()):
        name = next(name for name, distributions in extras.items() if not distributions)
        _fail(f"extras.{name}", "must list at least one distribution")

    return SupportMatrix(
        reference_provider=reference_provider,
        python_supported=python_supported,
        python_next=python_next,
        airflow=tuple(airflow),
        tiers=tiers,
        constraints_url=constraints_url,
        uv_version=uv_version,
        services=services,
        extras=extras,
    )


def derive_lanes(matrix: SupportMatrix) -> tuple[Lane, ...]:
    """Derive one lane per Airflow line and one per remaining supported Python."""
    lowest_python = matrix.python_supported[0]
    newest = matrix.airflow[-1]
    lanes: list[Lane] = []
    for index, line in enumerate(matrix.airflow):
        roles = ["line"]
        if index == 0:
            roles.insert(0, "floor")
        if index == len(matrix.airflow) - 1:
            roles.insert(0, "newest")
        lanes.append(_make_lane(matrix, line, lowest_python, tuple(roles)))
    for python in matrix.python_supported[1:]:
        lanes.append(_make_lane(matrix, newest, python, ("python-range",)))
    return tuple(lanes)


def _make_lane(
    matrix: SupportMatrix, line: AirflowLine, python: str, roles: tuple[str, ...]
) -> Lane:
    return Lane(
        id=f"airflow-{line.release}-py{python}",
        airflow=line.release,
        line=line.line,
        python=python,
        tier=line.tier,
        constraints_url=matrix.constraints_url.format(
            airflow=line.release, python=python
        ),
        roles=roles,
    )


def floor_lane(matrix: SupportMatrix) -> Lane:
    """Return the lane for the oldest Airflow line on the lowest Python."""
    return next(lane for lane in derive_lanes(matrix) if "floor" in lane.roles)


def newest_lane(matrix: SupportMatrix) -> Lane:
    """Return the newest Airflow lane on the lowest Python."""
    return next(lane for lane in derive_lanes(matrix) if "newest" in lane.roles)


def typecheck_targets(matrix: SupportMatrix) -> tuple[TypecheckTarget, ...]:
    """Return the highest Airflow line in each tier on the lowest Python."""
    lowest_python = matrix.python_supported[0]
    tiers = dict.fromkeys(line.tier for line in matrix.airflow)
    targets: list[TypecheckTarget] = []
    for tier in tiers:
        line = next(line for line in reversed(matrix.airflow) if line.tier == tier)
        flags = matrix.tiers[tier]
        args = tuple(f"--always-true={flag}" for flag in flags.always_true) + tuple(
            f"--always-false={flag}" for flag in flags.always_false
        )
        targets.append(
            TypecheckTarget(
                tier=tier,
                lane=_make_lane(matrix, line, lowest_python, ("line",)),
                mypy_args=args,
            )
        )
    return tuple(targets)


def render_table(matrix: SupportMatrix) -> str:
    """Render the checked-in compatibility table from the support matrix."""
    lines = [
        "<!-- Generated from `.github/support-matrix.toml`; run `python .github/scripts/support_matrix.py render --write`. -->",
        "| Airflow | Tested release | Python tested in CI | Python supported by this Airflow line |",
        "| --- | --- | --- | --- |",
    ]
    lanes = derive_lanes(matrix)
    for airflow in reversed(matrix.airflow):
        tested_python = [lane.python for lane in lanes if lane.line == airflow.line]
        supported = airflow.python
        lines.append(
            f"| {airflow.line} | {airflow.release} | {', '.join(tested_python)} | "
            f"{supported[0]}–{supported[-1]} |"
        )
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class Disagreement:
    rule: str
    source: str
    field: str
    expected: str
    actual: str
    hint: str


def check_consistency(matrix: SupportMatrix, repo_root: Path) -> list[Disagreement]:
    """Compare package metadata, checked-in fragments and compose with the matrix."""
    from packaging.requirements import Requirement
    from packaging.specifiers import InvalidSpecifier, SpecifierSet
    from packaging.version import Version

    findings: list[Disagreement] = []

    def add(
        rule: str, source: str, field: str, expected: object, actual: object, hint: str
    ) -> None:
        findings.append(
            Disagreement(rule, source, field, str(expected), str(actual), hint)
        )

    pyproject_path = repo_root / "pyproject.toml"
    try:
        import tomllib

        project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]
    except (OSError, KeyError, ValueError) as exc:
        add(
            "C5",
            "pyproject.toml",
            "project metadata",
            "readable project table",
            exc,
            "Fix pyproject.toml metadata",
        )
        project = {}

    airflow_req: Requirement | None = None
    try:
        airflow_req = next(
            req
            for req in (Requirement(value) for value in project.get("dependencies", []))
            if req.name.lower().replace("_", "-") == "apache-airflow"
        )
    except (StopIteration, ValueError) as exc:
        add(
            "C5",
            "pyproject.toml",
            "apache-airflow requirement",
            "one valid direct requirement",
            exc,
            "Declare apache-airflow in project.dependencies",
        )

    if airflow_req is not None:
        specifiers = list(airflow_req.specifier)
        lower = next((s.version for s in specifiers if s.operator == ">="), None)
        expected_floor = matrix.airflow[0].line
        actual_floor = ".".join(lower.split(".")[:2]) if lower else "missing >= bound"
        if actual_floor != expected_floor:
            add(
                "C1",
                "pyproject.toml",
                "apache-airflow floor",
                expected_floor,
                actual_floor,
                "Align the >= floor with the first Airflow line",
            )
        excluded = [s for s in specifiers if s.operator == "!="]
        if excluded:
            add(
                "C2",
                "pyproject.toml",
                "apache-airflow exclusions",
                "no != clauses",
                ", ".join(map(str, excluded)),
                "Remove the exclusion or record and encode the allowed defect exception",
            )
        outside = [
            line.release
            for line in matrix.airflow
            if Version(line.release) not in airflow_req.specifier
        ]
        if outside:
            add(
                "C2",
                "pyproject.toml",
                "apache-airflow tested releases",
                ", ".join(matrix_line.release for matrix_line in matrix.airflow),
                f"outside: {', '.join(outside)} ({airflow_req.specifier})",
                "Make the declared requirement admit every tested release",
            )

    python_spec = project.get("requires-python")
    if isinstance(python_spec, str):
        try:
            parsed = SpecifierSet(python_spec)
        except InvalidSpecifier as exc:
            add(
                "C5",
                "pyproject.toml",
                "requires-python",
                "valid PEP 440 specifier",
                str(exc),
                "Correct project.requires-python in pyproject.toml",
            )
            parsed = None
        if parsed is not None:
            lower_python = next(
                (s.version for s in parsed if s.operator in (">", ">=")), None
            )
            expected_python = matrix.python_supported[0]
            actual_python = (
                ".".join(lower_python.split(".")[:2])
                if lower_python
                else "missing lower bound"
            )
            if actual_python != expected_python:
                add(
                    "C3",
                    "pyproject.toml",
                    "requires-python floor",
                    expected_python,
                    actual_python,
                    "Align requires-python with the lowest supported Python",
                )
            upper = [s for s in parsed if s.operator in ("<", "<=", "~=")]
            if upper:
                add(
                    "C3",
                    "pyproject.toml",
                    "requires-python upper bound",
                    "no <, <= or ~= bounds",
                    ", ".join(map(str, upper)),
                    "Remove the requires-python upper bound",
                )
    else:
        add(
            "C5",
            "pyproject.toml",
            "requires-python",
            "valid string specifier",
            python_spec,
            "Set project.requires-python",
        )

    expected_classifiers = {
        f"Programming Language :: Python :: {v}" for v in matrix.python_supported
    }
    actual_classifiers = {
        v
        for v in project.get("classifiers", [])
        if re.fullmatch(r"Programming Language :: Python :: 3\.\d+", v)
    }
    if actual_classifiers != expected_classifiers:
        add(
            "C4",
            "pyproject.toml",
            "Python classifiers",
            ", ".join(sorted(expected_classifiers)),
            ", ".join(sorted(actual_classifiers)),
            "Align Python classifiers with python.supported",
        )

    expected_table = render_table(matrix)
    fragment = repo_root / ".github/support-matrix.md"
    try:
        actual_fragment = fragment.read_text(encoding="utf-8")
    except FileNotFoundError:
        actual_fragment = "<missing file>"
    except OSError as exc:
        actual_fragment = f"<unreadable: {exc}>"
    if actual_fragment != expected_table:
        add(
            "C6",
            ".github/support-matrix.md",
            "rendered table",
            expected_table,
            actual_fragment,
            "Regenerate with `python .github/scripts/support_matrix.py render --write`",
        )

    try:
        tracked = subprocess.run(
            ["git", "-C", str(repo_root), "ls-files", "--cached", "-z", "--", "*.md"],
            capture_output=True,
            check=True,
        ).stdout.split(b"\0")
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = (
            exc.stderr.decode(errors="replace")
            if isinstance(exc, subprocess.CalledProcessError)
            and isinstance(exc.stderr, bytes)
            else (exc.stderr or str(exc))
            if isinstance(exc, subprocess.CalledProcessError)
            else str(exc)
        )
        add(
            "C6",
            "git ls-files",
            "tracked Markdown enumeration",
            "tracked Markdown files enumerated",
            detail,
            "Run the consistency check from a readable Git worktree",
        )
        tracked = []
    begin, end = "<!-- support-matrix:begin -->", "<!-- support-matrix:end -->"
    for raw_path in tracked:
        if not raw_path:
            continue
        rel = raw_path.decode("utf-8", errors="replace")
        path = repo_root / rel
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            add(
                "C6",
                rel,
                "marked support-matrix block",
                "readable tracked Markdown file",
                f"<unreadable: {exc.strerror or exc}>",
                "Restore read access to this tracked Markdown file and rerun the check",
            )
            continue
        cursor = 0
        while (start := content.find(begin, cursor)) >= 0:
            body_start = start + len(begin)
            stop = content.find(end, body_start)
            if stop < 0:
                actual = "<missing end marker>"
                cursor = len(content)
            else:
                actual = content[body_start:stop].lstrip("\r\n")
                cursor = stop + len(end)
            if actual != expected_table:
                add(
                    "C6",
                    rel,
                    "marked support-matrix block",
                    expected_table,
                    actual,
                    "Regenerate with `python .github/scripts/support_matrix.py render --write` and update this marked copy",
                )

    compose_path = repo_root / "docker/spark/docker-compose.yml"
    try:
        compose = compose_path.read_text(encoding="utf-8")
    except OSError:
        compose = ""
    match = re.search(r"\$\{SPARK_IMAGE:-([^}]+)\}", compose)
    expected_image = matrix.services["spark"].image
    actual_image = match.group(1) if match else "<missing ${SPARK_IMAGE:-...} default>"
    if actual_image != expected_image:
        add(
            "C7",
            "docker/spark/docker-compose.yml",
            "SPARK_IMAGE default",
            expected_image,
            actual_image,
            "Set the compose default to services.spark.image",
        )
    return findings


def _lane_dict(lane: Lane) -> dict[str, object]:
    return {
        "id": lane.id,
        "airflow": lane.airflow,
        "line": lane.line,
        "python": lane.python,
        "tier": lane.tier,
        "constraints_url": lane.constraints_url,
        "roles": list(lane.roles),
    }


def _outputs(matrix: SupportMatrix) -> dict[str, str]:
    lanes = derive_lanes(matrix)
    targets = typecheck_targets(matrix)
    output: dict[str, str] = {
        "lanes": json.dumps([_lane_dict(lane) for lane in lanes]),
        "typecheck": json.dumps(
            [
                {
                    "tier": target.tier,
                    "lane": _lane_dict(target.lane),
                    "mypy_args": list(target.mypy_args),
                }
                for target in targets
            ]
        ),
    }
    for service_name in ("postgres", "spark"):
        service = matrix.services[service_name]
        selected = [
            lane for role in service.lanes for lane in lanes if role in lane.roles
        ]
        output[service_name] = json.dumps([_lane_dict(lane) for lane in selected])
        output[f"{service_name}-image"] = service.image
    spark = matrix.services["spark"]
    assert spark.pyspark is not None
    output["pyspark-version"] = spark.pyspark
    output["canary-latest"] = json.dumps({"python": matrix.python_supported[-1]})
    highest_python = matrix.python_supported[-1]
    oldest_high_python = next(
        line for line in matrix.airflow if highest_python in line.python
    )
    output["canary-lowest-direct"] = json.dumps(
        [
            {
                "airflow": "floor",
                "python": matrix.python_supported[0],
                "constraints_template": matrix.constraints_url,
            },
            {
                "airflow": oldest_high_python.release,
                "python": highest_python,
                "constraints_url": matrix.constraints_url.format(
                    airflow=oldest_high_python.release, python=highest_python
                ),
            },
        ]
    )
    output["lowest-python"] = matrix.python_supported[0]
    output["next-python"] = matrix.python_next
    output["uv-version"] = matrix.uv_version
    return output


def main(argv: list[str] | None = None) -> int:
    """Validate the matrix or emit workflow outputs from it."""
    default_path = (
        Path(__file__).resolve().parents[2] / ".github" / "support-matrix.toml"
    )
    parser = argparse.ArgumentParser(description="Validate or emit support matrix data")
    parser.add_argument("command", nargs="?", choices=("emit", "render", "check"))
    parser.add_argument(
        "--matrix", type=Path, default=default_path, help="matrix TOML file"
    )
    render_mode = parser.add_mutually_exclusive_group()
    render_mode.add_argument(
        "--write", action="store_true", help="write rendered fragment"
    )
    render_mode.add_argument(
        "--check", action="store_true", help="check rendered fragment"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="GitHub output file (defaults to GITHUB_OUTPUT or stdout)",
    )
    args = parser.parse_args(argv)
    try:
        matrix = load(args.matrix)
    except MatrixError as exc:
        print(f"{args.matrix}: {exc}", file=sys.stderr)
        return 2
    if args.command == "render":
        if not (args.write or args.check):
            parser.error("render requires --write or --check")
        fragment = Path(__file__).resolve().parents[1] / "support-matrix.md"
        rendered = render_table(matrix)
        if args.write:
            fragment.write_text(rendered, encoding="utf-8")
            return 0
        try:
            current = fragment.read_text(encoding="utf-8")
        except OSError:
            current = ""
        if current != rendered:
            print(
                "support-matrix.md is stale or missing; regenerate it with "
                "`python .github/scripts/support_matrix.py render --write`",
                file=sys.stderr,
            )
            return 1
        return 0
    if args.command == "check":
        disagreements = check_consistency(matrix, Path(__file__).resolve().parents[2])
        for item in disagreements:
            print(
                f"{item.rule} {item.source}: {item.field}: expected {item.expected}; "
                f"actual {item.actual}. Hint: {item.hint}"
            )
        return 1 if disagreements else 0
    if args.command == "emit":
        if args.write or args.check:
            parser.error("render options require the render command")
        values = _outputs(matrix)
        rendered = "".join(f"{key}={value}\n" for key, value in values.items())
        output_path = args.output or (
            Path(os.environ["GITHUB_OUTPUT"]) if os.getenv("GITHUB_OUTPUT") else None
        )
        if output_path is None:
            sys.stdout.write(rendered)
        else:
            with output_path.open("a", encoding="utf-8") as handle:
                handle.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
