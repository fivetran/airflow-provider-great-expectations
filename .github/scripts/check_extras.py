"""Check that every provider extra resolves to its drivers on every lane.

    check_extras.py

For each lane the support matrix derives and each extra under
``[project.optional-dependencies]`` other than the test and lint extras, this script
asks ``uv pip compile`` to resolve ``apache-airflow==<release>`` together with the
provider and that extra, under the lane's constraints file (minus the pins of the
developer tools, exactly as the lane installer filters them). Nothing is installed.
A cell fails when:

* the extra has no entry in the support matrix's ``[extras]`` map (reported first,
  by name, before anything is resolved);
* uv cannot resolve it;
* uv warns that a package does not have an extra the provider asked for, such as an
  extra that Great Expectations (GX) renamed or removed (the message names both the
  provider extra and the GX extra);
* the resolution lacks any driver that the matrix lists for the extra.

The result is a table of lanes by extras with the reasons for each failure, written
to stdout and to the job summary. Exit status: 0 when every cell passes, 1 when any
cell fails, 2 for invalid input (an invalid matrix or project file, or no uv).

This script uses only the standard library: the extras job installs nothing but uv.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from types import ModuleType

    from support_matrix import Lane, SupportMatrix

PLATFORM = "x86_64-manylinux_2_28"
# One resolution of one extra takes seconds; a hung one must not eat the job's budget.
COMPILE_TIMEOUT = 600
# The most of uv's explanation that a table row carries.
MAX_EXPLANATION = 600

_PINNED_PACKAGE = re.compile(
    r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?\s*(?:==|@)"
)
_UNKNOWN_EXTRA = re.compile(
    r"warning: The package `([^`]+)` does not have an extra named `([^`]+)`"
)
# The arrow that uv prints before the first line of its explanation of a conflict.
_RESOLVER_MARK = "\u2570\u2500\u25b6"


class ExtrasCheckError(Exception):
    """Input that can't be checked at all, with the exit status to report."""

    def __init__(self, message: str, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


# --------------------------------------------------------------------------
# The sibling scripts, loaded by file path
# --------------------------------------------------------------------------


def _sibling(stem: str) -> ModuleType:
    """Load a script from this directory by its file path, once."""
    name = f"{stem}_for_check_extras"
    module = sys.modules.get(name)
    if module is None:
        path = Path(__file__).with_name(f"{stem}.py")
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load {stem} from {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


def installer() -> ModuleType:
    """The lane installer, whose constraints download and filter this check reuses."""
    return _sibling("lane_install")


def load_matrix(path: Path) -> SupportMatrix:
    """Load the support matrix with the matrix script's own loader."""
    module = _sibling("support_matrix")
    try:
        matrix: SupportMatrix = module.load(path)
    except module.MatrixError as exc:
        raise ExtrasCheckError(f"{path}: {exc}") from exc
    return matrix


def derive_lanes(matrix: SupportMatrix) -> tuple[Lane, ...]:
    lanes: tuple[Lane, ...] = _sibling("support_matrix").derive_lanes(matrix)
    return lanes


# --------------------------------------------------------------------------
# The extras the provider defines
# --------------------------------------------------------------------------


def _project(pyproject: Path) -> dict[str, Any]:
    import tomllib

    try:
        with pyproject.open("rb") as handle:
            project = tomllib.load(handle).get("project")
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ExtrasCheckError(
            f"{pyproject}: cannot read project metadata: {exc}"
        ) from exc
    if not isinstance(project, dict):
        raise ExtrasCheckError(f"{pyproject}: no [project] table")
    return project


def project_name(pyproject: Path) -> str:
    name = _project(pyproject).get("name")
    if not isinstance(name, str) or not name:
        raise ExtrasCheckError(f"{pyproject}: [project] has no name")
    return name


def provider_extras(pyproject: Path) -> tuple[str, ...]:
    """Return the provider's extras, other than the test and lint extras, sorted."""
    skipped = {installer().normalize_name(name) for name in installer().EXEMPT_EXTRAS}
    optional = _project(pyproject).get("optional-dependencies", {})
    return tuple(
        sorted(
            (
                name
                for name in optional
                if installer().normalize_name(name) not in skipped
            ),
            key=installer().normalize_name,
        )
    )


def unmapped_extras(extras: Iterable[str], mapped: Iterable[str]) -> tuple[str, ...]:
    """Return the extras that have no entry in the matrix's `[extras]` map, by name."""
    known = {installer().normalize_name(name) for name in mapped}
    return tuple(
        sorted(
            (name for name in extras if installer().normalize_name(name) not in known),
            key=installer().normalize_name,
        )
    )


# --------------------------------------------------------------------------
# Resolving one extra
# --------------------------------------------------------------------------


def requirements_text(project: str, extra: str, release: str, root: Path) -> str:
    """Return what uv resolves: the lane's Airflow and the provider with one extra."""
    return (
        f"apache-airflow=={release}\n{project}[{extra}] @ {root.resolve().as_uri()}\n"
    )


def compile_command(
    uv: str, requirements: Path, constraints: Path, python: str
) -> list[str]:
    """Compose the `uv pip compile` command for one lane and one extra. Nothing is run."""
    return [
        uv,
        "pip",
        "compile",
        str(requirements),
        "--constraint",
        str(constraints),
        "--python-version",
        python,
        "--python-platform",
        PLATFORM,
        "--no-header",
        "--no-annotate",
    ]


def run_command(
    command: Sequence[str], *, timeout: float | None = None, cwd: Path | None = None
) -> CommandResult:
    """Run a command and capture its output. A command that can't run is a failure."""
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired:
        return CommandResult(124, "", f"error: timed out after {timeout:g} seconds")
    except OSError as exc:
        return CommandResult(127, "", f"error: cannot run {command[0]}: {exc}")
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def download_constraints(url: str, airflow: str, python: str) -> str:
    """Download a lane's constraints file; a 404 is an explicit error, never a fallback."""
    text: str = installer().fetch_constraints(url, airflow, python)
    return text


def _collapse(text: str) -> str:
    return " ".join(text.split())


def _derivations(stderr: str) -> list[str]:
    """Return the steps of uv's explanation of a failed resolution, one line each.

    uv prints the explanation after an arrow marker (`_RESOLVER_MARK`), as paragraphs
    separated by blank lines. Each paragraph that derives a conclusion ("we can
    conclude") is a step.
    """
    lines = stderr.splitlines()
    for index, line in enumerate(lines):
        if _RESOLVER_MARK in line:
            paragraphs: list[list[str]] = [[line.split(_RESOLVER_MARK, 1)[1]]]
            for following in lines[index + 1 :]:
                if following.strip():
                    paragraphs[-1].append(following)
                elif paragraphs[-1]:
                    paragraphs.append([])
            steps = [
                _collapse(" ".join(paragraph)) for paragraph in paragraphs if paragraph
            ]
            return [step for step in steps if "we can conclude" in step] or steps[:1]
    return []


def _first_sentence(text: str) -> str:
    return re.split(r"(?<=\.)\s", text, maxsplit=1)[0]


def _explanation(stderr: str) -> str | None:
    """Return the first and the last step of uv's explanation, or None if it has none.

    The first step is the first conflict uv found; the last is the one that settled it.
    They are the same when uv needed only one.
    """
    steps = _derivations(stderr)
    if not steps:
        return None
    first, last = _first_sentence(steps[0]), _first_sentence(steps[-1])
    return first if first == last else f"{first} Last step: {last}"


def _shortened(text: str) -> str:
    if len(text) <= MAX_EXPLANATION:
        return text
    return text[: MAX_EXPLANATION - 4].rstrip() + " ..."


def _resolved_packages(stdout: str) -> set[str]:
    names: set[str] = set()
    for line in stdout.splitlines():
        if line.lstrip().startswith("#"):
            continue
        match = _PINNED_PACKAGE.match(line)
        if match is not None:
            names.add(installer().normalize_name(match.group(1)))
    return names


def classify(
    extra: str, drivers: Sequence[str], result: CommandResult
) -> tuple[str, ...]:
    """Return why a compile of one extra fails, or nothing when it passes."""
    reasons: list[str] = []
    seen: set[tuple[str, str]] = set()
    for package, missing in _UNKNOWN_EXTRA.findall(
        result.stderr + "\n" + result.stdout
    ):
        if (package, missing) in seen:
            continue
        seen.add((package, missing))
        name = installer().normalize_name(package.split("==", 1)[0])
        if name == "great-expectations":
            reasons.append(
                f"provider extra `{extra}` requests GX extra `{missing}`, which "
                f"{package} does not have"
            )
        else:
            reasons.append(
                f"provider extra `{extra}` leads to a request for extra `{missing}` "
                f"of {package}, which does not have it"
            )
    if result.returncode != 0:
        explanation = _explanation(result.stderr)
        if explanation is not None:
            reasons.append(
                "uv could not resolve the requirements: " + _shortened(explanation)
            )
        else:
            detail = _collapse(result.stderr) or "no error output"
            reasons.append(
                f"uv failed (exit {result.returncode}): " + _shortened(detail)
            )
        return tuple(reasons)
    resolved = _resolved_packages(result.stdout)
    missing_drivers = [
        driver
        for driver in drivers
        if installer().normalize_name(driver) not in resolved
    ]
    if missing_drivers:
        noun = "driver" if len(missing_drivers) == 1 else "drivers"
        reasons.append(f"resolved without its {noun} {', '.join(missing_drivers)}")
    return tuple(reasons)


# --------------------------------------------------------------------------
# The report
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Cell:
    lane: str
    extra: str
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.reasons


def lane_label(lane: Lane) -> str:
    return f"Airflow {lane.airflow}, Python {lane.python}"


def render_report(
    *,
    lanes: Sequence[Lane],
    extras: Sequence[str],
    cells: Mapping[tuple[str, str], Cell],
    preamble: Sequence[str],
) -> str:
    """Render the pass/fail table and the reasons for each failure, as Markdown."""
    lines = ["### Extras check", "", *preamble, ""]
    lines.append("| Lane | " + " | ".join(extras) + " |")
    lines.append("| --- |" + " --- |" * len(extras))
    failures: dict[tuple[str, tuple[str, ...]], list[str]] = {}
    for lane in lanes:
        row = []
        for extra in extras:
            cell = cells[(lane.id, extra)]
            row.append("pass" if cell.passed else "FAIL")
            if not cell.passed:
                failures.setdefault((lane.id, cell.reasons), []).append(extra)
        lines.append(f"| {lane_label(lane)} | " + " | ".join(row) + " |")
    total = len(lanes) * len(extras)
    failed = sum(len(names) for names in failures.values())
    lines.append("")
    if failures:
        lines.append(f"{failed} of {total} resolutions fail.")
        lines.append("")
        by_id = {lane.id: lane for lane in lanes}
        for (lane_id, reasons), names in failures.items():
            which = (
                f"all {len(extras)} extras"
                if len(names) == len(extras) > 1
                else ", ".join(f"`{name}`" for name in names)
            )
            lines.append(
                f"- {lane_label(by_id[lane_id])}, {which}: {'; '.join(reasons)}"
            )
    else:
        lines.append(f"All {total} resolutions pass.")
    return "\n".join(lines) + "\n"


def _append_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with Path(path).open("a", encoding="utf-8") as handle:
            handle.write(f"{text}\n")
    except OSError as exc:
        print(f"check_extras: cannot write the job summary: {exc}", file=sys.stderr)


def _uv_version(uv: str) -> str:
    result = run_command([uv, "--version"], timeout=60)
    fields = result.stdout.split()
    return fields[1] if result.returncode == 0 and len(fields) > 1 else "unknown"


# --------------------------------------------------------------------------
# The check
# --------------------------------------------------------------------------


def run_check(root: Path) -> int:
    """Resolve every extra on every lane. Returns the exit status."""
    matrix = load_matrix(root / ".github" / "support-matrix.toml")
    pyproject = root / "pyproject.toml"
    project = project_name(pyproject)
    extras = provider_extras(pyproject)
    try:
        exempt = installer().exempt_names(pyproject)
    except installer().LaneInstallError as exc:
        raise ExtrasCheckError(str(exc)) from exc

    unmapped = unmapped_extras(extras, matrix.extras)
    if unmapped:
        names = ", ".join(f"`{name}`" for name in unmapped)
        message = (
            "### Extras check failed\n\n"
            f"The support matrix has no `[extras]` entry for {names}. List the "
            "drivers that each extra must bring in `.github/support-matrix.toml`."
        )
        print(message)
        _append_summary(message)
        return 1

    uv = shutil.which("uv")
    if uv is None:
        raise ExtrasCheckError("uv was not found on PATH; install it first")
    drivers = {
        installer().normalize_name(name): value for name, value in matrix.extras.items()
    }
    lanes = derive_lanes(matrix)
    preamble = [
        f"uv {_uv_version(uv)} (the matrix pins {matrix.uv_version}), "
        f"host Python {sys.version.split()[0]}, target platform {PLATFORM}.",
        "Each cell resolves `apache-airflow==<release>` and the provider with one "
        "extra, under the lane's constraints without the exempt tool pins "
        f"({', '.join(sorted(exempt))}), for the lane's Python. Nothing is installed.",
    ]

    cells: dict[tuple[str, str], Cell] = {}
    with tempfile.TemporaryDirectory(prefix="check-extras-") as scratch_name:
        scratch = Path(scratch_name)
        for lane in lanes:
            label = lane_label(lane)
            print(f"{label}: constraints {lane.constraints_url}", flush=True)
            try:
                text = download_constraints(
                    lane.constraints_url, lane.airflow, lane.python
                )
            except installer().LaneInstallError as exc:
                for extra in extras:
                    cells[(lane.id, extra)] = Cell(lane.id, extra, (str(exc),))
                continue
            filtered, removed = installer().filter_constraints(text, exempt)
            if removed:
                print(
                    "  removed from the constraints: " + ", ".join(removed), flush=True
                )
            constraints = scratch / f"constraints-{lane.airflow}-py{lane.python}.txt"
            constraints.write_text(filtered, encoding="utf-8")
            for extra in extras:
                requirements = scratch / f"{lane.id}-{extra}.in"
                requirements.write_text(
                    requirements_text(project, extra, lane.airflow, root),
                    encoding="utf-8",
                )
                result = run_command(
                    compile_command(uv, requirements, constraints, lane.python),
                    timeout=COMPILE_TIMEOUT,
                    cwd=scratch,
                )
                cells[(lane.id, extra)] = Cell(
                    lane.id,
                    extra,
                    classify(extra, drivers[installer().normalize_name(extra)], result),
                )

    report = render_report(lanes=lanes, extras=extras, cells=cells, preamble=preamble)
    print()
    print(report, end="")
    _append_summary(report)
    return 0 if all(cell.passed for cell in cells.values()) else 1


def main(argv: Sequence[str] | None = None, *, repo_root: Path | None = None) -> int:
    """Run the extras check. See the module docstring for the exit statuses."""
    argparse.ArgumentParser(
        prog="check_extras.py", description=(__doc__ or "").split("\n\n")[0]
    ).parse_args(argv)
    root = repo_root or Path(__file__).resolve().parents[2]
    try:
        return run_check(root)
    except ExtrasCheckError as exc:
        print(f"check_extras: {exc}", file=sys.stderr)
        _append_summary(f"### Extras check failed\n\n{exc}\n")
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
