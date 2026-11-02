"""Install one CI lane the same way in every job, and record what got installed.

    lane_install.py --mode MODE [--airflow VERSION|floor] [--python X.Y]
                    [--constraints-url URL] [--constraints-template T]
                    [--extras a,b] [--with REQ ...]

Modes:

* ``constrained``: ``apache-airflow==VERSION`` under that release's constraints
  file, minus the pins of the developer tools that the ``tests`` and ``lint``
  extras pin.
* ``latest`` and ``next-python``: the provider's own requirements, with no
  constraints.
* ``lowest-direct``: as ``constrained``, but the direct dependencies resolve to the
  lowest versions that ``pyproject.toml`` admits. ``--airflow floor`` uses the
  lower bound declared for ``apache-airflow``.
* ``prerelease``: as ``constrained``, but a missing constraints file is noted
  instead of fatal, because pre-releases don't always have one. uv installs a
  pre-release only where it is pinned, so the target's own exact pre-release
  requirements (``apache-airflow-core==X``, ``apache-airflow-task-sdk==Y``, ...) are
  read from its PyPI metadata and pinned explicitly next to it.
* ``tools``: only the ``lint`` extra's pinned tools and the CI scripts'
  requirements; neither Airflow nor the provider is installed.

Everything installs into the interpreter that runs this script, so activate the
lane's Python first. Exit status: 0 on success, 1 when the lane fails to install
(a missing or unreachable constraints file, or uv failing), 2 for invalid input.
A missing constraints file never falls back to an unconstrained install.

This script uses only the standard library: lane jobs run it on a bare
interpreter, before anything is installed.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from support_matrix import SupportMatrix

MODES = ("constrained", "latest", "lowest-direct", "prerelease", "next-python", "tools")
# Modes that install `apache-airflow==<release>` under that release's constraints.
AIRFLOW_MODES = ("constrained", "lowest-direct", "prerelease")
SCRIPTS_REQUIREMENTS = Path(".github/scripts/requirements.txt")
# The extras whose `==` pins are developer tools, exempt from the constraints.
EXEMPT_EXTRAS = ("tests", "lint")
REPORT_DISTRIBUTIONS = (
    "apache-airflow",
    "apache-airflow-core",
    "apache-airflow-task-sdk",
    "great-expectations",
    "SQLAlchemy",
    "pandas",
    "pyspark",
    "pytest",
    "mypy",
    "ruff",
)

PYPI_JSON_URL = "https://pypi.org/pypi/{project}/{version}/json"

_AIRFLOW_VERSION = re.compile(r"\d+\.\d+\.\d+(?:\.?(?:a|b|rc|dev|post)\d+)*")
_EXTRAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*(?:,[A-Za-z0-9][A-Za-z0-9._-]*)*")
# A normalized version; the groups capture a pre-release (a, b, rc) or a dev release.
_VERSION = re.compile(r"\d+(?:\.\d+)*(?:\.?(a|b|rc)\d+)?(?:\.?post\d+)?(\.?dev\d+)?")
_EXACT_PIN = re.compile(r"==\s*([^\s,*]+)")
_REQUIREMENT = re.compile(
    r"([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)\s*(?:\[[^\]]*\])?\s*([=<>!~;@(].*)?"
)


class LaneInstallError(Exception):
    """A lane that can't be installed, with the exit status to report."""

    def __init__(self, message: str, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class ConstraintsNotFound(LaneInstallError):
    """The constraints file for a release and Python doesn't exist (HTTP 404)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, exit_code=1)


@dataclass(frozen=True)
class Request:
    mode: str
    airflow: str | None
    python: str | None
    constraints_url: str | None
    constraints_template: str | None
    extras: tuple[str, ...]
    with_requirements: tuple[str, ...]


# --------------------------------------------------------------------------
# Requirement strings, using only the standard library
# --------------------------------------------------------------------------


def normalize_name(name: str) -> str:
    """Return the PEP 503 normalized form of a distribution name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _parse_requirement(text: str) -> tuple[str, str] | None:
    """Split a requirement into its normalized name and its version clauses."""
    match = _REQUIREMENT.fullmatch(text.strip())
    if match is None:
        return None
    clauses = (match.group(2) or "").split(";", 1)[0].strip()
    return normalize_name(match.group(1)), clauses


def _is_pinned(clauses: str) -> bool:
    return any(
        re.fullmatch(r"===?\s*[^\s*,]+", clause.strip())
        for clause in clauses.strip("() ").split(",")
    )


def _project_table(pyproject: Path) -> dict[str, Any]:
    import tomllib

    try:
        with pyproject.open("rb") as handle:
            project = tomllib.load(handle).get("project")
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise LaneInstallError(
            f"{pyproject}: cannot read project metadata: {exc}"
        ) from exc
    if not isinstance(project, dict):
        raise LaneInstallError(f"{pyproject}: no [project] table")
    return project


def pinned_requirements(pyproject: Path, extras: Iterable[str]) -> tuple[str, ...]:
    """Return the requirements pinned with `==` in the given extras, in file order."""
    optional = _project_table(pyproject).get("optional-dependencies", {})
    pins: list[str] = []
    for extra in extras:
        if extra not in optional:
            raise LaneInstallError(f"{pyproject}: no {extra!r} extra is defined")
        for text in optional[extra]:
            parsed = _parse_requirement(text)
            if parsed is not None and _is_pinned(parsed[1]):
                pins.append(text.strip())
    return tuple(dict.fromkeys(pins))


def exempt_names(pyproject: Path) -> frozenset[str]:
    """Return the names whose pins the constraints must not impose.

    These are the requirements pinned with `==` in the `tests` and `lint` extras:
    the test runner and its plugins, the type checker, and the linter.
    """
    names: set[str] = set()
    for text in pinned_requirements(pyproject, EXEMPT_EXTRAS):
        parsed = _parse_requirement(text)
        if parsed is not None:
            names.add(parsed[0])
    return frozenset(names)


def is_prerelease(version: str) -> bool:
    """Return whether a normalized version is an a, b, rc or dev release."""
    match = _VERSION.fullmatch(version)
    return match is not None and bool(match.group(1) or match.group(2))


def prerelease_pins(requires_dist: Iterable[str]) -> tuple[str, ...]:
    """Return the exact `==` pins of pre-releases in a release's requirements.

    uv installs a pre-release only where one is pinned, and a requirement's own
    transitive pins don't count, so each of these is given to uv as a top-level
    requirement. Entries for optional extras are skipped: they aren't installed
    unless the extra is. Any other marker stays on the requirement, so uv decides
    whether it applies.
    """
    pins: list[str] = []
    for text in requires_dist:
        parsed = _parse_requirement(text)
        if parsed is None or re.search(r"\bextra\b", text.partition(";")[2]):
            continue
        pin = _EXACT_PIN.fullmatch(parsed[1].strip("() "))
        if pin is not None and is_prerelease(pin.group(1)):
            pins.append(text.strip())
    return tuple(dict.fromkeys(pins))


def parse_floor(requirement: str) -> str:
    """Return the `>=` bound of a requirement, normalized to major.minor.patch."""
    parsed = _parse_requirement(requirement)
    bounds: list[tuple[int, ...]] = []
    for clause in (parsed[1] if parsed else "").strip("() ").split(","):
        match = re.fullmatch(r">=\s*(\d+(?:\.\d+){0,2})", clause.strip())
        if match:
            bounds.append(tuple(int(part) for part in match.group(1).split(".")))
    if not bounds:
        raise LaneInstallError(
            f"no usable lower bound (>= major.minor[.patch]) in the requirement "
            f"'{requirement}', so there is no floor to install"
        )
    return ".".join(str(part) for part in (*max(bounds), 0, 0)[:3])


def airflow_floor(pyproject: Path) -> str:
    """Return the floor declared for `apache-airflow` in `project.dependencies`."""
    for text in _project_table(pyproject).get("dependencies", []):
        parsed = _parse_requirement(text)
        if parsed is not None and parsed[0] == "apache-airflow":
            return parse_floor(text)
    raise LaneInstallError(
        f"{pyproject}: no apache-airflow requirement in project.dependencies"
    )


def filter_constraints(
    text: str, exempt: frozenset[str]
) -> tuple[str, tuple[str, ...]]:
    """Drop the exempt pins from a constraints file.

    Returns the filtered text and the lines that were removed. Comments, blank
    lines and lines that aren't requirements are kept.
    """
    kept: list[str] = []
    removed: list[str] = []
    for line in text.splitlines():
        content = line.split("#", 1)[0].strip()
        parsed = _parse_requirement(content) if content else None
        if parsed is not None and parsed[0] in exempt:
            removed.append(line)
        else:
            kept.append(line)
    return "".join(f"{line}\n" for line in kept), tuple(removed)


# --------------------------------------------------------------------------
# Support matrix and constraints
# --------------------------------------------------------------------------


def load_matrix(path: Path) -> SupportMatrix:
    """Load the support matrix with the matrix script's own loader."""
    name = "support_matrix_for_lane_install"
    module = sys.modules.get(name)
    if module is None:
        module_path = Path(__file__).with_name("support_matrix.py")
        spec = importlib.util.spec_from_file_location(name, module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(
                f"cannot load the support matrix module at {module_path}"
            )
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    try:
        return module.load(path)
    except module.MatrixError as exc:
        raise LaneInstallError(f"{path}: {exc}") from exc


def constraints_url_for(
    request: Request, release: str, python: str, matrix_path: Path
) -> str:
    """Return the constraints URL: the given one, or a template filled in."""
    if request.constraints_url:
        return request.constraints_url
    template = request.constraints_template or load_matrix(matrix_path).constraints_url
    try:
        return template.format(airflow=release, python=python)
    except (IndexError, KeyError, ValueError) as exc:
        raise LaneInstallError(
            f"invalid constraints template {template!r}: {exc!r}"
        ) from exc


def fetch_constraints(url: str, airflow: str, python: str) -> str:
    """Download a constraints file. A 404 is reported explicitly, never worked around."""
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise ConstraintsNotFound(
                f"no constraints file for Airflow {airflow} on Python {python} "
                f"(HTTP 404): {url}"
            ) from exc
        reason = f"HTTP {exc.code}"
    except (OSError, UnicodeDecodeError) as exc:
        reason = str(exc)
    raise LaneInstallError(
        f"cannot download the constraints file for Airflow {airflow} on Python "
        f"{python}: {url}: {reason}",
        exit_code=1,
    )


def fetch_requires_dist(project: str, version: str) -> list[str]:
    """Read a release's requirements from its PyPI metadata. A failure is never worked around."""
    url = PYPI_JSON_URL.format(project=project, version=version)
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        reason = f"HTTP {exc.code}"
    except (OSError, UnicodeDecodeError) as exc:
        reason = str(exc)
    except json.JSONDecodeError as exc:
        reason = f"invalid JSON: {exc}"
    else:
        info = data.get("info") if isinstance(data, dict) else None
        if not isinstance(info, dict):
            reason = "the response has no 'info' object"
        elif info.get("requires_dist") is None:
            return []
        elif isinstance(info["requires_dist"], list) and all(
            isinstance(item, str) for item in info["requires_dist"]
        ):
            return list(info["requires_dist"])
        else:
            reason = "'info.requires_dist' is not a list of strings"
    raise LaneInstallError(
        f"cannot read the PyPI metadata for {project} {version}: {url}: {reason}",
        exit_code=1,
    )


# --------------------------------------------------------------------------
# Arguments
# --------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lane_install.py", description=(__doc__ or "").split("\n\n")[0]
    )
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument(
        "--airflow", help="Airflow release to install, or 'floor' (lowest-direct only)"
    )
    parser.add_argument(
        "--python", help="the lane's Python, X.Y; must match the active interpreter"
    )
    parser.add_argument("--constraints-url", help="constraints file URL, used as given")
    parser.add_argument(
        "--constraints-template",
        help="constraints URL template with {airflow} and {python}",
    )
    parser.add_argument("--extras", help="comma-separated provider extras to install")
    parser.add_argument(
        "--with",
        dest="with_requirements",
        action="append",
        nargs="+",
        default=[],
        metavar="REQ",
        help="additional requirement to install (repeatable)",
    )
    return parser


def _given(value: str | None) -> str | None:
    """Treat an empty argument, such as an unset workflow output, as absent."""
    if value is None:
        return None
    return value.strip() or None


def parse_request(argv: Sequence[str] | None = None) -> Request:
    """Parse and validate the command line. Invalid input raises with exit status 2."""
    args = _parser().parse_args(argv)
    mode: str = args.mode
    airflow = _given(args.airflow)
    python = _given(args.python)
    url = _given(args.constraints_url)
    template = _given(args.constraints_template)
    extras_text = _given(args.extras)
    with_requirements = tuple(
        item.strip()
        for group in args.with_requirements
        for item in group
        if item.strip()
    )

    if mode in AIRFLOW_MODES:
        if airflow is None:
            raise LaneInstallError(f"--airflow is required for --mode {mode}")
        if airflow == "floor":
            if mode != "lowest-direct":
                raise LaneInstallError(
                    "'floor' is only valid with --mode lowest-direct"
                )
        elif not _AIRFLOW_VERSION.fullmatch(airflow):
            raise LaneInstallError(
                f"invalid --airflow {airflow!r}: expected a release such as 2.11.2"
            )
        if url is not None and template is not None:
            raise LaneInstallError(
                "give only one of --constraints-url and --constraints-template"
            )
    else:
        for option, value in (
            ("--airflow", airflow),
            ("--constraints-url", url),
            ("--constraints-template", template),
        ):
            if value is not None:
                raise LaneInstallError(f"{option} is not used by --mode {mode}")

    if mode == "tools":
        if extras_text is not None:
            raise LaneInstallError("--extras is not used by --mode tools")
        if with_requirements:
            raise LaneInstallError("--with is not used by --mode tools")
    if extras_text is not None and not _EXTRAS.fullmatch(extras_text):
        raise LaneInstallError(
            f"invalid --extras {extras_text!r}: expected names separated by commas"
        )
    if python is not None and not re.fullmatch(r"\d+\.\d+", python):
        raise LaneInstallError(f"invalid --python {python!r}: expected X.Y")
    for requirement in with_requirements:
        if requirement.startswith("-"):
            raise LaneInstallError(
                f"--with requirements must not start with '-': {requirement!r}"
            )

    return Request(
        mode=mode,
        airflow=airflow,
        python=python,
        constraints_url=url,
        constraints_template=template,
        extras=tuple(extras_text.split(",")) if extras_text else (),
        with_requirements=with_requirements,
    )


# --------------------------------------------------------------------------
# Installing and reporting
# --------------------------------------------------------------------------


def build_command(
    mode: str,
    *,
    uv: str,
    python_executable: str,
    repo_root: Path,
    airflow: str | None = None,
    constraints_file: Path | None = None,
    prerelease_pins: Sequence[str] = (),
    extras: Sequence[str] = (),
    with_requirements: Sequence[str] = (),
    tool_pins: Sequence[str] = (),
    scripts_requirements: Path | None = None,
) -> list[str]:
    """Compose the `uv pip install` command for a mode. Nothing is run."""
    command = [uv, "pip", "install", "--python", python_executable]
    if mode == "tools":
        command += tool_pins
        if scripts_requirements is not None:
            command += ["--requirement", str(scripts_requirements)]
        return command
    command += [
        "--resolution",
        "lowest-direct" if mode == "lowest-direct" else "highest",
    ]
    if constraints_file is not None:
        command += ["--constraint", str(constraints_file)]
    if airflow is not None:
        command.append(f"apache-airflow=={airflow}")
    command += prerelease_pins
    target = f"{repo_root}[{','.join(extras)}]" if extras else str(repo_root)
    command += ["--editable", target]
    command += with_requirements
    return command


def installed_versions(names: Iterable[str]) -> dict[str, str]:
    """Return the version of each named distribution that is installed."""
    importlib.invalidate_caches()
    versions: dict[str, str] = {}
    for name in names:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
    return versions


def render_report(
    *, heading: str, rows: Sequence[tuple[str, str]], notes: Sequence[str] = ()
) -> str:
    """Render the version report as Markdown, for stdout and the job summary."""
    lines = [f"### {heading}", "", "| Component | Version |", "| --- | --- |"]
    lines += [f"| {name} | {version} |" for name, version in rows]
    if notes:
        lines.append("")
        lines += [f"- {note}" for note in notes]
    return "\n".join(lines) + "\n"


def _append_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with Path(path).open("a", encoding="utf-8") as handle:
            handle.write(f"{text}\n")
    except OSError as exc:
        print(f"lane_install: cannot write the job summary: {exc}", file=sys.stderr)


def _active_python() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def _uv_version(uv: str) -> str:
    try:
        result = subprocess.run(
            [uv, "--version"], capture_output=True, text=True, check=False
        )
    except OSError:
        return "unknown"
    fields = result.stdout.split()
    return fields[1] if result.returncode == 0 and len(fields) > 1 else "unknown"


def _prepare_constraints(
    request: Request, release: str, python: str, root: Path, scratch: Path
) -> tuple[Path | None, list[str]]:
    """Download the lane's constraints and write them without the exempt pins."""
    pyproject = root / "pyproject.toml"
    url = constraints_url_for(
        request, release, python, root / ".github" / "support-matrix.toml"
    )
    print(f"Constraints: {url}", flush=True)
    exempt = exempt_names(pyproject)
    try:
        text = fetch_constraints(url, release, python)
    except ConstraintsNotFound as exc:
        if request.mode != "prerelease":
            raise
        note = f"{exc}; installed without constraints"
        print(note, flush=True)
        return None, [note]
    filtered, removed = filter_constraints(text, exempt)
    exempted = ", ".join(sorted(exempt))
    print(f"Exempt tool pins (from the tests and lint extras): {exempted}")
    if removed:
        print("Removed from the constraints: " + ", ".join(removed), flush=True)
        removal = "Removed from the constraints: " + ", ".join(
            f"`{r}`" for r in removed
        )
    else:
        print("None of the exempt pins appear in the constraints.", flush=True)
        removal = "None of the exempt tool pins appear in the constraints"
    path = scratch / f"constraints-{release}-py{python}.txt"
    path.write_text(filtered, encoding="utf-8")
    return path, [f"Constraints: {url}", removal]


def install(request: Request, root: Path) -> None:
    """Install the lane and print its version report. Failures raise."""
    active = _active_python()
    python = request.python or active
    if python != active:
        raise LaneInstallError(
            f"--python {python} does not match the active interpreter "
            f"(Python {active}); activate a Python {python} environment first"
        )
    if shutil.which("uv") is None:
        raise LaneInstallError("uv was not found on PATH; install it first")

    pyproject = root / "pyproject.toml"
    release: str | None = None
    notes: list[str] = []
    tool_pins: tuple[str, ...] = ()
    scripts_requirements: Path | None = None
    description = f"{request.mode}"
    if request.mode == "tools":
        tool_pins = pinned_requirements(pyproject, ("lint",))
        scripts_requirements = root / SCRIPTS_REQUIREMENTS
        pins = ", ".join(f"`{pin}`" for pin in tool_pins)
        notes.append(
            f"Installed: {pins}, and the requirements in `{SCRIPTS_REQUIREMENTS}`"
        )
    elif request.airflow is not None:
        is_floor = request.airflow == "floor"
        release = airflow_floor(pyproject) if is_floor else request.airflow
        description += f", Airflow {release}" + (
            " (declared floor)" if is_floor else ""
        )
    else:
        description += ", Airflow as the provider requires"
    description += f", Python {python}"

    with tempfile.TemporaryDirectory(prefix="lane-install-") as scratch:
        constraints_file: Path | None = None
        prerelease_requirements: tuple[str, ...] = ()
        if release is not None:
            constraints_file, constraint_notes = _prepare_constraints(
                request, release, python, root, Path(scratch)
            )
            notes += constraint_notes
            if request.mode == "prerelease" and is_prerelease(release):
                prerelease_requirements = prerelease_pins(
                    fetch_requires_dist("apache-airflow", release)
                )
                pinned = ", ".join(f"`{pin}`" for pin in prerelease_requirements)
                notes.append(
                    f"Pre-release requirements of apache-airflow {release}, pinned "
                    f"explicitly: {pinned or 'none'}"
                )
                print(notes[-1].replace("`", ""), flush=True)
        command = build_command(
            request.mode,
            uv="uv",
            python_executable=sys.executable,
            repo_root=root,
            airflow=release,
            constraints_file=constraints_file,
            prerelease_pins=prerelease_requirements,
            extras=request.extras,
            with_requirements=request.with_requirements,
            tool_pins=tool_pins,
            scripts_requirements=scripts_requirements,
        )
        print("+ " + shlex.join(command), flush=True)
        status = subprocess.run(command, check=False).returncode
    if status != 0:
        raise LaneInstallError(
            f"install failed: uv exited with status {status}; its error is in the "
            "log above",
            exit_code=1,
        )

    versions = installed_versions(REPORT_DISTRIBUTIONS)
    rows = [("Python", sys.version.split()[0]), ("uv", _uv_version("uv"))]
    rows += [
        (name, versions[name]) for name in REPORT_DISTRIBUTIONS if name in versions
    ]
    report = render_report(
        heading=f"Lane install: {description}", rows=rows, notes=notes
    )
    print(report, end="", flush=True)
    _append_summary(report)


def main(argv: Sequence[str] | None = None, *, repo_root: Path | None = None) -> int:
    """Install a lane. See the module docstring for the modes and exit statuses."""
    root = repo_root or Path(__file__).resolve().parents[2]
    try:
        install(parse_request(argv), root)
    except LaneInstallError as exc:
        print(f"lane_install: {exc}", file=sys.stderr)
        _append_summary(f"### Lane install failed\n\n{exc}\n")
        return exc.exit_code
    return 0


if __name__ == "__main__":
    sys.exit(main())
