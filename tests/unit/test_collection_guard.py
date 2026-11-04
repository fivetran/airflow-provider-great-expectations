from __future__ import annotations

import configparser
import importlib.util
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType

pytest_plugins = ["pytester"]

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).parents[2]
CONFTEST = REPO_ROOT / "tests" / "conftest.py"
PYTEST_INI = REPO_ROOT / "pytest.ini"

UNMARKED = """
    def test_untiered():
        pass
"""
UNIT = """
    import pytest

    @pytest.mark.unit
    def test_tiered_unit():
        pass
"""
INTEGRATION = """
    import pytest

    @pytest.mark.integration
    def test_tiered_integration():
        pass
"""


def _load_conftest() -> ModuleType:
    spec = importlib.util.spec_from_file_location("guard_conftest_under_test", CONFTEST)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TIER_MARKERS = sorted(_load_conftest().TIER_MARKERS)


def _write(root: Path, relpath: str, source: str) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source))


@pytest.fixture
def project(pytester: pytest.Pytester) -> pytest.Pytester:
    """A project that holds the repository's pytest.ini and tests/conftest.py as is."""
    (pytester.path / "pytest.ini").write_text(PYTEST_INI.read_text())
    (pytester.mkdir("tests") / "conftest.py").write_text(CONFTEST.read_text())
    return pytester


def _run(project: pytest.Pytester, *args: str) -> tuple[pytest.ExitCode, str]:
    result = project.runpytest_inprocess(*args)
    return pytest.ExitCode(result.ret), f"{result.stdout.str()}\n{result.stderr.str()}"


def test_unmarked_test_fails_under_unit_selection(project: pytest.Pytester) -> None:
    _write(project.path, "tests/unit/test_tiered.py", UNIT)
    _write(project.path, "tests/unit/test_untiered.py", UNMARKED)

    ret, output = _run(project, "-m", "unit", "tests")

    assert ret != pytest.ExitCode.OK
    assert "tests/unit/test_untiered.py::test_untiered" in output
    assert "test_tiered_unit" not in output
    assert "passed" not in output


@pytest.mark.parametrize(
    "selection",
    [
        pytest.param(["-m", "unit"], id="marker-expression"),
        pytest.param(["-k", "test_tiered_unit"], id="keyword-expression"),
        pytest.param(["-m", "unit", "--collect-only", "-q"], id="collect-only"),
    ],
)
def test_unmarked_test_fails_when_its_directory_is_not_selected(
    project: pytest.Pytester, selection: list[str]
) -> None:
    _write(project.path, "tests/unit/test_tiered.py", UNIT)
    _write(project.path, "tests/integration/test_untiered.py", UNMARKED)

    ret, output = _run(project, *selection, "tests")

    assert ret != pytest.ExitCode.OK
    assert "tests/integration/test_untiered.py::test_untiered" in output


def test_test_with_only_a_parametrize_marker_fails(project: pytest.Pytester) -> None:
    _write(project.path, "tests/unit/test_tiered.py", UNIT)
    _write(
        project.path,
        "tests/unit/test_untiered.py",
        """
        import pytest

        @pytest.mark.parametrize("n", [1, 2])
        def test_parametrized(n):
            pass

        @pytest.mark.skipif(False, reason="not a tier marker")
        def test_skipif():
            pass
        """,
    )

    ret, output = _run(project, "-m", "unit", "tests")

    assert ret != pytest.ExitCode.OK
    for nodeid in ("test_parametrized[1]", "test_parametrized[2]", "test_skipif"):
        assert f"tests/unit/test_untiered.py::{nodeid}" in output
    assert "test_tiered_unit" not in output


def test_guard_runs_before_other_collection_hooks_drop_items(
    project: pytest.Pytester,
) -> None:
    _write(project.path, "tests/unit/test_tiered.py", UNIT)
    _write(project.path, "tests/integration/test_untiered.py", UNMARKED)
    # A conftest that is loaded later than the root one runs its hooks first,
    # unless the guard asks to go first.
    _write(
        project.path,
        "tests/unit/conftest.py",
        """
        def pytest_collection_modifyitems(items):
            items[:] = [item for item in items if item.get_closest_marker("unit")]
        """,
    )

    ret, output = _run(project, "tests")

    assert ret != pytest.ExitCode.OK
    assert "tests/integration/test_untiered.py::test_untiered" in output


def test_unit_test_passes(project: pytest.Pytester) -> None:
    _write(project.path, "tests/unit/test_tiered.py", UNIT)
    _write(project.path, "tests/integration/test_tiered_integration.py", INTEGRATION)

    ret, output = _run(project, "-m", "unit", "tests")

    assert ret == pytest.ExitCode.OK, output
    assert "1 passed" in output


@pytest.mark.parametrize("marker", TIER_MARKERS)
def test_every_tier_marker_is_accepted(project: pytest.Pytester, marker: str) -> None:
    _write(
        project.path,
        "tests/integration/test_tiered.py",
        f"""
        import pytest

        pytestmark = pytest.mark.{marker}

        def test_tiered():
            pass
        """,
    )

    ret, output = _run(project, "-m", marker, "tests")

    assert ret == pytest.ExitCode.OK, output
    assert "1 passed" in output


def test_tier_markers_are_exactly_the_registered_markers() -> None:
    ini = configparser.ConfigParser()
    ini.read(PYTEST_INI)
    registered = [
        line.split(":")[0].strip()
        for line in ini["pytest"]["markers"].splitlines()
        if line.strip()
    ]

    assert sorted(registered) == TIER_MARKERS
