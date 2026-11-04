"""Tests for the provider's single source of Airflow imports.

The expected tier is derived here from the installed ``apache-airflow``
distribution's metadata, independently of the module under test, and the
expected objects are loaded by module path for that tier. So this module has no
static Airflow import and no version branch of its own: a wrong flag or a wrong
source path in ``version_compat`` shows up as a failed comparison.
"""

from __future__ import annotations

import importlib
from importlib.metadata import version
from typing import Final

import pytest
from packaging.version import Version

from great_expectations_provider import version_compat
from great_expectations_provider.common.errors import GXValidationFailed

pytestmark = pytest.mark.unit

TIER_2_11: Final = "2.11"
TIER_3_0: Final = "3.0"
TIER_3_1_PLUS: Final = "3.1+"

# The module each name must come from, per tier.
EXPECTED_SOURCES: Final[dict[str, dict[str, str]]] = {
    "BaseOperator": {
        TIER_2_11: "airflow.models.baseoperator",
        TIER_3_0: "airflow.sdk",
        TIER_3_1_PLUS: "airflow.sdk",
    },
    "chain": {
        TIER_2_11: "airflow.models.baseoperator",
        TIER_3_0: "airflow.sdk",
        TIER_3_1_PLUS: "airflow.sdk",
    },
    "DAG": {
        TIER_2_11: "airflow.models.dag",
        TIER_3_0: "airflow.sdk",
        TIER_3_1_PLUS: "airflow.sdk",
    },
    "Context": {
        TIER_2_11: "airflow.utils.context",
        TIER_3_0: "airflow.sdk",
        TIER_3_1_PLUS: "airflow.sdk",
    },
    "BaseHook": {
        TIER_2_11: "airflow.hooks.base",
        TIER_3_0: "airflow.hooks.base",
        TIER_3_1_PLUS: "airflow.sdk",
    },
    "Connection": {
        TIER_2_11: "airflow.models.connection",
        TIER_3_0: "airflow.models.connection",
        TIER_3_1_PLUS: "airflow.sdk",
    },
    "AirflowException": {
        TIER_2_11: "airflow.exceptions",
        TIER_3_0: "airflow.exceptions",
        TIER_3_1_PLUS: "airflow.exceptions",
    },
}


def _installed_airflow_base_version() -> tuple[int, int, int]:
    """The installed ``apache-airflow`` release, ignoring pre-release segments."""
    installed = Version(version("apache-airflow"))
    return installed.major, installed.minor, installed.micro


def _installed_tier() -> str:
    base_version = _installed_airflow_base_version()
    if base_version >= (3, 1, 0):
        return TIER_3_1_PLUS
    if base_version >= (3, 0, 0):
        return TIER_3_0
    return TIER_2_11


def test_flags_match_installed_airflow() -> None:
    base_version = _installed_airflow_base_version()

    assert version_compat.get_base_airflow_version_tuple() == base_version
    assert version_compat.AIRFLOW_V_3_0_PLUS is (base_version >= (3, 0, 0))
    assert version_compat.AIRFLOW_V_3_1_PLUS is (base_version >= (3, 1, 0))


@pytest.mark.parametrize("name", sorted(EXPECTED_SOURCES))
def test_name_comes_from_tier_source(name: str) -> None:
    source = EXPECTED_SOURCES[name][_installed_tier()]
    expected = getattr(importlib.import_module(source), name)

    assert (
        getattr(version_compat, name) is expected
    ), f"{name} should be {source}.{name} on Airflow {version('apache-airflow')}"


def test_validation_failed_is_every_airflow_exception() -> None:
    exception_sources = ["airflow.exceptions"]
    if _installed_tier() == TIER_3_1_PLUS:
        exception_sources.append("airflow.sdk.exceptions")

    for source in exception_sources:
        airflow_exception = importlib.import_module(source).AirflowException
        assert issubclass(GXValidationFailed, airflow_exception), source
        assert isinstance(GXValidationFailed(), airflow_exception), source
