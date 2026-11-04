"""The provider's single source of Airflow imports, selected by Airflow version.

Every Airflow class or function the provider uses comes from this module, so the
differences between Airflow versions live in one place. Each name is imported from
a path that, on the Airflow versions it serves, imports without a deprecation
warning and is a real class or function to type checkers.

Two flags select the import path:

- ``AIRFLOW_V_3_0_PLUS`` is true on Airflow 3.0 and later.
- ``AIRFLOW_V_3_1_PLUS`` is true on Airflow 3.1 and later.

A pre-release counts as its final version (3.4.0b1 counts as 3.4.0).

The branches serve three tiers:

- Airflow 2.11 (both flags false): ``BaseOperator`` and ``chain`` from
  ``airflow.models.baseoperator``, ``DAG`` from ``airflow.models.dag``,
  ``Context`` from ``airflow.utils.context``, ``BaseHook`` from
  ``airflow.hooks.base`` and ``Connection`` from ``airflow.models.connection``.
- Airflow 3.0 (``AIRFLOW_V_3_0_PLUS`` only): ``BaseOperator``, ``chain``, ``DAG``
  and ``Context`` from ``airflow.sdk``; ``BaseHook`` and ``Connection`` keep their
  2.11 paths, because ``airflow.sdk`` provides ``BaseHook`` only from 3.1 and
  ``BaseHook.get_connection`` returns ``airflow.models.connection.Connection``
  through 3.0.
- Airflow 3.1 and later (both flags true): all six from ``airflow.sdk``.

``AirflowException`` comes from ``airflow.exceptions`` on every tier. From
Airflow 3.1 that class is also the one ``airflow.sdk`` exposes, so
``GXValidationFailed`` is caught by an ``except`` clause on either path.

Type checkers cannot know which Airflow version will be installed, so they
evaluate the flags statically: each tier is checked with its flags passed on the
command line, for example for Airflow 3.0::

    mypy --always-true AIRFLOW_V_3_0_PLUS --always-false AIRFLOW_V_3_1_PLUS .

Use ``--always-false`` for both flags on Airflow 2.11 and ``--always-true`` for
both on Airflow 3.1 and later. Without the flags, mypy checks every branch and
reports errors from the tiers that are not installed.

Dropping support for a tier later means deleting its branch and, once no branch
uses it, its flag.
"""

from __future__ import annotations

from airflow.exceptions import AirflowException


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    """Return the installed Airflow version as ``(major, minor, micro)``.

    Pre-release and local segments are dropped, so 3.4.0b1 gives ``(3, 4, 0)``.
    """
    from airflow import __version__
    from packaging.version import Version

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_PLUS: bool = get_base_airflow_version_tuple() >= (3, 0, 0)
AIRFLOW_V_3_1_PLUS: bool = get_base_airflow_version_tuple() >= (3, 1, 0)

if AIRFLOW_V_3_0_PLUS:
    # Airflow 3.0 and later.
    from airflow.sdk import DAG, BaseOperator, Context, chain
else:
    # Airflow 2.11.
    from airflow.models.baseoperator import BaseOperator, chain
    from airflow.models.dag import DAG
    from airflow.utils.context import Context

if AIRFLOW_V_3_1_PLUS:
    # Airflow 3.1 and later.
    from airflow.sdk import BaseHook, Connection
else:
    # Airflow 2.11 and 3.0.
    from airflow.hooks.base import BaseHook
    from airflow.models.connection import Connection

__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "AIRFLOW_V_3_1_PLUS",
    "DAG",
    "AirflowException",
    "BaseHook",
    "BaseOperator",
    "Connection",
    "Context",
    "chain",
]
