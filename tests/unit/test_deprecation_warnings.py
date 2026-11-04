"""Tests that importing and running the provider emits no deprecation warnings.

A warning counts against the provider when it is attributed to a provider module:
Airflow's deprecated-import shims and its DAG-argument checks warn with a stack
level that points at the calling module, so a legacy import or an implicit DAG
schedule in a provider module is charged to that module. Warnings raised while
Airflow or another dependency imports itself are attributed to that dependency
and do not count.

Detection turns matching warnings into errors with a module-scoped filter. That
works whatever Airflow does with ``warnings.showwarning``, which Airflow 2.x
replaces and Airflow 3.x routes to logging, so a plain recording
``catch_warnings`` would miss warnings there. Airflow itself only adds filters
scoped to its own modules, which leave these in force.

This module imports no Airflow module. Airflow is first imported by the provider
under the filters.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import textwrap
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Final, Union
from unittest.mock import Mock

import pandas as pd
import pytest
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext

pytestmark = pytest.mark.unit

PACKAGE: Final = "great_expectations_provider"
# Matches the provider package and every module under it.
PROVIDER_MODULES: Final = r"^great_expectations_provider(\..*)?$"
# Every Airflow deprecation class is a subclass of one of these.
DEPRECATION_CATEGORIES: Final = (
    DeprecationWarning,
    PendingDeprecationWarning,
    FutureWarning,
)
IMPORT_TIMEOUT_SECONDS: Final = 300

# Runs in a fresh interpreter. It receives one JSON argument:
# {"package", "source_root", "pattern", "categories", "expected"}.
IMPORT_EVERY_MODULE: Final = textwrap.dedent(
    """
    import builtins
    import importlib
    import json
    import pkgutil
    import sys
    import traceback
    import warnings

    # Imported before the filters exist, so that Great Expectations' own
    # import-time warnings are not charged to the provider module that happens
    # to import it first.
    import great_expectations  # noqa: F401

    config = json.loads(sys.argv[1])
    sys.path.insert(0, config["source_root"])
    for name in config["categories"]:
        warnings.filterwarnings(
            "error", category=getattr(builtins, name), module=config["pattern"]
        )

    imported = set()
    failed = set()
    failures = []


    def import_module(name):
        try:
            importlib.import_module(name)
        except Exception as error:
            failed.add(name)
            failures.append(
                f"{name}: {type(error).__name__}: {error}\\n{traceback.format_exc()}"
            )
        else:
            imported.add(name)


    def ignore_package_import_error(name):
        # import_module has already recorded this failure.
        pass


    package = config["package"]
    import_module(package)
    if package in imported:
        for info in pkgutil.walk_packages(
            sys.modules[package].__path__,
            prefix=package + ".",
            onerror=ignore_package_import_error,
        ):
            import_module(info.name)

    for name in sorted(set(config["expected"]) - imported - failed):
        failures.append(f"{name}: was not imported")

    if failures:
        print("\\n".join(failures), file=sys.stderr)
        sys.exit(1)
    """
)

Operator = Union[
    GXValidateBatchOperator,
    GXValidateCheckpointOperator,
    GXValidateDataFrameOperator,
]

COLUMN: Final = "col_A"


def escalate_provider_deprecation_warnings() -> None:
    for category in DEPRECATION_CATEGORIES:
        warnings.filterwarnings("error", category=category, module=PROVIDER_MODULES)


def provider_source() -> tuple[Path, list[str]]:
    """The directory holding the provider package, and every module under it."""
    spec = importlib.util.find_spec(PACKAGE)
    assert spec is not None
    assert spec.submodule_search_locations is not None
    package_dir = Path(next(iter(spec.submodule_search_locations)))
    module_names = sorted(
        ".".join((PACKAGE, *path.relative_to(package_dir).with_suffix("").parts))
        for path in package_dir.rglob("*.py")
        if path.name != "__init__.py"
    )
    return package_dir.parent, module_names


def test_importing_provider_emits_no_deprecation_warning() -> None:
    source_root, module_names = provider_source()
    # The checks below are only meaningful if the child really covers these.
    assert f"{PACKAGE}.version_compat" in module_names
    assert any(name.startswith(f"{PACKAGE}.example_dags.") for name in module_names)
    config = {
        "package": PACKAGE,
        "source_root": str(source_root),
        "pattern": PROVIDER_MODULES,
        "categories": [category.__name__ for category in DEPRECATION_CATEGORIES],
        "expected": module_names,
    }

    completed = subprocess.run(
        [sys.executable, "-c", IMPORT_EVERY_MODULE, json.dumps(config)],
        capture_output=True,
        text=True,
        timeout=IMPORT_TIMEOUT_SECONDS,
        check=False,
    )

    assert completed.returncode == 0, (
        "importing a provider module emitted a deprecation warning, or failed:\n"
        f"{completed.stderr}"
    )


def three_rows() -> pd.DataFrame:
    return pd.DataFrame({COLUMN: ["a", "b", "c"]})


def passing_expectation(context: AbstractDataContext) -> ExpectColumnValuesToBeInSet:
    return ExpectColumnValuesToBeInSet(column=COLUMN, value_set=["a", "b", "c", "d"])


def build_batch_operator() -> GXValidateBatchOperator:
    def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
        return (
            context.data_sources.add_pandas(name="datasource")
            .add_dataframe_asset("asset")
            .add_batch_definition_whole_dataframe("batch definition")
        )

    return GXValidateBatchOperator(
        task_id="validate_batch",
        configure_batch_definition=configure_batch_definition,
        configure_expectations=passing_expectation,
        batch_parameters={"dataframe": three_rows()},
    )


def build_checkpoint_operator() -> GXValidateCheckpointOperator:
    def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
        batch_definition = (
            context.data_sources.add_pandas(name="datasource")
            .add_dataframe_asset("asset")
            .add_batch_definition_whole_dataframe("batch definition")
        )
        suite = context.suites.add(
            ExpectationSuite(name="suite", expectations=[passing_expectation(context)])
        )
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                name="validation definition", data=batch_definition, suite=suite
            )
        )
        return context.checkpoints.add(
            Checkpoint(
                name="checkpoint", validation_definitions=[validation_definition]
            )
        )

    return GXValidateCheckpointOperator(
        task_id="validate_checkpoint",
        configure_checkpoint=configure_checkpoint,
        batch_parameters={"dataframe": three_rows()},
    )


def build_dataframe_operator() -> GXValidateDataFrameOperator:
    return GXValidateDataFrameOperator(
        task_id="validate_dataframe",
        configure_dataframe=three_rows,
        configure_expectations=passing_expectation,
    )


@pytest.mark.parametrize(
    "build_operator",
    [
        pytest.param(build_batch_operator, id="batch"),
        pytest.param(build_checkpoint_operator, id="checkpoint"),
        pytest.param(build_dataframe_operator, id="dataframe"),
    ],
)
def test_operator_emits_no_deprecation_warning(
    build_operator: Callable[[], Operator],
) -> None:
    task_instance = Mock()
    context: Any = {"ti": task_instance}

    with warnings.catch_warnings():
        escalate_provider_deprecation_warnings()
        build_operator().execute(context=context)

    # The run reached the end and passed, so the warning filters saw a real run.
    task_instance.xcom_push.assert_called_once()
    assert task_instance.xcom_push.call_args.kwargs["value"]["success"] is True
