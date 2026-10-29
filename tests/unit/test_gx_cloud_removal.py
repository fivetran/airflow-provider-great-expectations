import importlib
import json
from typing import Any
from unittest.mock import Mock

import pandas as pd
import pytest
from airflow.exceptions import AirflowException
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)

pytestmark = pytest.mark.unit


class TestGXCloudRemoval:
    def test_hook_module_is_removed(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("great_expectations_provider.hooks.gx_cloud")

    def test_provider_info_has_no_cloud_entries(self) -> None:
        from great_expectations_provider.get_provider_info import get_provider_info

        provider_info = get_provider_info()
        assert "hooks" not in provider_info
        assert "connection-types" not in provider_info
        assert "name" in provider_info
        assert "description" in provider_info
        assert "gx_cloud" not in json.dumps(provider_info).lower()

    @pytest.mark.parametrize(
        ("operator_type", "argument"),
        [
            ("batch", "conn_id"),
            ("batch", "context_type"),
            ("dataframe", "conn_id"),
            ("dataframe", "context_type"),
            ("checkpoint", "conn_id"),
        ],
    )
    def test_removed_argument_is_rejected(
        self, operator_type: str, argument: str
    ) -> None:
        common: dict[str, Any] = {"task_id": "removed_argument_guard"}
        if operator_type == "batch":
            common["configure_batch_definition"] = lambda context: None
            common["configure_expectations"] = lambda context: None
            operator_class: Any = GXValidateBatchOperator
        elif operator_type == "dataframe":
            common["configure_dataframe"] = lambda: pd.DataFrame({"value": [1, 2, 3]})
            common["configure_expectations"] = lambda context: None
            operator_class = GXValidateDataFrameOperator
        else:
            common["configure_checkpoint"] = lambda context: None
            operator_class = GXValidateCheckpointOperator

        with pytest.raises((TypeError, AirflowException)) as error:
            operator_class(**common, **{argument: "unsupported"})
        assert "Invalid arguments were passed" in str(error.value)
        assert argument in str(error.value)

    def test_checkpoint_rejects_cloud_context_type(self) -> None:
        with pytest.raises(ValueError) as error:
            GXValidateCheckpointOperator(
                task_id="unsupported_checkpoint_context",
                configure_checkpoint=Mock(),
                context_type="cloud",  # type: ignore[arg-type]
            )
        assert "cloud" in str(error.value)
        assert "ephemeral" in str(error.value)
        assert "file" in str(error.value)

    @pytest.mark.parametrize("operator_type", ["batch", "dataframe", "checkpoint"])
    def test_operator_ignores_leftover_cloud_environment(
        self, monkeypatch: pytest.MonkeyPatch, operator_type: str
    ) -> None:
        monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "fake-token")
        monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", "fake-org")
        monkeypatch.setenv("GX_CLOUD_WORKSPACE_ID", "fake-workspace")
        column = "value"
        dataframe = pd.DataFrame({column: [1, 2, 3]})
        expectation = ExpectColumnValuesToBeInSet(column=column, value_set=[1, 2, 3])

        def configure_batch_definition(context):
            return (
                context.data_sources.add_pandas(name="removal guard source")
                .add_dataframe_asset("removal guard asset")
                .add_batch_definition_whole_dataframe("removal guard batch")
            )

        def configure_expectations(context):
            if operator_type == "checkpoint":
                batch_definition = configure_batch_definition(context)
                suite = context.suites.add(
                    ExpectationSuite(
                        name="removal guard suite", expectations=[expectation]
                    )
                )
                validation = context.validation_definitions.add(
                    ValidationDefinition(
                        name="removal guard validation",
                        data=batch_definition,
                        suite=suite,
                    )
                )
                return context.checkpoints.add(
                    Checkpoint(
                        name="removal guard checkpoint",
                        validation_definitions=[validation],
                    )
                )
            return expectation

        if operator_type == "batch":
            operator: Any = GXValidateBatchOperator(
                task_id="leftover_environment_batch",
                configure_batch_definition=configure_batch_definition,
                configure_expectations=configure_expectations,
                batch_parameters={"dataframe": dataframe},
            )
        elif operator_type == "dataframe":
            operator = GXValidateDataFrameOperator(
                task_id="leftover_environment_dataframe",
                configure_dataframe=lambda: dataframe,
                configure_expectations=configure_expectations,
            )
        else:
            operator = GXValidateCheckpointOperator(
                task_id="leftover_environment_checkpoint",
                configure_checkpoint=configure_expectations,
                context_type="ephemeral",
                batch_parameters={"dataframe": dataframe},
            )

        ti = Mock()
        operator.execute(context={"ti": ti})

        result = ti.xcom_push.call_args.kwargs["value"]
        assert result["success"] is True
