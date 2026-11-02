from pathlib import Path
from typing import Callable
from unittest.mock import Mock

import great_expectations as gx
import pandas as pd
import pytest
from great_expectations import expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.data_context import AbstractDataContext

from great_expectations_provider.common.errors import GXValidationFailed
from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from tests.integration.conftest import rand_name


class TestValidateBatchOperator:
    COL_NAME = "my_column"

    @pytest.mark.integration
    def test_with_ephemeral_context(self) -> None:
        task_id = f"validate_batch_ephemeral_integration_test_{rand_name()}"
        dataframe = pd.DataFrame({self.COL_NAME: ["a", "b", "c"]})
        batch_parameters = {"dataframe": dataframe}

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name=task_id)
                .add_dataframe_asset(task_id)
                .add_batch_definition_whole_dataframe(task_id)
            )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> gxe.ExpectColumnValuesToBeInSet:
            return gxe.ExpectColumnValuesToBeInSet(
                column=self.COL_NAME,
                value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
            )

        validate_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters=batch_parameters,
        )

        mock_ti = Mock()
        validate_batch.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    @pytest.mark.integration
    def test_file_system_data_source(
        self,
        load_csv_data: Callable[[Path, list[dict]], None],
        tmp_path: Path,
    ) -> None:
        task_id = f"validate_batch_file_system_integration_test_{rand_name()}"
        file_name = "data.csv"
        data_location = tmp_path / file_name
        load_csv_data(
            data_location,
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ],
        )

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_pandas_filesystem(
                    name=task_id,
                    base_directory=tmp_path,
                )
                .add_csv_asset(name=task_id)
                .add_batch_definition_path(
                    name=task_id,
                    path=file_name,
                )
            )

        def configure_expectations_suite(
            context: AbstractDataContext,
        ) -> gx.ExpectationSuite:
            return context.suites.add_or_update(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column="age",
                            min_value=0,
                            max_value=100,
                        ),
                        gxe.ExpectTableRowCountToEqual(value=2),
                    ],
                )
            )

        validate_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            configure_expectations=configure_expectations_suite,
        )

        mock_ti = Mock()
        validate_batch.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    @pytest.mark.postgres
    def test_sql_data_source(
        self,
        table_name: str,
        load_postgres_data: Callable[[list[dict]], None],
        postgres_connection_string: str,
    ) -> None:
        task_id = f"validate_batch_sql_integration_test_{rand_name()}"
        load_postgres_data(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ]
        )

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_postgres(
                    name=task_id,
                    connection_string=postgres_connection_string,
                )
                .add_table_asset(
                    name=task_id,
                    table_name=table_name,
                )
                .add_batch_definition_whole_table(task_id)
            )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> gxe.ExpectColumnValuesToBeBetween:
            return gxe.ExpectColumnValuesToBeBetween(
                column="age",
                min_value=0,
                max_value=100,
            )

        validate_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            configure_expectations=configure_expectations,
        )

        mock_ti = Mock()
        validate_batch.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    @pytest.mark.integration
    def test_validation_failure_raises_exception(self) -> None:
        """Test that validation failure raises GXValidationFailed exception."""
        task_id = f"validate_batch_failure_integration_test_{rand_name()}"
        # Create data that will fail validation
        dataframe = pd.DataFrame(
            {self.COL_NAME: ["x", "y", "z"]}
        )  # values NOT in expected set
        batch_parameters = {"dataframe": dataframe}

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name=task_id)
                .add_dataframe_asset(task_id)
                .add_batch_definition_whole_dataframe(task_id)
            )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> gxe.ExpectColumnValuesToBeInSet:
            return gxe.ExpectColumnValuesToBeInSet(
                column=self.COL_NAME,
                value_set=["a", "b", "c"],  # different values to cause failure
            )

        validate_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters=batch_parameters,
        )

        mock_ti = Mock()
        with pytest.raises(GXValidationFailed):
            validate_batch.execute(context={"ti": mock_ti})

    @pytest.mark.integration
    def test_validation_failure_xcom_contains_result(self) -> None:
        """Test that when validation fails and exception is raised, xcom still contains the failed result."""
        task_id = f"validate_batch_failure_xcom_integration_test_{rand_name()}"
        # Create data that will fail validation
        dataframe = pd.DataFrame(
            {self.COL_NAME: ["x", "y", "z"]}
        )  # values NOT in expected set

        batch_parameters = {"dataframe": dataframe}

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name=task_id)
                .add_dataframe_asset(task_id)
                .add_batch_definition_whole_dataframe(task_id)
            )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> gxe.ExpectColumnValuesToBeInSet:
            return gxe.ExpectColumnValuesToBeInSet(
                column=self.COL_NAME,
                value_set=["a", "b", "c"],  # different values to cause failure
            )

        validate_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters=batch_parameters,
        )

        mock_ti = Mock()
        with pytest.raises(GXValidationFailed):
            validate_batch.execute(context={"ti": mock_ti})

        # Verify that xcom_push was called with the failed validation result
        mock_ti.xcom_push.assert_called_once()
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["key"] == "return_value"
        result = call_args[1]["value"]
        assert result["success"] is False
