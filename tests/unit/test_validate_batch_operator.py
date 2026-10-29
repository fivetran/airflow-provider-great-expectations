import json
import warnings
from typing import TYPE_CHECKING, Literal
from unittest.mock import Mock, create_autospec

import pandas as pd
import pytest
from great_expectations import ExpectationSuite
from great_expectations.core import ExpectationValidationResult
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.common.constants import USER_AGENT_STR
from great_expectations_provider.common.errors import GXValidationFailed
from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

pytestmark = pytest.mark.unit


class TestValidateBatchOperator:
    def test_expectation(self):
        """Expect that an Expectation can be used as an `expect` parameter to generate a result."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["a", "b", "c"]})
        expect = ExpectColumnValuesToBeInSet(
            column=column_name, value_set=["a", "b", "c", "d", "e"]
        )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return expect

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=configure_ephemeral_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters={"dataframe": df},
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_batch.execute(context=context)

        # assert
        # Get the result from xcom_push call
        mock_ti.xcom_push.assert_called_once_with(
            key="return_value", value=mock_ti.xcom_push.call_args[1]["value"]
        )
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        deserialized_result = ExpectationValidationResult(**pushed_result)
        assert deserialized_result.success

    def test_expectation_suite(self):
        """Expect that an ExpectationSuite can be used as an `expect` parameter to generate a result."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["a", "b", "c"]})

        def configure_expectations_suite(
            context: AbstractDataContext,
        ) -> ExpectationSuite:
            return context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name, value_set=["a", "b", "c", "d", "e"]
                        ),
                    ],
                )
            )

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=configure_ephemeral_batch_definition,
            configure_expectations=configure_expectations_suite,
            batch_parameters={"dataframe": df},
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_batch.execute(context=context)

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        json.dumps(pushed_result)  # result must be json serializable
        assert pushed_result["success"] is True

    @pytest.mark.parametrize(
        "result_format,expected_result",
        [
            pytest.param("BOOLEAN_ONLY", {}, id="boolean"),
            pytest.param(
                "BASIC",
                {
                    "element_count": 3,
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "partial_unexpected_list": [],
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "unexpected_percent_total": 0.0,
                },
                id="basic",
            ),
            pytest.param(
                "SUMMARY",
                {
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_counts": [],
                    "partial_unexpected_index_list": [],
                },
                id="summary",
            ),
            pytest.param(
                "COMPLETE",
                {
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_counts": [],
                    "partial_unexpected_index_list": [],
                    "unexpected_list": [],
                    "unexpected_index_list": [],
                    "unexpected_index_query": "df.filter(items=[], axis=0)",
                },
                id="complete",
            ),
        ],
    )
    def test_result_format(
        self,
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
        expected_result: dict,
    ):
        """Expect that valid values of param result_format alter result as expected."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["a", "b", "c"]})

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
            )

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=configure_ephemeral_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters={"dataframe": df},
            result_format=result_format,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_batch.execute(context=context)

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        # check the result of the first (only) expectation
        assert pushed_result["expectations"][0]["result"] == expected_result

    def test_uses_ephemeral_context(self, mock_gx: Mock):
        """Expect that the operator uses an ephemeral context."""

        # arrange
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: Mock(),
            configure_expectations=configure_expectations,
            batch_parameters={"dataframe": Mock()},
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_batch.execute(context=context)

        # assert
        mock_gx.get_context.assert_called_once_with(
            mode="ephemeral",
            user_agent_str=USER_AGENT_STR,
        )

    def test_batch_parameters_passed_on_init(self, mock_gx: Mock):
        """Expect that param batch_parameters is passed to BatchDefinition.get_batch"""
        # arrange
        mock_context = mock_gx.get_context.return_value
        mock_validation_definition = (
            mock_context.validation_definitions.add_or_update.return_value
        )
        mock_batch_definition = Mock()
        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }

        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: mock_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters=batch_parameters,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_batch.execute(context=context)

        # assert
        mock_validation_definition.run.assert_called_once_with(
            batch_parameters=batch_parameters
        )

    def test_batch_parameters_passed_through_context_parameters(self, mock_gx: Mock):
        """Expect that param batch_parameters is passed to BatchDefinition.get_batch"""
        # arrange
        mock_context = mock_gx.get_context.return_value
        mock_validation_definition = (
            mock_context.validation_definitions.add_or_update.return_value
        )
        mock_batch_definition = create_autospec(BatchDefinition)
        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }

        def configure_expectations_suite(
            context: AbstractDataContext,
        ) -> ExpectationSuite:
            return create_autospec(ExpectationSuite)

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: mock_batch_definition,
            configure_expectations=configure_expectations_suite,
        )
        mock_ti = Mock()
        context: Context = {
            "ti": mock_ti,
            "params": {"gx_batch_parameters": batch_parameters},  # type: ignore[typeddict-item]
        }

        # act
        validate_batch.execute(context=context)

        # assert
        mock_validation_definition.run.assert_called_once_with(
            batch_parameters=batch_parameters
        )

    def test_context_batch_parameters_take_precedence(self, mock_gx: Mock):
        """Expect that param batch_parameters is passed to BatchDefinition.get_batch"""
        # arrange
        mock_context = mock_gx.get_context.return_value
        mock_validation_definition = (
            mock_context.validation_definitions.add_or_update.return_value
        )
        mock_batch_definition = create_autospec(BatchDefinition)
        init_batch_parameters = {
            "year": "2020",
            "month": "02",
            "day": "09",
        }
        context_batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }

        def configure_expectations_suite(
            context: AbstractDataContext,
        ) -> ExpectationSuite:
            return create_autospec(ExpectationSuite)

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: mock_batch_definition,
            configure_expectations=configure_expectations_suite,
            batch_parameters=init_batch_parameters,
        )
        mock_ti = Mock()
        context: Context = {
            "ti": mock_ti,
            "params": {"gx_batch_parameters": context_batch_parameters},  # type: ignore[typeddict-item]
        }

        # act
        validate_batch.execute(context=context)

        # assert
        mock_validation_definition.run.assert_called_once_with(
            batch_parameters=context_batch_parameters
        )

    def test_validation_definition_construction(self, mock_gx: Mock):
        """Expect that the expect param, the task_id, and the return value
        of the configure_batch_definition param are used to construct a ValidationDefinition.
        """
        # arrange
        task_id = "test_validation_definition_construction"
        mock_context = mock_gx.get_context.return_value
        mock_validation_definition_factory = mock_context.validation_definitions
        mock_validation_definition = mock_gx.ValidationDefinition.return_value
        mock_batch_definition = create_autospec(BatchDefinition)
        expect = create_autospec(ExpectationSuite)

        def configure_expectations_suite(
            context: AbstractDataContext,
        ) -> ExpectationSuite:
            return expect

        validate_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=lambda context: mock_batch_definition,
            configure_expectations=configure_expectations_suite,
            batch_parameters=Mock(),
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_batch.execute(context=context)

        # assert
        mock_gx.ValidationDefinition.assert_called_once_with(
            name=task_id, suite=expect, data=mock_batch_definition
        )
        mock_validation_definition_factory.add_or_update.assert_called_once_with(
            validation=mock_validation_definition
        )

    def test_validation_failure_raises_exception(self):
        """Expect that when validation fails, GXValidationFailed exception is raised."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame(
            {column_name: ["x", "y", "z"]}
        )  # values NOT in the expected set

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c"],  # different values to cause failure
            )

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_failure",
            configure_batch_definition=configure_ephemeral_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters={"dataframe": df},
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_batch.execute(context=context)

    def test_validation_failure_xcom_contains_result(self):
        """Expect that when validation fails and exception is raised, xcom still contains the result."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame(
            {column_name: ["x", "y", "z"]}
        )  # values NOT in the expected set

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c"],  # different values to cause failure
            )

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_failure",
            configure_batch_definition=configure_ephemeral_batch_definition,
            configure_expectations=configure_expectations,
            batch_parameters={"dataframe": df},
        )

        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_batch.execute(context=context)

        # Verify that xcom_push was called with the validation result
        mock_ti.xcom_push.assert_called_once()
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["key"] == "return_value"
        result = call_args[1]["value"]
        assert result["success"] is False

    def test_deprecated_expect_parameter_raises_warning(self):
        """Expect that using deprecated 'expect' parameter raises DeprecationWarning but still works."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["a", "b", "c"]})
        expect = ExpectColumnValuesToBeInSet(
            column=column_name, value_set=["a", "b", "c", "d", "e"]
        )

        # act & assert
        with pytest.warns(
            DeprecationWarning, match="The 'expect' parameter is deprecated"
        ):
            validate_batch = GXValidateBatchOperator(
                task_id="validate_batch_deprecated",
                configure_batch_definition=configure_ephemeral_batch_definition,
                expect=expect,
                batch_parameters={"dataframe": df},
            )

        # Verify it still works
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]
        validate_batch.execute(context=context)
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    def test_missing_configure_expectations_raises_value_error(self):
        """Expect that omitting both 'configure_expectations' and deprecated 'expect' raises ValueError."""

        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        # act & assert
        with pytest.raises(ValueError, match="configure_expectations is required"):
            GXValidateBatchOperator(
                task_id="validate_batch_missing",
                configure_batch_definition=configure_ephemeral_batch_definition,
                batch_parameters={"dataframe": pd.DataFrame()},
            )
