import json
import warnings
from typing import TYPE_CHECKING, Literal
from unittest.mock import Mock, create_autospec

import pandas as pd
import pytest
from great_expectations import ExpectationSuite
from great_expectations.core import ExpectationValidationResult
from great_expectations.data_context import AbstractDataContext, EphemeralDataContext
from great_expectations.datasource.fluent import PandasDatasource, SparkDatasource
from great_expectations.datasource.fluent.pandas_datasource import (
    DataFrameAsset as PandasDataFrameAsset,
)
from great_expectations.datasource.fluent.spark_datasource import (
    DataFrameAsset as SparkDataFrameAsset,
)
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.common.constants import USER_AGENT_STR
from great_expectations_provider.common.errors import GXValidationFailed
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

pytestmark = pytest.mark.unit


class MockSparkDataFrame:
    # Dummy class so we don't have to have pyspark dep in unit tests
    ...


MockSparkDataFrame.__name__ = "DataFrame"


class TestValidateDataFrameOperator:
    def test_expectation(self) -> None:
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return expect

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=configure_dataframe,
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        json.dumps(pushed_result)  # result must be json serializable
        deserialized_result = ExpectationValidationResult(**pushed_result)
        assert deserialized_result.success

    def test_expectation_suite(self) -> None:
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        def configure_expectations_suite(
            context: AbstractDataContext,
        ) -> ExpectationSuite:
            return context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name,
                            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
                        ),
                    ],
                )
            )

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=configure_dataframe,
            configure_expectations=configure_expectations_suite,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

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
    ) -> None:
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return expect

        validate_df = GXValidateDataFrameOperator(
            task_id="test-result-format",
            configure_dataframe=configure_dataframe,
            configure_expectations=configure_expectations,
            result_format=result_format,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        # check the result of the first (only) expectation
        assert pushed_result["expectations"][0]["result"] == expected_result

    def test_uses_ephemeral_context(self, mock_gx_with_pandas_datasource: Mock):
        """Expect that the operator uses an ephemeral context."""

        # arrange
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=Mock(spec=pd.DataFrame)),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

        # assert
        mock_gx_with_pandas_datasource.get_context.assert_called_once_with(
            mode="ephemeral",
            user_agent_str=USER_AGENT_STR,
        )

    def test_pandas_does_not_error_when_no_datasource(
        self, mock_gx_no_datasource: Mock
    ) -> None:
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=Mock(spec=pd.DataFrame)),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

    def test_pandas_does_not_error_when_no_asset(
        self, mock_gx_with_pandas_datasource_but_no_asset: Mock
    ) -> None:
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=Mock(spec=pd.DataFrame)),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

    def test_pandas_does_not_error_when_no_batch_definition(
        self, mock_gx_with_pandas_datasource_but_no_batch_definition: Mock
    ) -> None:
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=Mock(spec=pd.DataFrame)),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

    def test_spark_does_not_error_when_no_datasource(
        self, mock_gx_no_datasource: Mock
    ) -> None:
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=Mock(spec=pd.DataFrame)),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

    def test_spark_does_not_error_when_no_asset(
        self, mock_gx_with_spark_datasource_but_no_asset: Mock
    ) -> None:
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=MockSparkDataFrame()),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

    def test_spark_does_not_error_when_no_batch_definition(
        self, mock_gx_with_spark_datasource_but_no_batch_definition: Mock
    ) -> None:
        def configure_expectations(context: AbstractDataContext) -> Mock:
            return Mock()

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(return_value=MockSparkDataFrame()),
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)

    def test_validation_failure_raises_exception(self) -> None:
        """Expect that when validation fails, GXValidationFailed exception is raised."""

        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame(
                {column_name: ["x", "y", "z"]}
            )  # values NOT in the expected set

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c"],  # different values to cause failure
        )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return expect

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_failure",
            configure_dataframe=configure_dataframe,
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_df.execute(context=context)

    def test_validation_failure_xcom_contains_result(self) -> None:
        """Expect that when validation fails and exception is raised, xcom still contains the result."""

        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame(
                {column_name: ["x", "y", "z"]}
            )  # values NOT in the expected set

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c"],  # different values to cause failure
        )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return expect

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_failure",
            configure_dataframe=configure_dataframe,
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_df.execute(context=context)

        # Verify that xcom_push was called with the validation result
        mock_ti.xcom_push.assert_called_once()
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["key"] == "return_value"
        result = call_args[1]["value"]
        assert result["success"] is False

    def test_deprecated_expect_parameter_raises_warning(self) -> None:
        """Expect that using deprecated 'expect' parameter raises DeprecationWarning but still works."""
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )

        # act & assert
        with pytest.warns(
            DeprecationWarning, match="The 'expect' parameter is deprecated"
        ):
            validate_df = GXValidateDataFrameOperator(
                task_id="validate_df_deprecated",
                configure_dataframe=configure_dataframe,
                expect=expect,
            )

        # Verify it still works
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]
        validate_df.execute(context=context)
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    def test_missing_configure_expectations_raises_value_error(self) -> None:
        """Expect that omitting both 'configure_expectations' and deprecated 'expect' raises ValueError."""

        # arrange
        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({"col": [1, 2, 3]})

        # act & assert
        with pytest.raises(ValueError, match="configure_expectations is required"):
            GXValidateDataFrameOperator(
                task_id="validate_df_missing",
                configure_dataframe=configure_dataframe,
            )

    @pytest.fixture
    def mock_gx_no_datasource(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_datasource = create_autospec(spec=PandasDatasource)
        mock_datasource.get_asset.side_effect = LookupError
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(side_effect=KeyError)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx

    @pytest.fixture
    def mock_gx_with_pandas_datasource(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_datasource = create_autospec(spec=PandasDatasource)
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(return_value=mock_datasource)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx

    @pytest.fixture
    def mock_gx_with_pandas_datasource_but_no_asset(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_datasource = create_autospec(spec=PandasDatasource)
        mock_datasource.get_asset.side_effect = LookupError
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(return_value=mock_datasource)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx

    @pytest.fixture
    def mock_gx_with_pandas_datasource_but_no_batch_definition(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_asset = create_autospec(
            spec=PandasDataFrameAsset,
            get_batch_definition=Mock(side_effect=KeyError),
        )
        mock_datasource = create_autospec(
            spec=PandasDatasource,
            get_asset=Mock(return_value=mock_asset),
        )
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(return_value=mock_datasource)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx

    @pytest.fixture
    def mock_gx_with_spark_datasource(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_datasource = create_autospec(spec=SparkDatasource)
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(return_value=mock_datasource)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx

    @pytest.fixture
    def mock_gx_with_spark_datasource_but_no_asset(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_datasource = create_autospec(spec=SparkDatasource)
        mock_datasource.get_asset.side_effect = LookupError
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(return_value=mock_datasource)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx

    @pytest.fixture
    def mock_gx_with_spark_datasource_but_no_batch_definition(
        self,
        mock_gx: Mock,
    ) -> Mock:
        mock_asset = create_autospec(
            spec=SparkDataFrameAsset,
            get_batch_definition=Mock(side_effect=KeyError),
        )
        mock_datasource = create_autospec(
            spec=SparkDatasource,
            get_asset=Mock(return_value=mock_asset),
        )
        mock_context = create_autospec(
            spec=EphemeralDataContext,
            data_sources=Mock(get=Mock(return_value=mock_datasource)),
        )
        mock_gx.get_context.return_value = mock_context
        return mock_gx
