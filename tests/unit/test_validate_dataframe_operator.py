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
from great_expectations_provider.version_compat import AirflowException

if TYPE_CHECKING:
    from great_expectations_provider.version_compat import Context

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

    def test_expect_parameter_is_rejected(self) -> None:
        """Expect that the removed 'expect' parameter fails at construction as an invalid argument."""

        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            return ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c"],  # type: ignore[arg-type]
            )

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a"],  # type: ignore[arg-type]
        )

        # act & assert
        with pytest.raises(
            (AirflowException, TypeError), match="Invalid arguments were passed"
        ) as exc_info:
            GXValidateDataFrameOperator(
                task_id="validate_df_expect",
                configure_dataframe=configure_dataframe,
                configure_expectations=configure_expectations,
                expect=expect,
            )
        assert "'expect'" in str(exc_info.value)

    def test_expect_parameter_without_configure_expectations_is_rejected(
        self,
    ) -> None:
        """Expect that a 1.x style call passing only 'expect' fails at construction."""

        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a"],  # type: ignore[arg-type]
        )

        # act & assert
        with pytest.raises(
            (AirflowException, TypeError),
            match="missing keyword argument 'configure_expectations'",
        ):
            GXValidateDataFrameOperator(  # type: ignore[call-arg]
                task_id="validate_df_expect_only",
                configure_dataframe=configure_dataframe,
                expect=expect,
            )

    def test_missing_configure_expectations_is_rejected(self) -> None:
        """Expect that omitting 'configure_expectations' fails at construction."""

        # arrange
        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({"col": [1, 2, 3]})

        # act & assert
        with pytest.raises(
            (AirflowException, TypeError),
            match="missing keyword argument 'configure_expectations'",
        ):
            GXValidateDataFrameOperator(  # type: ignore[call-arg]
                task_id="validate_df_missing",
                configure_dataframe=configure_dataframe,
            )

    def test_construction_does_not_call_configure_dataframe(self) -> None:
        """Expect that constructing the operator, as DAG parsing does, loads no data."""

        # arrange
        configure_dataframe = Mock(return_value=pd.DataFrame({"col_A": ["a"]}))

        # act
        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_construction",
            configure_dataframe=configure_dataframe,
            configure_expectations=Mock(),
        )

        # assert
        configure_dataframe.assert_not_called()
        assert validate_df.configure_dataframe is configure_dataframe
        assert not hasattr(validate_df, "dataframe")

    def test_execute_calls_configure_dataframe_once_and_validates_its_result(
        self,
    ) -> None:
        """Expect that execute() loads the DataFrame exactly once and validates that DataFrame."""

        # arrange
        column_name = "col_A"
        configure_dataframe = Mock(
            return_value=pd.DataFrame({column_name: ["a", "b", "c", "d"]})
        )
        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )
        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_execute_loads_data",
            configure_dataframe=configure_dataframe,
            configure_expectations=lambda context: expect,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        calls_after_construction = configure_dataframe.call_count
        validate_df.execute(context=context)

        # assert
        assert calls_after_construction == 0
        configure_dataframe.assert_called_once_with()
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True
        # the four rows returned by configure_dataframe are the rows validated
        assert pushed_result["expectations"][0]["result"]["element_count"] == 4

    def test_each_execute_calls_configure_dataframe_again(self) -> None:
        """Expect that every run loads fresh data, so a retry sees the source as it is now."""

        # arrange
        column_name = "col_A"
        configure_dataframe = Mock(
            side_effect=[
                pd.DataFrame({column_name: ["a", "b", "c"]}),
                pd.DataFrame({column_name: ["x", "y", "z"]}),
            ]
        )

        def configure_expectations(
            context: AbstractDataContext,
        ) -> ExpectColumnValuesToBeInSet:
            # a new Expectation per run: GX does not accept one that a previous run has used
            return ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c"],  # type: ignore[arg-type]
            )

        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_execute_reloads_data",
            configure_dataframe=configure_dataframe,
            configure_expectations=configure_expectations,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context)
        with pytest.raises(GXValidationFailed):
            validate_df.execute(context=context)

        # assert
        assert configure_dataframe.call_count == 2

    def test_configure_dataframe_error_is_raised_by_execute_not_construction(
        self,
    ) -> None:
        """Expect that a failing data source fails the task, not the DAG import."""

        # arrange
        def configure_dataframe() -> pd.DataFrame:
            raise ConnectionError("data source unavailable")

        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_df = GXValidateDataFrameOperator(
            task_id="validate_df_source_unavailable",
            configure_dataframe=configure_dataframe,
            configure_expectations=Mock(),
        )

        # assert
        with pytest.raises(ConnectionError, match="data source unavailable"):
            validate_df.execute(context=context)
        mock_ti.xcom_push.assert_not_called()

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
