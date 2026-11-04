from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal

from great_expectations_provider.common.errors import GXValidationFailed
from great_expectations_provider.common.gx_context_actions import (
    load_data_context,
    run_validation_definition,
)
from great_expectations_provider.version_compat import BaseOperator

if TYPE_CHECKING:
    from great_expectations import ExpectationSuite
    from great_expectations.core.batch import BatchParameters
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation

    from great_expectations_provider.version_compat import Context


class GXValidateBatchOperator(BaseOperator):
    """
    An operator to use Great Expectations to validate Expectations against a Batch of data in your Airflow DAG.

    Args:
        task_id: Airflow task ID. Alphanumeric name used in the Airflow UI and to name the GX objects the Operator creates.
        configure_batch_definition: A callable that returns a BatchDefinition to configure GX to read your data.
            For more information, see https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/#create-a-batch-definition).
        configure_expectations: A callable that takes an AbstractDataContext and returns an Expectation or
            ExpectationSuite to validate against the Batch. Available Expectations can be found at
            https://greatexpectations.io/expectations.
        batch_parameters: dictionary that specifies a time-based Batch of data  to validate your Expectations against.
            Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending)
            or the oldest Batch if the Batch Definition has been configured to sort descending.
            For more information see https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data.
        result_format: control the verbosity of returned Validation Results. Possible values are
            "BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE". Defaults to "SUMMARY". See
            https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format
            for more information.
    """

    def __init__(
        self,
        configure_batch_definition: Callable[[AbstractDataContext], BatchDefinition],
        configure_expectations: Callable[
            [AbstractDataContext], Expectation | ExpectationSuite
        ],
        batch_parameters: BatchParameters | None = None,
        result_format: (
            Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"] | None
        ) = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        if batch_parameters is None:
            self.batch_parameters = {}
        else:
            self.batch_parameters = batch_parameters
        self.configure_batch_definition = configure_batch_definition
        self.configure_expectations = configure_expectations
        self.result_format = result_format

    def execute(self, context: Context) -> None:
        gx_context = load_data_context()
        batch_definition = self.configure_batch_definition(gx_context)

        expect = self.configure_expectations(gx_context)

        runtime_batch_params = context.get("params", {}).get("gx_batch_parameters")  # type: ignore[call-overload]
        if runtime_batch_params:
            batch_parameters = runtime_batch_params
        else:
            batch_parameters = self.batch_parameters
        result = run_validation_definition(
            task_id=self.task_id,
            expect=expect,
            batch_definition=batch_definition,
            result_format=self.result_format,
            batch_parameters=batch_parameters,
            gx_context=gx_context,
        )
        result_dict = result.describe_dict()
        context["ti"].xcom_push(key="return_value", value=result_dict)
        if not result.success:
            raise GXValidationFailed(result_dict, self.task_id)
