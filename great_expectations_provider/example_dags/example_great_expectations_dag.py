"""Example Airflow DAG that validates the sample taxi data three ways.

The DAG runs a batch validation, a DataFrame validation and a checkpoint
validation in sequence, with one operator for each.

This example takes ``DAG`` and ``chain`` from the provider's ``version_compat``
module, so that it loads on every Airflow version the provider supports. In your
own DAGs on Airflow 3, import them from ``airflow.sdk``; on Airflow 2.11, import
them from Airflow's ``airflow.models`` modules.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
from great_expectations_provider.version_compat import DAG, chain

if TYPE_CHECKING:
    import pandas as pd
    from great_expectations import Checkpoint, ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext

base_path = Path(__file__).parents[2]
data_dir = base_path / "include" / "data"
data_file = data_dir / "yellow_tripdata_sample_2019-01.csv"


# configuration functions
def configure_pandas_batch_definition(context: AbstractDataContext) -> BatchDefinition:
    """This function takes a GX Context and returns a BatchDefinition that
    can load our CSV files from the data directory."""
    data_source = context.data_sources.add_pandas_filesystem(
        name="Extract Data Source",
        base_directory=data_dir,
    )
    asset = data_source.add_csv_asset(name="Extract CSV Asset")
    batch_definition = asset.add_batch_definition_monthly(
        name="Extract Batch Definition",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    )
    return batch_definition


def configure_data_frame() -> pd.DataFrame:
    import pandas as pd

    return pd.read_csv(data_file)


def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
    """This function takes a GX Context and returns a Checkpoint that
    can load our CSV files from the data directory, validate them
    against an ExpectationSuite, and run Actions."""
    # import GX objects locally per Airflow best practices

    import great_expectations.expectations as gxe
    from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

    # setup data source, asset, batch definition
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name="Load Datasource", base_directory=data_dir
        )
        .add_csv_asset("Load Asset")
        .add_batch_definition_monthly(
            name="Load Batch Definition",
            regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
        )
    )
    # setup expectation suite
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name="Load ExpectationSuite",
            expectations=[
                gxe.ExpectTableRowCountToBeBetween(
                    min_value=9000,
                    max_value=11000,
                ),
                gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
                gxe.ExpectColumnValuesToBeBetween(
                    column="passenger_count", min_value=1, max_value=6
                ),
            ],
        )
    )
    # setup validation definition
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="Load Validation Definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )
    # setup checkpoint
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="Load Checkpoint",
            validation_definitions=[validation_definition],
            actions=[],
        )
    )
    return checkpoint


# To demo validation failure, use FAILURE_MONTH as a batch parameter instead of SUCCESS_MONTH
SUCCESS_MONTH = 1
FAILURE_MONTH = 2
batch_parameters = {"year": 2019, "month": SUCCESS_MONTH}


def configure_expectations_suite(context: AbstractDataContext) -> ExpectationSuite:
    # import GX objects locally per Airflow best practices

    import great_expectations.expectations as gxe
    from great_expectations import ExpectationSuite

    return context.suites.add_or_update(
        ExpectationSuite(
            name="Taxi Data Expectations",
            expectations=[
                gxe.ExpectTableRowCountToBeBetween(
                    min_value=9000,
                    max_value=11000,
                ),
                gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
                gxe.ExpectColumnValuesToBeBetween(
                    column="passenger_count", min_value=1, max_value=6
                ),
            ],
        )
    )


with DAG(
    dag_id="gx_provider_example_dag",
    schedule=None,
) as dag:
    validate_extract = GXValidateBatchOperator(
        task_id="validate_extract",
        configure_batch_definition=configure_pandas_batch_definition,
        configure_expectations=configure_expectations_suite,
        batch_parameters=batch_parameters,
    )

    validate_transform = GXValidateDataFrameOperator(
        task_id="validate_transform",
        configure_dataframe=configure_data_frame,
        configure_expectations=configure_expectations_suite,
    )

    validate_load = GXValidateCheckpointOperator(
        task_id="validate_load",
        configure_checkpoint=configure_checkpoint,
        batch_parameters=batch_parameters,
    )

    chain(
        validate_extract,
        validate_transform,
        validate_load,
    )
