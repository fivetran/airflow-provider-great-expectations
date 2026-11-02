# Getting started

[Great Expectations](https://greatexpectations.io/) (GX) is a framework for describing data using expressive tests and then validating that the data meets test criteria. [Great Expectations](https://greatexpectations.io/) maintains the Great Expectations Airflow Provider to give users a convenient method for running validations directly from their DAGs. The Great Expectations Airflow Provider has three Operators to choose from, which vary in the amount of configuration they require and the flexibility they provide.

- `GXValidateDataFrameOperator`
- `GXValidateBatchOperator`
- `GXValidateCheckpointOperator`


## Operator use cases

When deciding which Operator best fits your use case, consider the location of the data you are validating and whether you need external alerts or actions to be triggered by the Operator.

- If your data is in memory as a Spark or Pandas DataFrame, we recommend using the `GXValidateDataFrameOperator`. This option requires only a DataFrame and your Expectations to create a validation result.
- If your data is not in memory, we recommend configuring GX to connect to it by defining a BatchDefinition with the `GXValidateBatchOperator`. This option requires a BatchDefinition and your Expectations to create a validation result.
- If you want to [trigger actions](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions) based on validation results, use the `GXValidateCheckpointOperator`. This option supports all features of GX Core, so it requires the most configuration - you have to define a  Checkpoint, BatchDefinition, ExpectationSuite, and ValidationDefinition to get validation results.

The `GXValidateDataFrameOperator` and `GXValidateBatchOperator` use an Ephemeral Data Context. The `GXValidateCheckpointOperator` supports Ephemeral and File Data Contexts.

- The DataFrame and Batch operators use an Ephemeral Data Context. The serialized Validation Result is available within the DAG as the task result, but does not persist externally for viewing results across multiple runs.
- If you want to manage Validation Results yourself, use a File Data Context. With this option, Validation Results can be viewed in [Data Docs](https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_data_docs/). Only the `GXValidateCheckpointOperator` supports the File Data Context.

## Prerequisites

- [Python](https://www.python.org/) version 3.10 to 3.13
- [Great Expectations](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx) version 1.7.0+
- [Apache Airflow®](https://airflow.apache.org/) version 2.1.0+

## Assumed knowledge

To get the most out of this getting started guide, make sure you have an understanding of:

- The basics of Great Expectations. See [Try GX Core](https://docs.greatexpectations.io/docs/core/introduction/try_gx/).
- Airflow fundamentals, such as writing DAGs and defining tasks. See [Get started with Apache Airflow](https://www.astronomer.io/docs/learn/get-started-with-airflow/).
- Airflow Operators. See [Operators 101](https://www.astronomer.io/docs/learn/what-is-an-operator/).
- Airflow connections. See [Managing your Connections in Apache Airflow](https://www.astronomer.io/docs/learn/connections/).

## Install the provider and dependencies

1. Install the provider.

   ```bash
   pip install airflow-provider-great-expectations
   ```
2. (Optional) Install additional dependencies for the data sources you’ll use. For example, to install the optional Snowflake dependency, use the following command.

   ```bash
   pip install "airflow-provider-great-expectations[snowflake]"
   ```
   The following backends are supported as optional dependencies:
    - `athena`
    - `azure`
    - `bigquery`
    - `gcp`
    - `mssql`
    - `postgresql`
    - `s3`
    - `snowflake`
    - `spark`
    - `trino`

## Configure an Operator

After deciding [which Operator best fits your use case](#operator-use-cases), follow the Operator-specific instructions below to configure it.

### Data Frame Operator

1. Import the Operator.

    ```python
    from great_expectations_provider.operators.validate_dataframe import (
        GXValidateDataFrameOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    from typing import TYPE_CHECKING

    from great_expectations_provider.operators.validate_dataframe import (
        GXValidateDataFrameOperator,
    )

    if TYPE_CHECKING:
        from pandas import DataFrame
        from great_expectations import ExpectationSuite
        from great_expectations.data_context import AbstractDataContext
        from great_expectations.expectations import Expectation

    def configure_dataframe() -> DataFrame:
        import pandas as pd  # airflow best practice is to not import heavy dependencies in the top level
        return pd.read_csv("/path/to/my/data")

    def configure_expectations(context: AbstractDataContext) -> Expectation | ExpectationSuite:
        from great_expectations import ExpectationSuite
        return context.suites.add_or_update(
            ExpectationSuite(
                name="example expectation suite",
                expectations=[...]
            )
        )

    my_data_frame_operator = GXValidateDataFrameOperator(
        task_id="my_data_frame_operator",
        configure_dataframe=configure_dataframe,
        configure_expectations=configure_expectations,
    )
    ```

    - **`task_id`**: Airflow task ID. Alphanumeric name used in the Airflow UI and to name the GX objects the Operator creates.
    - **`configure_dataframe`**: function that returns a DataFrame to pass data to the Operator.
    - **`configure_expectations`**: function that returns either a [single Expectation](https://docs.greatexpectations.io/docs/core/define_expectations/create_an_expectation) or an [Expectation Suite](https://docs.greatexpectations.io/docs/core/define_expectations/organize_expectation_suites) to validate against your data.
    - **`result_format` (optional)**: accepts `BOOLEAN_ONLY`, `BASIC`, `SUMMARY`, or `COMPLETE` to set the [verbosity of returned Validation Results](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/). Defaults to `SUMMARY`.

   For more details, explore this [end-to-end code sample](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags/example_great_expectations_dag.py#L134-L138).

### Batch Operator

1. Import the Operator.

    ```python
    from great_expectations_provider.operators.validate_batch import (
        GXValidateBatchOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    from typing import TYPE_CHECKING

    from great_expectations_provider.operators.validate_batch import (
        GXValidateBatchOperator,
    )

    if TYPE_CHECKING:
        from great_expectations import ExpectationSuite
        from great_expectations.data_context import AbstractDataContext
        from great_expectations.expectations import Expectation
        from great_expectations.core.batch_definition import BatchDefinition


    def configure_expectations(context: AbstractDataContext) -> Expectation | ExpectationSuite:
        from great_expectations import ExpectationSuite
        return context.suites.add_or_update(
            ExpectationSuite(
                name="example expectation suite",
                expectations=[...]
            )
        )

    def configure_postgres_batch_definition(context: AbstractDataContext) -> BatchDefinition:
        """This function takes a GX Context and returns a BatchDefinition that
        loads a batch of data from a PostgreSQL table."""
        return (
            context.data_sources.add_postgres(
             name="example data source",
             connection_string="${POSTGRES_CONNECTION_STRING}",  # load credentials from environment
            )
            .add_table_asset(
             name="example table asset",
             table_name="my_table",
            )
            .add_batch_definition_whole_table(
                name="example batch definition"
            )
        )


    my_batch_operator = GXValidateBatchOperator(
        task_id="my_postgres_batch_operator",
        configure_batch_definition=configure_postgres_batch_definition,
        configure_expectations=configure_expectations,
    )
    ```

    - **`task_id`**: Airflow task ID. Alphanumeric name used in the Airflow UI and to name the GX objects the Operator creates.
    - **`configure_batch_definition`**: function that returns a [BatchDefinition](https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/#create-a-batch-definition) to configure GX to read your data.
    - **`configure_expectations`**: function that returns either a [single Expectation](https://docs.greatexpectations.io/docs/core/define_expectations/create_an_expectation) or an [Expectation Suite](https://docs.greatexpectations.io/docs/core/define_expectations/organize_expectation_suites) to validate against your data.
    - **`batch_parameters` (optional)**: dictionary that specifies a [time-based Batch of data](https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data) to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.
    - **`result_format` (optional)**: accepts `BOOLEAN_ONLY`, `BASIC`, `SUMMARY`, or `COMPLETE` to set the [verbosity of returned Validation Results](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/). Defaults to `SUMMARY`.

   For more details, explore this [end-to-end code sample](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags/example_dag_with_batch_parameters.py#L123-L127).

### Checkpoint Operator

1. Import the Operator.

    ```python
    from great_expectations_provider.operators.validate_checkpoint import (
        GXValidateCheckpointOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    from typing import TYPE_CHECKING
    from pathlib import Path

    from great_expectations_provider.operators.validate_checkpoint import (
        GXValidateCheckpointOperator,
    )

    if TYPE_CHECKING:
        from great_expectations import ExpectationSuite, Checkpoint
        from great_expectations.data_context import AbstractDataContext



    def configure_csv_checkpoint(context: AbstractDataContext) -> Checkpoint:
        """This function takes a GX Context and returns a Checkpoint that
        can load CSV files from the data directory, validate them
        against an ExpectationSuite, and run Actions."""
        # import GX objects locally per Airflow best practices

        import great_expectations.expectations as gxe
        from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

        # Set up data source, asset, batch definition
        batch_definition = (
            context.data_sources.add_pandas_filesystem(
                name="Load Datasource", base_directory=Path("/path/to/my/data")
            )
            .add_csv_asset("Load Asset")
            .add_batch_definition_monthly(
                name="Load Batch Definition",
                regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
            )
        )
        # Set up expectation suite
        expectation_suite = context.suites.add(
            ExpectationSuite(
                name="Load ExpectationSuite",
                expectations=[...],
            )
        )
        # Set up validation definition
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                name="Load Validation Definition",
                data=batch_definition,
                suite=expectation_suite,
            )
        )
        # Set up checkpoint
        checkpoint = context.checkpoints.add(
            Checkpoint(
                name="Load Checkpoint",
                validation_definitions=[validation_definition],
                actions=[],
            )
        )
        return checkpoint


    my_checkpoint_operator = GXValidateCheckpointOperator(
        task_id="my_checkpoint_operator",
        configure_checkpoint=configure_csv_checkpoint,
    )
    ```

    - **`task_id`**: alphanumeric name used in the Airflow UI.
    - **`configure_checkpoint`**: function that returns a [Checkpoint](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions), which orchestrates a ValidationDefinition, BatchDefinition, and ExpectationSuite. The Checkpoint can also specify a Result Format and trigger actions based on Validation Results.
    - **`batch_parameters` (optional)**: dictionary that specifies a [time-based Batch of data](https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data) to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.
    - **`context_type`**: accepts `ephemeral` or `file` to set the [Data Context](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) used by the Operator. Defaults to `ephemeral`, which does not persist results between runs. To manage Validation Results yourself, use `file` and complete the additional File Data Context configuration below.
    - **`configure_file_data_context` (optional)**: function that returns a [FileDataContext](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context?context_type=file). Applicable only when using a File Data Context. See the additional File Data Context configuration below for more information.

   For more details, explore this [end-to-end code sample](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags/example_dag_with_batch_parameters.py#L134-L137).

3. If you use a File Data Context, pass the `configure_file_data_context` parameter. This takes a function that returns a [FileDataContext](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context?context_type=file). By default, GX will write results in the configuration directory. If you are retrieving your FileDataContext from a remote location, you can yield the FileDataContext in the `configure_file_data_context` function and write the directory back to the remote after control is returned to the generator.

### Manage Data Source credentials with Airflow Connections

The Great Expectations Airflow Provider includes functions to retrieve connection credentials from other Airflow provider Connections.
The following external Connections are supported:

#### Supported Connection Types

| Connection Type | API Function | External Provider Documentation |
|-----------------|-------------|--------------------------------|
| **Amazon Redshift** | `build_redshift_connection_string(conn_id, schema=None)` | [Amazon Provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html) |
| **MySQL** | `build_mysql_connection_string(conn_id, schema=None)` | [MySQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html) |
| **Microsoft SQL Server** | `build_mssql_connection_string(conn_id, schema=None)` | [Microsoft SQL Server Provider](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/connections/mssql.html) |
| **PostgreSQL** | `build_postgres_connection_string(conn_id, schema=None)` | [PostgreSQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html) |
| **Snowflake** | `build_snowflake_connection_string(conn_id, schema=None)` | [Snowflake Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) |
| **Snowflake (Key-based Auth)** | `build_snowflake_key_connection(conn_id, schema=None)` | [Snowflake Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) |
| **Google Cloud BigQuery** | `build_gcpbigquery_connection_string(conn_id, schema=None)` | [Google Cloud Provider](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html) |
| **SQLite** | `build_sqlite_connection_string(conn_id)` | [SQLite Provider](https://airflow.apache.org/docs/apache-airflow-providers-sqlite/stable/connections/sqlite.html) |
| **AWS Athena** | `build_aws_connection_string(conn_id, schema=None, database=None, s3_path=None, region=None)` | [Amazon Provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html) |
| **Trino** | `build_trino_connection_string(conn_id, schema=None, catalog=None)` | [Trino Provider](https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/connections/trino.html) |

#### Usage

To use these functions, first install the Airflow Provider that maintains the connection you need,
and use the Airflow UI to configure the Connection with your credentials.
Then, import the function you need from `great_expectations_provider.common.external_connections`
and use it within your `configure_batch_definition` or `configure_checkpoint` function.

```python
   from __future__ import annotations

   from typing import TYPE_CHECKING

   from great_expectations_provider.common.external_connections import (
       build_postgres_connection_string,
   )

   if TYPE_CHECKING:
       from great_expectations.data_context import AbstractDataContext
       from great_expectations.core.batch_definition import BatchDefinition


   def configure_postgres_batch_definition(
           context: AbstractDataContext,
   ) -> BatchDefinition:
       task_id = "example_task"
       table_name = "example_table"
       postgres_conn_id = "example_conn_id"
       return (
           context.data_sources.add_postgres(
               name=task_id,
               connection_string=build_postgres_connection_string(
                   conn_id=postgres_conn_id
               ),
           )
           .add_table_asset(
               name=task_id,
               table_name=table_name,
           )
           .add_batch_definition_whole_table(task_id)
       )

```


## Add the configured Operator to a DAG

After configuring an Operator, add it to a DAG. Explore our [example DAGs](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags), which have sample tasks that demonstrate Operator functionality.

Note that the shape of the Validation Results depends on both the Operator type and Result Format configuration.
- `GXValidateDataFrameOperator` and `GXValidateBatchOperator` return a serialized [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/reference/api/core/expectationsuitevalidationresult_class/)
- `GXValidateCheckpointOperator` returns a [CheckpointResult](https://docs.greatexpectations.io/docs/reference/api/checkpoint/CheckpointResult_class).
- The included fields depend on the [Result Format verbosity](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/?results=verbosity#validation-results-reference-tables).

## Run the DAG

[Trigger the DAG manually](https://www.astronomer.io/docs/learn/get-started-with-airflow#step-7-run-the-new-dag) or [run it on a schedule](https://www.astronomer.io/docs/learn/scheduling-in-airflow/) to start validating your expectations of your data.