# Migration guide

This guide will help you migrate from V0 to V1 of the Great Expectations Airflow Provider. If you are upgrading from 1.x and used GX Cloud, `conn_id`, or `context_type`, see [Upgrade from 1.x to 2.0: GX Cloud removal](#upgrade-from-1x-to-20-gx-cloud-removal).

Here is an overview of key differences between versions:

| Provider version | V0 | V1                                                                                                                                                    |
|---|---|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| Operators | `GreatExpectationsOperator` | `GXValidateDataFrameOperator`<br>`GXValidateBatchOperator`<br>`GXValidateCheckpointOperator`                                                          |
| GX version | 0.18 and earlier | 1.7.0 and later                                                                                                                                       |
| Data Contexts | File | Ephemeral<br>File (`GXValidateCheckpointOperator` only)                                                                                      |
| Response handling | By default, any Expectation failure raises an `AirflowException`. To override this behavior and continue running the pipeline even if tests fail, you can set the `fail_task_on_validation_failure` flag to `False`. | Regardless of Expectation failure or success, a Validation Result is made available to subsequent tasks, which can decide what to do with the result. |

For guidance on which Operator and Data Context best fit your needs, see [Operator use cases](getting-started.md/#operator-use-cases). Note that while File Data Contexts are still supported with `GXValidateCheckpointOperator`, they require extra configuration and can be challenging to use when Airflow is running in a distributed environment. Most uses of the legacy `GreatExpectationsOperator` can now be satisfied with an Ephemeral Data Context with either the `GXValidateDataFrameOperator` or the `GXValidateBatchOperator` to minimize configuration.

## Switch to the new Data Frame or Batch Operator (recommended)

The configuration options for the new `GXValidateDataFrameOperator` and `GXValidateBatchOperator` are streamlined compared to the old `GreatExpectationsOperator`. Switching to one of these doesn’t involve translating existing configuration into new syntax line for line but rather paring back to a more minimal configuration.

- See [getting started](getting-started.md) for an overview of required configuration.
- Explore [examples](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags) of end-to-end configuration and usage.

## Migrate to the new Checkpoint Operator

If you want to update your existing `GreatExpectationsOperator` configuration to use the new `GXValidateCheckpointOperator` with a File Data Context, follow these steps:

1. Update your GX project to be compatible with GX V1 by following the [GX V0 to V1 Migration Guide](https://docs.greatexpectations.io/docs/reference/learn/migration_guide).

2. Write a function that instantiates and returns your File Data Context.

   Here is a basic example for running Airflow in a stable file system where you can rely on your GX project being discoverable at the same place over multiple runs. This is important because GX will write Validation Results back to the project directory.

   ```python
    from __future__ import annotations
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from great_expectations.data_context import FileDataContext

    def configure_file_data_context() -> FileDataContext:
        # import GX locally per Airflow best practices
        from great_expectations import get_context

        return get_context(
           mode="file",
           project_root_dir="./path_to_your_existing_project"
        )
   ```

   Here's a more advanced example for running Airflow in an environment where the underlying file system is not stable. The steps here are as follows:
    - fetch your GX project
    - load the context
    - yield the context to the Operator
    - after the Operator has finished, write your project configuration back to the remote

   Be aware that you are responsible for managing concurrency, in the case that multiple tasks are reading and writing back to the remote simultaneously.

   ```python
    from __future__ import annotations
    from typing import TYPE_CHECKING, Generator

    if TYPE_CHECKING:
        from great_expectations.data_context import FileDataContext

    def configure_file_data_context() -> Generator[FileDataContext, None, None]:
        # import GX locally per Airflow best practices
        from great_expectations import get_context

        # load your GX project from its remote source
        yield get_context(
           mode="file",
           project_root_dir="./path_to_the_local_copy_of_your_existing_project"
        )
        # write your GX project back to its remote source
   ```

3. Write a function that returns your Checkpoint.

   ```python
    from __future__ import annotations
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from great_expectations import Checkpoint
        from great_expectations.data_context import AbstractDataContext


    def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
       return context.checkpoints.get(name="<YOUR CHECKPOINT NAME>")
   ```

- See [getting started](getting-started.md) for more information about required and optional configuration.
- Explore [examples](https://github.com/great-expectations/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags) of end-to-end configuration and usage.


## Migrate Connections

The legacy Great Expectations Airflow Provider accepted a `conn_id` argument, which
would attempt to retrieve credentials from other Airflow provider Connections. That logic
is now available as provider-specific functions in `great_expectations_provider.common.external_connections`,
and can be used when configuring your Great Expectations data source within the `configure_batch_definition` or
`configure_checkpoint` function.

Here is an example that uses the `build_snowflake_key_connection` function to connect to Snowflake using a private key.

```python
from __future__ import annotations
from typing import TYPE_CHECKING

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from great_expectations_provider.common.external_connections import (
    build_snowflake_key_connection
)

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation
    from great_expectations import ExpectationSuite


def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
    snowflake_config = build_snowflake_key_connection(conn_id="snowflake_conn_id")
    return (
        context.data_sources.add_snowflake(
            name="snowflake example",
            account=snowflake_config.account,
            user=snowflake_config.user,
            role=snowflake_config.role,
            private_key=snowflake_config.private_key.decode(),
            warehouse=snowflake_config.warehouse,
            database=snowflake_config.database,
            schema=snowflake_config.schema_name,
        )
        .add_table_asset(name="<SCHEMA.TABLE>", table_name="<TABLE_NAME>")
        .add_batch_definition_whole_table(  # you can also batch by year, month, or day here
            name="Whole Table Batch Definition"
        )
    )


def configure_expectations(context: AbstractDataContext) -> Expectation | ExpectationSuite:
    from great_expectations import ExpectationSuite
    return context.suites.add_or_update(
        ExpectationSuite(
            name="<SUITE_NAME>",
            expectations=[
                # define expectations
            ],
        )
    )


my_batch_operator = GXValidateBatchOperator(
    task_id="my_batch_operator",
    configure_batch_definition=configure_batch_definition,
    configure_expectations=configure_expectations,
)
```

## Upgrade from 1.x to 2.0: GX Cloud removal

GX Cloud stopped being publicly available on 2026-06-01, and Great Expectations (GX) 1.18 and later raise an error for Cloud Data Contexts. Provider 2.0 removes the `great_expectations_provider.hooks.gx_cloud` module and its `GXCloudHook`, `GXCloudConfig`, and `IncompleteGXCloudConfigError` classes, as well as the `gx_cloud` connection type. It also removes the `conn_id` parameter from all three operators and the `context_type` parameter from `GXValidateBatchOperator` and `GXValidateDataFrameOperator`; those operators always use an Ephemeral Data Context. `GXValidateCheckpointOperator` no longer accepts the `"cloud"` value for `context_type`.

### What to change

| 1.x usage | 2.0 action |
|---|---|
| `conn_id=` on any operator | Delete the argument. |
| `context_type=` on `GXValidateBatchOperator` or `GXValidateDataFrameOperator` | Delete the argument. |
| `context_type="cloud"` on `GXValidateCheckpointOperator` | Use `"ephemeral"` (the default), or use `"file"` with `configure_file_data_context`. |
| `conn_id` or `context_type` in `default_args` | Delete those keys. Airflow silently ignores `default_args` keys an operator does not accept; `GXValidateCheckpointOperator` validates `context_type`. |
| `gx_cloud` Airflow connections | Delete the connections. |
| `GX_CLOUD_*` variables in the Airflow environment | Remove the variables. The operators ignore them, but your own callables that call `gx.get_context()` without a `mode` raise while they are set. |
| Imports from `great_expectations_provider.hooks.gx_cloud` | Delete the imports. |

### Errors you will see if you do not migrate

At DAG parse, Airflow reports `TypeError` on Airflow 3 or `AirflowException` on Airflow 2 for invalid operator arguments. The message begins `Invalid arguments were passed to <Operator>` and includes the invalid values under `**kwargs`: `{…}`. This applies to `conn_id` on any operator and `context_type` on the batch or DataFrame operator.

For an unsupported checkpoint `context_type`, construction raises `ValueError` with this message (where the received value is shown):

```text
Parameter `context_type` must be 'ephemeral' or 'file', got 'cloud'
```

Importing the removed hook module raises `ModuleNotFoundError`.

### Keeping validation results

To keep persistent results and view them in Data Docs, use a File Data Context with `GXValidateCheckpointOperator`; see the [File Data Context setup step](getting-started.md#checkpoint-operator). You can also read the operator's XCom `return_value` in a downstream task. Results no longer include a GX Cloud link.
