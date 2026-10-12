# Apache Airflow Provider for Great Expectations

## 2.0.0a1 (2026-10-12)
* [FEATURE] Add support for Airflow v3.3; drop support for Airflow < v3.1.3
* [FEATURE] Add Trino connection support
* [MAINTENANCE] Use VERSION from constants.py during releases

## 1.0.0 (2026-01-26)
* [FEATURE] Workspace ID + GXCloud connection by @joshua-stauffer in #222
* [FEATURE] Refactor `expect` parameter to `configure_expectations` by @joshua-stauffer in #229
* [MAINTENANCE] Update core dependencies by @joshua-stauffer in #221
* [MAINTENANCE] Add upper bounds for Airflow and GX by @NathanFarmer in #227
* [MAINTENANCE] Resolve zizmor alerts - credential persistance, overly broad permissions and unpinned actions by @pankajkoti in #214
* [MAINTENANCE] CI: Add GitHub CodeQL analysis workflow by @pankajkoti in #210
* [MAINTENANCE] Add Zizmor analysis GitHub action by @pankajkoti in #209
* [MAINTENANCE] pre-commit-ci bot updates in #211, #213, #215, #217, #218, #219, #220, #223
* [DOCUMENTATION] Update CI to publish docs to GX GitHub Pages by @NathanFarmer in #228
* [DOCUMENTATION] Improve and document integration test setup by @NathanFarmer in #226
* [DOCUMENTATION] Add release process documentation by @NathanFarmer in #231

## 1.0.0a5 (2025-07-15)
* [FEATURE] Support external airflow connections by @joshua-stauffer in #201
* [BUGFIX] Raise error on validation failure by @joshua-stauffer in #196
* [DOCUMENTATION] Publish documentation on GitHub pages, refactor setup.cfg into pyproject.toml by @pankajkoti in #186
* [MAINTENANCE] Bump GX version to 1.3.11 by @tyler-hoffman in #188
* [MAINTENANCE] Use migrate command to initialise airflow metadata and uv to install dependencies in CI by @pankajkoti in #199
* [MAINTENANCE] Upgrade pre-commit hook version as being attempted by pre-commit bot in PR 189 by @pankajkoti in #190
* [MAINTENANCE] Require Authorize for all jobs on pull requests from external contributors in CI by @pankajkoti in #202
* [MAINTENANCE] Change CI on trigger event to pull_request from pull_request_target by @pankajkoti in #204
* [MAINTENANCE] Add environment and permissions for trusted publishing by @pankajkoti in #208
* [MAINTENANCE] pre-commit-ci bot updates in #185, #191, #192, #193, #200, #203, #207

## 1.0.0a3 (2025-03-18)
* [BUGFIX] Fix documentation links for URLs that appear in PyPI package listing README by @pankajkoti in #186

## 1.0.0a2 (2025-03-18)
* [BUGFIX] Replace use of add_or_update with explicit get or update by @tyler-hoffman in #174
* [MAINTENANCE] Fix integration test around multiple runs of ValidateDataFrame by @tyler-hoffman in #176
* [MAINTENANCE] Bump gx version to 1.3.9 by @tyler-hoffman in #178
* [DOCUMENTATION]Mention Committers in the documentation by @pankajkoti in #180
* [MAINTENANCE] pre-commit autoupdate by @pre-commit-ci in #173, #175, #183
* [Documentation] Publish documentation on GitHub pages by @pankajkoti in #186

## 1.0.0a1 (2025-02-21)
* [FEATURE] Add support for GX Core 1.0 by @joshua-stauffer in #161
* [DOCUMENTATION] Add documentation for the new operators added for supporting GX Core 1.0 by @klavavej in #163
* [FEATURE] Add GX Cloud connection hook by @joshua-stauffer in #169

## 0.3.0 (2025-02-10)
* [MAINTENANCE] Add deprecation warnings for existing operators by @pankajkoti in https://github.com/astronomer/airflow-provider-great-expectations/pull/164
* [MAINTENANCE] Drop support for Python < 3.8 by @pankajkoti in https://github.com/astronomer/airflow-provider-great-expectations/pull/167
* [MAINTENANCE] pre-commit-ci bot updates in #156, #157, #159

## 0.2.9 (2024-09-02)
* [FEATURE] Add schema as a template field by @antelmoa https://github.com/astronomer/airflow-provider-great-expectations/pull/135
* [BUGFIX] Fix compatibility issue with great_expectations 1.0.0 by @mostafasayed5 in https://github.com/astronomer/airflow-provider-great-expectations/pull/149

## 0.2.8

* [FEATURE] Add create_temp_table support by @antelmoa in https://github.com/astronomer/airflow-provider-great-expectations/pull/129

## 0.2.7

* [BUGFIX] Fix schema parameter for Postgres connections by @TJanif in https://github.com/astronomer/airflow-provider-great-expectations/pull/117
* [BUGFIX] Add a driver parameter to the MSSQL connection string by @TJaniF in https://github.com/astronomer/airflow-provider-great-expectations/pull/113/

## 0.2.6
* [FEATURE] Use Snowflake provider to build connection string by @ivanstillfront in https://github.com/astronomer/airflow-provider-great-expectations/pull/98
* [BUGFIX] Snowflake Region Should be Optional by @mpgreg in https://github.com/astronomer/airflow-provider-great-expectations/pull/101
* [MAINTENANCE] Update deprecated map_metric import in custom by @cdkini in https://github.com/astronomer/airflow-provider-great-expectations/pull/99
* [FEATURE] Add Athena Connection Support by @denimalpaca in https://github.com/astronomer/airflow-provider-great-expectations/pull/91

## 0.2.5
* [BUGFIX] Schema variable update fixes Issues #94 and #87 by @mpgreg in https://github.com/astronomer/airflow-provider-great-expectations/pull/95
* [FEATURE] Snowflake key auth by @ivanstillfront in https://github.com/astronomer/airflow-provider-great-expectations/pull/93

## 0.2.4
* [MAINTENANCE] Simplify example path manipulation with `pathlib` (#77)
* [BUGFIX] Change kwargs in Snowflake URI generation (#84)
* [FEATURE] Add data asset name guard (#85)

## 0.2.3
* [FEATURE] Add schema parameter and data_asset_name parsing (#75)
* [MAINTENANCE] Fixes `README.md` errors when building the package (#80)
* [BUGFIX] Remove unnecessary ValueError (#82)

## 0.2.2
* [BUGFIX] Fix template extension bug (#64)

## 0.2.1
* [BUGFIX] Add back in removed template fields

## 0.2.0
* [FEATURE] Enable the use of a default Checkpoint when a Checkpoint is not supplied, with the option to use the OpenLineage Validation Action
* [FEATURE] Add support to use Airflow connection information instead of separate Great Expectation Datasources.
* [FEATURE] For cloud-store Data Docs, provide a link to Data Docs from Airflow
* [MAINTENANCE] Improvements to failure logging and testing

## 0.1.5
* [MAINTENANCE] Remove upper pin for SqlAlchemy to enable use with later versions of apache-airflow

## 0.1.4
* [BUGFIX] Fix bug around the instantiation of Checkpoints from CheckpointConfig
* [DOCS] Add test and example demonstrating the use of custom expectations in an Airflow pipeline

## 0.1.3
* [FEATURE] Improve performance by moving DataContext and Checkpoint initialization to Operator.execute() - thanks @denimalpaca

## 0.1.2
* [BUGFIX] Fix error with the instantiation of a Checkpoint from a CheckpointConfig

## 0.1.1
* [BUGFIX] Resolve dependency resolution conflict with Astronomer Docker image

## 0.1.0
* [FEATURE] Update Operator to work with Great Expectations V3 API - thanks @denimalpaca @josh-fell @jeffkpayne!
* [FEATURE] Combine BigQuery Operator with general Great Expectations Operator
* [MAINTENANCE] Update required Airflow version to Airflow 2.1

## 0.0.8
* [FEATURE] Add explicit Airflow version handling - thanks, @jeffkpayne!
* [FEATURE] Add example DAG setup conveniences - thanks, @jeffkpayne!
* [FEATURE] Defer initialization of DataContext and other resources - thanks, @jeffkpayne!
* [DOCS] Update README with virtual env and testing process - thanks, @jeffkpayne!
* [MAINTENANCE] Mark additional vars templated in BigQuery operator - thanks, @jeffkpayne!
* [MAINTENANCE] Add type hints and update docstrings - thanks, @jeffkpayne!
* [MAINTENANCE] Add unit and integration test coverage - thanks, @jeffkpayne!

## 0.0.7
* [BUGFIX] Addressed a bug whereby the fail_task_on_validation_failure in the BigQuery operator was being shadowed by the parent class.
* [ENHANCEMENT] Add support for validation operators when running LegacyCheckpoints with the GreatExpectationsOperator
* [DOCS] Fixed typos in documentation - thanks @armandduijn and @sunkickr!
* [MAINTENANCE] Make some improvements to the package by updating setup.py dependencies, exposing the example_dags within that, and adding an __init__.py to the example DAG directory - thanks @petedejoy and @pgzmnk!

## 0.0.6
* [BUGFIX] Update setup.py with appropriate versions

## 0.0.5
* [BREAKING] Updated GreatExpectations operator to work with class-based Checkpoints (Great Expectations >= 0.13.9)
* [ENHANCEMENT] Restructured project, provider metadata, examples, added "how to run" steps to README, and added tests

## 0.0.4
* [ENHANCEMENT] Adding BigQuery operator for easier use with BigQuery and Google Cloud Storage

## 0.0.3
* [ENHANCEMENT] Adding template fields
* [ENHANCEMENT] Adding fail callback function parameter

## 0.0.2
* [ENHANCEMENT] Updated Great Expectations examples

## 0.0.1
* Initial release of the provider
