# Compatibility and support policy

This page lists the Apache Airflow, Python and Great Expectations (GX) versions that the provider supports, how to install it so that you get a combination that works, and the rules that decide how that support changes over time.

## Supported versions

The table lists the Airflow releases and Python versions that the provider's automated tests run against.

--8<-- ".github/support-matrix.md"

- **Apache Airflow**: 2.11.0 and later, below 4.
- **Great Expectations**: `>=1.23.2,<2`, and `>=1.24.0` on Python 3.14 and later.
- **Python**: 3.11 and later, as each Airflow line allows (see the last column of the table).

## Install with constraints

Install the provider the way Airflow itself recommends: pin `apache-airflow==<version>` in the same command that installs the provider, and use Airflow's constraints file. See [Airflow's installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files) for background.

The constraints file for a release is published at this URL pattern:

```text
https://raw.githubusercontent.com/apache/airflow/constraints-<airflow version>/constraints-<python version>.txt
```

`<python version>` is the major and minor version only, such as `3.11`.

**New environment.** Install Airflow and the provider in one command, with the constraints file. Replace `<Airflow version>` with an Airflow release whose entry in the table's last column ("Python supported by this Airflow line") includes your Python version, because Airflow publishes constraints files only for those combinations:

```bash
AIRFLOW_VERSION="<Airflow version>"
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
pip install "apache-airflow==${AIRFLOW_VERSION}" "airflow-provider-great-expectations" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Optional extras go in the same command, for example `"airflow-provider-great-expectations[snowflake]"`.

**Existing environment.** Add the provider with `apache-airflow==<installed version>` in the same command, so that the resolver cannot change your Airflow version. This is also Airflow's own advice for adding packages later. Pass the constraints file as well, as above, so that the packages Airflow depends on stay at the versions that release was tested with.

```bash
pip install "apache-airflow==<installed version>" "airflow-provider-great-expectations" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-<installed version>/constraints-<python version>.txt"
```

### When the constraints file is required

On two groups of Airflow releases, an install without the constraints file produces a broken environment. Pinning Airflow alone is not enough.

- **Airflow 2.11.x.** Without constraints, the resolver installs pandas 3 next to SQLAlchemy 1.4, which is the SQLAlchemy line that Airflow 2.11 requires. pandas 3 needs SQLAlchemy 2, so pandas' SQL reads through a SQLAlchemy connection fail.
- **Airflow 3.1.0 and 3.1.1.** Without constraints, the resolver installs structlog 25.5 or later. Those two Airflow releases import a name that structlog removed in 25.5, so Airflow fails with an import error. The constraints file pins structlog 25.4.0.

Airflow's constraints files don't pin Great Expectations, so the provider's own GX range, listed under [Supported versions](#supported-versions), applies.

## Older Airflow and Python versions

If you run Airflow 2.10 or older, or Python 3.10, stay on provider 1.0.0. Provider 2.0 and later require Airflow 2.11 or later and Python 3.11 or later. When you pin or constrain Airflow as described above, the resolver picks 1.0.0 for you. To select it explicitly:

```bash
pip install "apache-airflow==<installed version>" "airflow-provider-great-expectations==1.0.0"
```

The 1.x line receives no further fixes.

## Support policy

- The supported Airflow and Python ranges match the official Apache Airflow providers'. The reference is the `apache-airflow` requirement and the `Requires-Python` of the latest `apache-airflow-providers-common-sql` release.
- When the official providers raise a floor, this provider's next release raises it too, as a minor release. Floors never move ahead of the official providers.
- No Airflow version inside the range is excluded. Upstream packaging problems that the official constraints files already avoid are handled by the install guidance above.
- Airflow is capped below 4 until an Airflow 4 pre-release has been tested.
- Python has no upper cap. A Python version is supported once Great Expectations and Airflow both support it and the provider's tests pass on it. The table above lists the Python versions that are tested.
- The tests cover every Airflow minor release from the floor to the latest release, each at its latest patch release, on the lowest supported Python. They also cover every supported Python on the latest Airflow. Every environment is installed with the official constraints files. A weekly check flags any drift from the official providers' floors.
- Only the latest release line receives fixes. Earlier major lines are frozen.
- The Great Expectations range is set on technical grounds and proven by a test job that installs the lowest supported versions. This policy does not govern it.
