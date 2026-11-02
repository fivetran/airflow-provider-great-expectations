#!/usr/bin/env bash
# Smoke-test the shipped example DAGs with the Airflow CLI that is on PATH.
#
# Works on Airflow 2.11 and 3.x. Runs, in order, and ends with a non-zero exit
# when any step fails:
#
#   1. Use an isolated Airflow home, with the example-DAG folder as the DAGs
#      folder and Airflow's bundled examples off.
#   2. Reset and migrate the metadata database.
#   3. Parse the folder.
#   4. Fail on any import error, or when no DAG loaded.
#   5. Run `airflow dags test <dag_id> 2024-01-01` for every loaded DAG.
#
# The lane's Python environment, with the provider installed, must already be
# active. The script can be run from any directory.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dags_folder="$root/great_expectations_provider/example_dags"
logical_date="2024-01-01"

if ! command -v airflow > /dev/null; then
  echo "error: the airflow command is not on PATH; activate the lane's environment first" >&2
  exit 1
fi
if [ ! -d "$dags_folder" ]; then
  echo "error: example DAG folder not found: $dags_folder" >&2
  exit 1
fi

# 1. Isolated home. RUNNER_TEMP is unset outside GitHub Actions, so fall back to
#    the system temporary directory. The database always lives inside the home:
#    the reset below must never reach a database configured in the environment.
airflow_home="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/airflow-home.XXXXXX")"
trap 'rm -rf "$airflow_home"' EXIT
unset AIRFLOW_CONFIG AIRFLOW__CORE__SQL_ALCHEMY_CONN
export AIRFLOW_HOME="$airflow_home"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$airflow_home/airflow.db"
export AIRFLOW__CORE__DAGS_FOLDER="$dags_folder"
export AIRFLOW__CORE__LOAD_EXAMPLES=false

# 2. Reset and migrate the metadata database.
echo "== Resetting and migrating the metadata database"
airflow db reset --yes
airflow db migrate

# 3. Parse the folder. `airflow dags reserialize` is the one parse command that
#    works on both lines: on 3.x, `dags list` and `dags list-import-errors` read
#    the metadata database, which stays empty until a parse fills it. Neither
#    `reserialize` nor `dags list` exits non-zero on an import error.
echo "== Parsing $dags_folder"
airflow dags reserialize

# 4. `dags list-import-errors` prints every import error and exits non-zero when
#    there is at least one; it prints "No data found" and exits 0 otherwise.
echo "== Checking for import errors"
if ! airflow dags list-import-errors; then
  echo "error: example DAGs failed to import (see the errors above)" >&2
  exit 1
fi

# `-o json` prints the DAGs as a JSON array on one line, next to log lines that vary
# by version. A DAG id can't hold a quote, so matching `"dag_id": "..."` is safe.
dag_ids="$(airflow dags list -o json | { grep -o '"dag_id": "[^"]*"' || true; } | sed 's/^"dag_id": "\(.*\)"$/\1/')"
if [ -z "$dag_ids" ]; then
  echo "error: no DAGs loaded from $dags_folder" >&2
  exit 1
fi
echo "== Loaded $(printf '%s\n' "$dag_ids" | wc -l | tr -d ' ') DAG(s):"
printf '%s\n' "$dag_ids" | sed 's/^/  /'

# 5. Test every loaded DAG, so one failure doesn't hide another.
failed=""
for dag_id in $dag_ids; do
  echo "== airflow dags test $dag_id $logical_date"
  if ! airflow dags test "$dag_id" "$logical_date"; then
    failed="$failed $dag_id"
  fi
done
if [ -n "$failed" ]; then
  echo "error: airflow dags test failed for:$failed" >&2
  exit 1
fi
echo "== All example DAGs passed"
