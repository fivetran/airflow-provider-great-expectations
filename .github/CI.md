# CI guide

How continuous integration works in this repository, and what a maintainer does when it needs a person. For running tests while developing, see the [contributing guide](../docs/contributing/contributing-guide.md). For cutting a release, see [RELEASE.md](../RELEASE.md). For credentials and environments, see [CI_SECRETS.md](../CI_SECRETS.md).

## Workflows

| Workflow | Runs on | Blocks a merge? | What it proves |
| --- | --- | --- | --- |
| `tests.yml` | pull request into `main`, push to `main`, weekly, manual | Yes: `tests-passed` | The declared ranges and docs agree with the support matrix; every extra resolves to its drivers; unit tests, the example DAGs and integration tests pass on every lane; the Postgres, Spark and Spark Connect tests pass; the latest and lowest-direct canaries pass. |
| `lint.yml` | pull request into `main`, push to `main`, manual | Yes: `lint-passed` | The code type-checks once per Airflow compatibility tier, and ruff format, ruff check and the CI scripts' type check are clean. |
| `zizmor.yml` | pull request, push to `main` | Yes: `zizmor` | zizmor finds nothing to report in any workflow. |
| `canaries.yml` | weekly, manual, pull request that changes the workflow, the CI scripts or the matrix (path filter) | No: nothing aggregates or requires it | The provider still installs and passes its unit tests and example DAGs on the newest Airflow pre-release and, once Airflow and Great Expectations both declare it, on the next Python. |
| `upstream-drift.yml` | weekly, manual, pull request that changes `pyproject.toml`, the matrix, the drift scripts or the workflow itself (path filter) | No: not a required check | The declared floors match the reference provider, the matrix lists the newest Airflow releases, and no supported-Python gap is open. |
| `report-scheduled-failure.yml` | called by `tests.yml`, `canaries.yml` and `upstream-drift.yml` after a failed scheduled run | No: it only files the tracking issue | Nothing; it opens or updates the tracking issue for a failed scheduled run. |
| `ci.yaml` | pull request into `main` (docs build), push to `main` (docs deploy), release published (docs deploy and PyPI publish) | No: `Build-Docs` is not part of either gate | The docs build; on a push or a published release, the docs deploy to `gh-pages` and the package publishes to PyPI. |
| `codeql.yml` | pull request into `main`, push to `main`, weekly | No: not a required check | CodeQL analysis of the Python code and the workflow definitions. |
| `slack-notifications.yml` | issue opened, pull request opened (by an author without write access) | No: it never reports a check on code | New issues and outside contributors' pull requests are announced in Slack. |

How to read the table:

- **Blocking** means a required status check. Three checks are required on `main`: `tests-passed`, `lint-passed` and `zizmor`. Each `-passed` check is a gate job that runs even when a job it needs failed, was cancelled or was skipped, and fails unless every one of them succeeded. The jobs behind a gate change names with the matrix, so require the gates and not the jobs. If you rename a gate or the `zizmor` job, update the required checks to match.
- **Fork pull requests** run the same workflows under GitHub's fork approval policy. Every job that runs pull request code holds a read-only token and uses no secrets and no environment, so the workflows run unchanged for a fork.
- **Scheduled runs** happen on the default branch whether or not a commit has landed. When a scheduled run of `tests.yml`, `canaries.yml` or `upstream-drift.yml` fails, the run opens or updates a tracking issue (see [Responding to a failure](#responding-to-a-failure)).
- **Hygiene**: every workflow starts with no token permissions and each job adds only what it uses; every action is pinned to a full commit SHA with its release in a comment, and Dependabot proposes the updates each week; every job has a timeout.
- **The jobs in `tests.yml`**: `matrix`, `consistency`, `extras`, `unit`, `integration`, `postgres`, `spark`, `spark-connect`, `canary-latest`, `canary-lowest-direct`, and the gate `tests-passed`. `lint.yml` runs `matrix`, `typecheck`, `ruff` and `lint-passed`. Matrix jobs name their lane, for example `unit (Airflow 3.3.2, Python 3.11)`, and never cancel one another.

## The support matrix

`.github/support-matrix.toml` is the one place that says which Airflow releases, Python versions and service images CI covers. The workflows read it, so they name no release, and a change to the file changes what runs without any workflow edit. Loading the file validates it and names the field of any entry that is wrong.

| Field | Meaning |
| --- | --- |
| `python.supported` | The Python versions the provider supports, lowest first. |
| `python.next` | The Python after the highest supported one. Only the canaries and the drift check use it. |
| `[[airflow]]` | One entry per Airflow line, oldest first: `line`, `release` (the exact release CI installs), `tier` (see `[tiers.*]`) and `python` (the Pythons that line supports). |
| `[tiers.*]` | The compatibility flags the type checker treats as always true or always false for the lines in that tier, so each tier is type-checked the way it runs. |
| `[install]` | The template for Airflow's constraints file and the pinned `uv` version. |
| `[services.postgres]`, `[services.spark]` | The service image (tag and digest), the lanes it runs on (`floor`, `newest`) and, for Spark, the PySpark version, which must match the image. |
| `[extras]` | For each extra the provider declares, the driver packages that must resolve. |
| `[policy]` | `reference_provider`: the Airflow provider whose declared floors the drift check compares against. |

A **lane** is one Airflow release on one Python. There is a lane for every `[[airflow]]` entry on the lowest supported Python (the first is the `floor` lane and the last is the `newest` lane) and a lane for every other supported Python on the newest Airflow line. `python3 .github/scripts/support_matrix.py emit` prints the lanes and every other value the workflows read.

**Changing the matrix**

- *Move a tested release forward*: edit the line's `release`, then regenerate the table.
- *Add an Airflow line*: append an `[[airflow]]` entry with its `tier` (add a `[tiers.*]` table if the line changes which compatibility flags apply) and the Pythons it supports, then regenerate the table. Its lanes and its type-check run appear with no workflow edit.
- *Add a Python*: add it to `python.supported`, to the `python` list of every Airflow line that supports it, and, when it was `python.next`, set a new `next`. Then update the classifiers in `pyproject.toml` and regenerate the table.
- *Drop a line or a Python*: remove it from the same places, and update `pyproject.toml` to match.
- Then open a pull request. The `consistency` job lists anything that still disagrees.

**Regenerating the table.** The compatibility table in `.github/support-matrix.md` is generated, so never edit it by hand:

```bash
python3 .github/scripts/support_matrix.py render --write   # rewrite the table
python3 .github/scripts/support_matrix.py render --check   # exit 1 if it is stale
```

A tracked Markdown file can show the same table between a pair of HTML comments named `support-matrix:begin` and `support-matrix:end`. The `consistency` job requires that block to equal the generated table, so paste the regenerated table in.

## Responding to a failure

Start from the failed check's log and job summary; each failure names the file and the value that disagrees.

- **`consistency`** (`support_matrix.py check`) prints one line per disagreement with the rule, the file, the expected and actual values and a hint. C1: the `apache-airflow` lower bound in `pyproject.toml` is not the first matrix line. C2: the requirement has a `!=` exclusion or does not admit every tested release. C3: `requires-python` has the wrong lower bound or any upper bound. C4: the Python classifiers differ from `python.supported`. C5: `pyproject.toml` has a missing or invalid field. C6: the generated table, or a block that copies it, is stale; regenerate it. C7: the Spark image default in `docker/spark/docker-compose.yml` differs from `services.spark.image`. Fix the file the line names, or the matrix when the matrix is what changed. Reproduce with `uv run --no-project --with-requirements .github/scripts/requirements.txt python .github/scripts/support_matrix.py check`.
- **`extras`** (`python3 .github/scripts/check_extras.py`) resolves every extra on every lane and prints a table of lanes by extras. A cell fails when a listed driver is missing from the resolution, or when an extra resolves to a Great Expectations extra that does not exist. An extra with no `[extras]` entry fails by name: add its drivers to the matrix. A driver missing on one lane is a real gap in that lane's constraints, so decide between changing the extra and narrowing the lane.
- **A lane's install step** fails when the lane's constraints and the provider's requirements cannot both be satisfied, or when Airflow publishes no constraints file for that release and Python. The installer then names the missing file and never falls back to an unconstrained install. Fix the requirement, or drop that Python from the Airflow line's `python` list when the line does not support it.
- **A lane's tests or example DAGs** fail on one Airflow line or one Python: reproduce on that lane (see [Running a lane locally](#running-a-lane-locally)), and fix the code. Do not skip or deselect a test to get green.
- **`typecheck`** fails for one tier: the check name carries the tier and its Airflow line. The job's log shows the exact `mypy` flags; the tier's `[tiers.*]` table is where those come from.
- **`canary-latest` and `canary-lowest-direct`** block. A red `canary-latest` means a new upstream release broke the provider: fix the code or add an upper bound. A red `canary-lowest-direct` means a declared lower bound is lower than what really works: raise it in `pyproject.toml`.
- **`zizmor`** prints each finding in its log. Fix the workflow; if a suppression is justified, write `# zizmor: ignore[rule]` on the line with the reason after it.
- **Upstream drift** prints findings numbered by rule.
  - D1: the provider's `apache-airflow` or `requires-python` floor differs from the reference provider's latest release. Align `pyproject.toml`, then the matrix, then the docs the `consistency` job names.
  - D2: a line's `release` is behind a final release on that line, or a newer minor is available under the provider's `apache-airflow` upper bound. Update the release, or add the line.
  - D3: the newest Airflow and Great Expectations both declare `python.next`, but the matrix does not support it. Add it as in "Changing the matrix".
  - An exit status of 2 means PyPI could not be read; re-run the job.
- **A tracking issue** titled `Scheduled CI failure: <workflow name>` is opened by the first failed scheduled run of `tests.yml`, `canaries.yml` or `upstream-drift.yml`. While it is open, later failures of the same workflow add a comment with the run link instead of a new issue. Follow the link, fix the cause, and close the issue yourself: nothing closes it. A pull request, a push or a manual run never opens one.

## Canaries

Two canaries sit inside `tests.yml` and block through `tests-passed`:

- `canary-latest` installs the newest release of every dependency, with no constraints, on the highest supported Python, then runs the unit tests and the example DAGs.
- `canary-lowest-direct` resolves the provider's direct dependencies to the lowest versions that `pyproject.toml` admits, under the matching Airflow constraints, then runs the unit tests. It runs at the declared Airflow floor on the lowest Python, and on the oldest Airflow line that supports the highest Python.

Two more run in `canaries.yml` and look ahead of the matrix:

- **Pre-release canary**: installs the newest Airflow pre-release (or the newest tested release when there is none), and the newest Great Expectations pre-release only when it is newer than the newest final release. Pre-releases install only where they are named, never wholesale.
- **Next-Python canary**: installs the provider on `python.next` once the newest Airflow and Great Expectations both declare it. Until then the job installs nothing and succeeds, and its summary lists the packages that do not declare the Python yet.

**Non-blocking** means none of the following: no job aggregates them, no check is required, and no job uses `continue-on-error`. A failing canary turns its own run red and, on a scheduled run, opens or updates a tracking issue, and it never holds a pull request. Read a red canary as early warning: decide whether the upstream release or the provider is wrong, and fix whichever it is before the release becomes final. A pull request runs `canaries.yml` only when it changes the workflow, the CI scripts or the matrix.

## Running a lane locally

Use a fresh virtual environment per lane, on the lane's Python (3.11 or newer runs the CI scripts). `lane_install.py` installs into the active interpreter and needs `uv` on `PATH`. Install the `uv` version that `[install].uv` in the matrix pins, so the lane matches CI.

```bash
python3 .github/scripts/support_matrix.py emit          # shows each lane's airflow, python and constraints_url
python3.11 -m venv /tmp/lane-venv && . /tmp/lane-venv/bin/activate
uv_version=$(python -c 'import tomllib; print(tomllib.load(open(".github/support-matrix.toml", "rb"))["install"]["uv"])')
python -m pip install "uv==$uv_version"
python .github/scripts/lane_install.py --mode constrained \
  --airflow 2.11.2 --python 3.11 \
  --constraints-url https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt \
  --extras tests
pytest -m unit tests -ra
.github/scripts/run_example_dags.sh
pytest -m integration tests -ra
```

The installer prints a version report (Python, uv, Airflow and its core packages, Great Expectations, SQLAlchemy, pandas, pytest) and the developer-tool pins it removed from the constraints. The other modes take the same arguments:

| Mode | Installs |
| --- | --- |
| `constrained` | One Airflow release under its constraints file. |
| `latest` | The provider's own requirements, newest releases, no constraints. |
| `lowest-direct` | As `constrained`, with direct dependencies at their lowest admitted versions; `--airflow floor` uses the declared lower bound. |
| `prerelease` | As `constrained`, with the target's own pre-release pins; a missing constraints file is noted, not fatal. |
| `next-python` | As `latest`, for `python.next`. |
| `tools` | Only the pinned lint tools and the CI scripts' requirements. |

For the type check, install the `lint` extra the same way and pass the tier's flags that `emit` prints as `typecheck`. The Postgres, Spark and Spark Connect jobs need a service; the contributing guide shows how to start one locally.

## Test tier markers and the collection guard

Every test carries at least one of the tier markers that `pytest.ini` registers: `unit`, `integration`, `postgres`, `spark_integration` and `spark_connect_integration`. Each CI job selects one tier with `-m`, so a test with no tier would never run in CI. The collection guard in `tests/conftest.py` closes that gap: it runs before `-m`, `-k` or `--deselect` filter anything and fails collection, listing every offending test, when a collected test has none of the five markers. Other markers, such as `parametrize`, do not count. `pytest.ini` also sets `--strict-markers`, so an unregistered marker is an error. A new tier needs a marker registered in `pytest.ini`, an entry in `TIER_MARKERS` and a CI job that selects it.

## Repository settings checklist

These settings live in GitHub, not in the repository, so code cannot change them. A maintainer works through the list; nothing here runs automatically.

- [ ] Require the checks `tests-passed`, `lint-passed` and `zizmor` on `main`, once all three are green there. `Build-Docs` (in `ci.yaml`) is not part of either gate; require it as well only if a docs build failure should block merging.
- [ ] Confirm Dependabot accepted `.github/dependabot.yml` (Insights, Dependency graph, Dependabot) and that its first update pull requests for the actions and the CI scripts' requirements open.
- [ ] Delete the `internal` and `external` environments, or re-scope them, now that no workflow references them. The two environments that remain in use are `pypi` and `notifications`.
- [ ] Check that the PyPI trusted publisher names this repository, the `ci.yaml` workflow and the `pypi` environment, and that the `pypi` environment carries the protection you want on publishing.
- [ ] Check that GitHub Pages serves the `gh-pages` branch.
- [ ] Choose the fork-PR approval policy (Settings, Actions, General: the approval setting for pull request workflows from forks). CI asks for no environment approval, so this setting is the approval gate for outside contributors. Every job that runs pull request code holds a read-only token and no secrets.
- [ ] Enable the Actions policy that requires actions to be pinned to a full-length commit SHA. Every `uses:` in the workflows already is.
- [ ] Check that the CLA bot and pre-commit.ci are installed on this repository and respond on a new pull request. Reinstall under the current organization if one does not.
- [ ] Delete repository and environment secrets and variables that no workflow references. List them with `gh secret list`, `gh variable list` and the `--env <name>` form of each, and compare with `secrets.` and `vars.` in `.github/workflows/`.
- [ ] Dismiss the stale code-scanning alerts that earlier zizmor runs uploaded (Security, Code scanning, filter on the zizmor tool). The zizmor job no longer uploads results, so those alerts will not close themselves. CodeQL's own alerts stay.
- [ ] Confirm the Slack bot token is set in the `notifications` environment and the bot is a member of the target channel. Verify the first post by opening an issue, or delete `slack-notifications.yml` and the `notifications` environment if you do not want Slack delivery.
- [ ] Watch the next published release run `ci.yaml` end to end: the release docs deploy and the PyPI publish run only on a published release, never on a saved draft. Every push to `main` already deploys the development docs.
- [ ] Re-enable scheduled workflows after 60 idle days. GitHub disables schedules in a public repository with no activity for 60 days; if the weekly runs stop, re-enable each scheduled workflow under Actions and run it once by hand.
