# dagster_orch

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Idempotency

Silver and gold layers are fully **idempotent** — re-running any asset produces the same result, with no double-counting.

### Delete-before-insert pattern (Iceberg)

Iceberg tables do not support direct `INSERT OVERWRITE` the way Hive-style tables do. Instead, each materialization uses a **delete-then-re-insert** cycle:

1. **DROP** existing table if present (`DROP TABLE IF EXISTS ...`)
2. **CREATE** fresh Iceberg table with explicit schema and partitioning
3. **INSERT** all data from source (full refresh, not incremental)

This means:
- Re-running any silver or gold asset replaces the entire table atomically
- No duplicate rows — the table is always in the same state regardless of how many times it has run
- `survey_year` partitioning ensures query performance on year-filtered workloads

### Why not incremental?

ACS and TIGER data are backfilled for all years on every run. TIGER geometry changes slowly (decennial census reshapes), and ACS revisions can affect prior 5-year estimates. A full refresh keeps all years consistent with the latest available data.

### dbt models

dbt staging views sit on top of gold tables and inherit this idempotency — they query the current state of the gold layer without any conditional logic.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
