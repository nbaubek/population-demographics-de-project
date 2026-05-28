# Project-specific commands for DemographIQ

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------

# Install all dependencies in both root and dagster-orch environments
sync:
    uv sync
    cd dagster-orch && uv sync

# Lint and type-check
check:
    ruff check .
    ruff format --check .
    mypy .

# -----------------------------------------------------------------------------
# AWS Infrastructure (Terraform)
# -----------------------------------------------------------------------------

# Preview infrastructure changes
infra-plan:
    cd infra && terraform plan

# Apply infrastructure changes
infra-apply:
    cd infra && terraform apply

# Destroy all infrastructure (careful!)
infra-destroy:
    cd infra && terraform destroy

# -----------------------------------------------------------------------------
# Dagster
# -----------------------------------------------------------------------------

# Start Dagster UI locally
dag-start:
    cd dagster-orch && source .venv/bin/activate && uv run dg dev

# Run dbt models against live Athena
dbt-build:
    cd dagster-orch && source .venv/bin/activate && uv run dbt run

# Run dbt tests
dbt-test:
    cd dagster-orch && source .venv/bin/activate && uv run dbt test

# Run specific dbt model
dbt-run MODEL:
    cd dagster-orch && source .venv/bin/activate && uv run dbt run --select {{MODEL}}

# -----------------------------------------------------------------------------
# Dashboard
# -----------------------------------------------------------------------------

# Start Flask dashboard
dashboard:
    cd dagster-orch && source .venv/bin/activate && uv run flask --app dashboard.app run --debug

# -----------------------------------------------------------------------------
# Dagster definitions check
# -----------------------------------------------------------------------------

# Validate project configuration and definitions
dag-check:
    cd dagster-orch && source .venv/bin/activate && uv run dg check