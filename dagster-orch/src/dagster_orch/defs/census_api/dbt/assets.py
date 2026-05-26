"""Dagster assets representing a dbt project.

This module exposes dbt models as Dagster assets, allowing the dbt project
to be orchestrated within Dagster's asset graph.
"""

import dagster as dg
from dagster_dbt import DbtProject

# Path to the dbt project directory (relative to this file's location)
DBT_PROJECT_PATH = "../dbt/population_demographics"


dbt_project = DbtProject(
    project_dir=DBT_PROJECT_PATH,
    raise_on_partial_data=False,
)


def get_dbt_assets():
    """Load all dbt models as Dagster assets."""
    return dbt_project.build_asset_resources(
        select=["population_demographics:*"],
        exclude=["population_demographics:*__dbt_backup"],
    )