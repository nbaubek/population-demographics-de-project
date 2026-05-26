"""Dagster assets representing a dbt project.

This module exposes dbt models as Dagster assets, allowing the dbt project
to be orchestrated within Dagster's asset graph.
"""

from pathlib import Path

import dagster as dg
from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets


# Path to the dbt project directory (relative to this file's location)
DBT_PROJECT_PATH = Path(__file__).absolute().parent.parent.parent.parent.parent.parent / "dbt" / "population_demographics"


dbt_project = DbtProject(project_dir=str(DBT_PROJECT_PATH))
dbt_resource = DbtCliResource(project_dir=dbt_project)


class PopulationDemographicsDbtTranslator(DagsterDbtTranslator):
    """Custom translator for dbt models in the population demographics pipeline."""

    def get_group_name(self, dbt_resource_props) -> str:
        return "dbt"

    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        """Use the dbt model name as the asset key."""
        return AssetKey(dbt_resource_props["name"])

    def get_upstream_asset_key(self, dbt_resource_props) -> AssetKey | None:
        """Map dbt source references to Dagster asset keys.
        
        dbt source('gold', 'gold_states') -> AssetKey('gold_states')
        dbt source('gold', 'gold_counties') -> AssetKey('gold_counties')
        """
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            # dbt source name becomes the asset key
            return AssetKey(dbt_resource_props["name"])
        return super().get_upstream_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=(DBT_PROJECT_PATH / "target" / "manifest.json"),
    dagster_dbt_translator=PopulationDemographicsDbtTranslator(),
)
def population_demographics_dbt_assets(
    context: dg.AssetExecutionContext, dbt: DbtCliResource
):
    """Stream all dbt model results as Dagster assets."""
    yield from dbt.cli(["compile"], context=context).stream()
    yield from dbt.cli(["run"], context=context).stream()
