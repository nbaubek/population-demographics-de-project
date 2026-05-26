from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource
from dagster_orch.defs.census_api.dbt import population_demographics_dbt_assets, dbt_project
from dagster_orch.defs.census_api.shared.resources import athena_resource


defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        assets=[population_demographics_dbt_assets],
        resources={
            "dbt": DbtCliResource(project_dir=dbt_project),
            "athena": athena_resource,
        },
    ),
)
