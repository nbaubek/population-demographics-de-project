from pathlib import Path

import dagster as dg
from dagster import definitions, load_from_defs_folder
from dagster_orch.defs.census_api.shared.resources import athena_resource


@definitions
def defs():
    base_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return base_defs.with_resources({"athena": athena_resource})