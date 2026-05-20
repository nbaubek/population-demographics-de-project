"""Shared resources for Census API pipeline."""

import boto3
import dagster as dg
from dagster_aws.athena import AthenaResource


class CensusAthenaResource(AthenaResource):
    """Athena resource configured for Census pipeline."""

    def __init__(self):
        client = boto3.client("athena", region_name="us-east-1")
        self.client = client
        self.workgroup = "population-demographics"
        self.polling_interval = 5
        self.max_polls = 120


@dg.resource
def athena_resource(_):
    """Athena resource for executing queries against AWS Athena."""
    return CensusAthenaResource()