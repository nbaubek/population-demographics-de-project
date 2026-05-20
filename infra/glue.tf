resource "aws_glue_catalog_database" "iceberg_bronze" {
  name        = "population_demographics_bronze"
  description = "Bronze layer - raw ACS and TIGER data"
}

resource "aws_glue_catalog_database" "iceberg_silver" {
  name        = "population_demographics_silver"
  description = "Silver layer - cleaned and joined data"
}

resource "aws_glue_catalog_database" "iceberg_gold" {
  name        = "population_demographics_gold"
  description = "Gold layer - socioeconomic models"
}