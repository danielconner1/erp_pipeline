from dagster import define_asset_job, ScheduleDefinition, Definitions
from .defs.ingest import ingest_raw_csv_to_parquet

defs = Definitions(assets=[ingest_raw_csv_to_parquet]
                   )
