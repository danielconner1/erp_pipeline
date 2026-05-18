from dagster import define_asset_job, ScheduleDefinition, Definitions
from .defs.jobs import telemetry_job, telemetry_schedule
from .defs.ingest import ingest_raw_csv_to_parquet
from .defs.process import process
from .defs.features import features

defs = Definitions(assets=[ingest_raw_csv_to_parquet, process, features],
                   schedules=[telemetry_schedule],
                   jobs=[telemetry_job]
                   )
