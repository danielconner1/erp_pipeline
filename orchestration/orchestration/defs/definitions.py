from dagster import define_asset_job, ScheduleDefinition, Definitions
from .jobs import telemetry_job, telemetry_schedule
from .ingest import ingest_raw_employee_csv
from .process import process
from .features import features

defs = Definitions(assets=[ingest_raw_csv_to_parquet, process, features],
                   schedules=[telemetry_schedule],
                   jobs=[telemetry_job]
                   )

