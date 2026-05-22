from dagster import define_asset_job, ScheduleDefinition, Definitions
from .ingest import ingest_raw_employee_csv

erp_job = define_asset_job(
    name="erp_job",
    selection=[
        ingest_raw_employee_csv
    ],
)

erp_schedule = ScheduleDefinition(
    name="erp_daily",
    job=erp_job,
    cron_schedule="0 8 * * *",
)
