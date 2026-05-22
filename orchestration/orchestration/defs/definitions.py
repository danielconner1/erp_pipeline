from dagster import Definitions
from .jobs import erp_job, erp_schedule
from .ingest import ingest_raw_employee_csv

defs = Definitions(assets=[ingest_raw_employee_csv],
                   schedules=[erp_schedule],
                   jobs=[erp_job]
                   )

