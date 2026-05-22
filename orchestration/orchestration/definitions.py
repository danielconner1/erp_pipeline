from dagster import Definitions
from .defs.jobs import erp_job, erp_schedule
from .defs.ingest import ingest_raw_employee_csv

defs = Definitions(assets=[ingest_raw_employee_csv],
                   schedules=[erp_schedule],
                   jobs=[erp_job]
                   )

