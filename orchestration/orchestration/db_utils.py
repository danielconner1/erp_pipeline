from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from typing import Literal

def insert_into_table_from_df(table_df: pd.DataFrame, table_name:str,
                              schema:str, db_url:str,
                              if_exists:Literal["fail", "replace", "append"] = 'append'):
    engine = create_engine(db_url)
    table_df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)

def get_pipeline_runs_count( db_url:str):
    engine = create_engine(db_url)
    count = engine.connect().execute(text("""
        SELECT COUNT(*)
        FROM erp.pipeline_runs
    """)).scalar()

    return count

def get_table_count( db_url:str, schema:str, table_name:str):
    engine = create_engine(db_url)
    count_query = text(f"SELECT COUNT(*) FROM {schema}.{table_name}")

    with engine.connect() as conn:
        count = conn.execute(count_query).scalar()

    return count

def insert_into_pipeline_runs_table(started: datetime, completed_at: datetime,
                                    files_processed: int, files_skipped: int,
                                    files_failed: int, stage: str, conn_str:str,
                                    schema:str):
    log_df = pd.DataFrame([{
        "stage": stage,
        "status": "processed",
        "files_processed": files_processed,
        "files_skipped": files_skipped,
        "files_failed": files_failed,
        "started_at": started,
        "completed_at": completed_at,
    }])

    insert_into_table_from_df(log_df, "pipeline_runs",
                              schema, conn_str, 'append')

