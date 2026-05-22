"""
Ingests raw CSV files with minimal preprocessing.
Logs processing results.

"""

import os
from dagster import asset, MaterializeResult
from datetime import datetime, timezone
from ..db_utils import (insert_into_pipeline_runs_table,
                        get_pipeline_runs_count,
                        insert_into_table_from_df, get_table_count)
from ..s3_utils import (get_file_list, get_df_from_s3_csv, move_s3_bucket_file,
                        has_been_processed, is_file)

from ..config import (
    S3_RAW_INCOMING_PATH,
    S3_RAW_ARCHIVE_PATH,
    S3_BUCKET,
    DATE_COLS,
    TABLE_MAP
)

def loading_stg_table(bucket, file_name, table_name, db_url, schema='erp'):
    df_s3 = get_df_from_s3_csv(bucket, file_name)
    insert_into_table_from_df(df_s3, table_name,
                              schema, db_url, 'replace')

    table_count = get_table_count(db_url, schema=schema, table_name=table_name)
    return table_count


@asset()
def ingest_raw_employee_csv():
    print("Starting ingest...")

    processed = 0
    skipped = 0
    failed = 0
    started = datetime.now(timezone.utc)

    s3_incoming_files = get_file_list(S3_BUCKET, S3_RAW_INCOMING_PATH)
    conn_str = os.environ.get("POSTGRES_URL")

    if not conn_str:
        raise Exception("POSTGRES Url not configured")

    for csv_file_name in s3_incoming_files:
        key = csv_file_name['Key']

        #Skip if it isn't a file
        if not is_file(key):
            continue

        file_name = key.split("/")[-1]
        archive_file_name = f"{S3_RAW_ARCHIVE_PATH}{file_name}"

        #Check to see if this file has been archived
        if has_been_processed(S3_BUCKET, archive_file_name):
            print(f"Skipping: {archive_file_name}")
            skipped += 1
            continue

        print(f"Processing {csv_file_name['Key']}")

        try:
            df = get_df_from_s3_csv(S3_BUCKET, key)
        except Exception as e:
            print(f"Exception while reading {key}: {e}")
            failed += 1
            continue

        # Cast date/time fields to smaller integer types (int16) since values are small,
        # reducing memory footprint without losing precision
        for col in DATE_COLS:
            if col not in df.columns:
                df[col] = df[col].astype("int16")

        try:
            table_name = TABLE_MAP[file_name]["staging"]
            rows_updated = loading_stg_table(S3_BUCKET, key, table_name,
                              conn_str, schema='erp')

            print("Staging rows updated:", rows_updated)
        except KeyError as ke:
            print(f"File {file_name} not mapped to staging table, skipping")
            skipped += 1
            continue

        # Move ingested files to archive directory
        try:

            # Archive
            move_s3_bucket_file(S3_BUCKET, key, archive_file_name)

            processed += 1
            print(f"Saved: {csv_file_name}")

        except Exception as e:
            failed += 1
            print(f"Failed to process csv file: {csv_file_name}", e)

    ended = datetime.now(timezone.utc)
    total_file_num = len(s3_incoming_files)

    table_count = get_pipeline_runs_count(conn_str)

    print("Pipeline runs count before insert:", table_count)
    print("Inserting into pipeline_runs table...")

    insert_into_pipeline_runs_table(started, ended, total_file_num, skipped, failed,
                                    "ingest", conn_str)

    table_count = get_pipeline_runs_count(conn_str)

    print("Pipeline runs count after insert:", table_count)


    # Outputs summary counts of processed and failed files
    print("SUMMARY RESULTS")
    print(f"Processed: {processed}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")

    return MaterializeResult(
        metadata={
            "processed": processed,
            "skipped": skipped,
            "failed": failed
        }
    )
