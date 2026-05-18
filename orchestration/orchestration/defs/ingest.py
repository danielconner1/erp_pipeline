"""
Convert raw telemetry CSV files to Parquet format with minimal preprocessing.
Skips existing files and logs processing results.

"""

import io
import os
from dagster import asset, MaterializeResult
from datetime import datetime, timezone
from ..db_utils import insert_into_pipeline_runs_table, get_pipeline_runs_count
from ..s3_utils import (get_file_list, get_df_from_s3_csv, move_s3_bucket_file,
                      upload_to_s3, has_been_processed, is_file)

from ..config import (
    DATE_COLS,
    S3_RAW_INCOMING_PATH,
    S3_RAW_ARCHIVE_PATH,
    S3_PARQUET_PATH,
    S3_BUCKET
)


@asset()
def ingest_raw_csv_to_parquet():
    print("Starting ingest...")

    processed = 0
    skipped = 0
    failed = 0
    started = datetime.now(timezone.utc)

    s3_incoming_files = get_file_list(S3_BUCKET, S3_RAW_INCOMING_PATH)

    for csv_file_name in s3_incoming_files:
        key = csv_file_name['Key']

        #Skip if it isn't a file
        if not is_file(key):
            continue

        # Build Parquet output path from CSV filename
        file_name = key.split("/")[-1]
        parquet_file_name = f"{S3_PARQUET_PATH}{file_name.replace('.csv', '.parquet')}"
        archive_file_name = f"{S3_RAW_ARCHIVE_PATH}{file_name}"

        #Check to see if this file has been archived
        if has_been_processed(S3_BUCKET, archive_file_name):
            print(f"Skipping: {archive_file_name}")
            skipped += 1
            move_s3_bucket_file(S3_BUCKET, key, archive_file_name)
            continue

        print(f"Processing {csv_file_name['Key']}")

        try:
            df = get_df_from_s3_csv(S3_BUCKET, key)
        except Exception as e:
            print(f"Exception while reading {key}: {e}")
            continue

        # Cast date/time fields to smaller integer types (int16) since values are small,
        # reducing memory footprint without losing precision
        for col in DATE_COLS:
            df[col] = df[col].astype("int16")

        # Convert unprocessed CSV files to Parquet
        try:
            buffer = io.BytesIO()

            #puts parquet in buffer
            df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            # Uploads parquet data to S3 bucket
            upload_to_s3(S3_BUCKET, parquet_file_name, buffer)

            # Archive
            move_s3_bucket_file(S3_BUCKET, key, archive_file_name)

            processed += 1
            print(f"Saved: {parquet_file_name}")

        except Exception as e:
            failed += 1
            print(f"Failed to process parquet file: {parquet_file_name}", e)

    ended = datetime.now(timezone.utc)
    total_file_num = len(s3_incoming_files)

    conn_str = os.environ.get("POSTGRES_URL")

    if not conn_str:
        print("Postgres URL not configured")

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


