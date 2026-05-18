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
    return MaterializeResult(
        metadata={
            "processed": 0,
            "skipped": 0,
            "failed": 0
        }
    )






