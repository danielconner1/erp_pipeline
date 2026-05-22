S3_BUCKET = 'erppipeline'
DATE_COLS = ["effective_date", "last_updated_at"]
S3_RAW_INCOMING_PATH = 'bronze/incoming'
S3_RAW_ARCHIVE_PATH = 'bronze/archive/'
S3_PROCESSED_PATH = 'silver/'
S3_OUTPUT_PATH = 'gold/'
S3_REGION = 'us-east-2'

TABLE_MAP = {"employee.csv": {
                                        "staging":"stg_employee",
                                        "core":"core_employee"
                                     }
}
