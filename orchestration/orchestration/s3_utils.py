import boto3
import io
import pandas as pd
from botocore.exceptions import ClientError

def connect_to_s3(region="us-east-2"):
    s3 = boto3.client("s3", region_name=region)
    return s3

def get_file_list(bucket,path):
    items = []
    s3 = connect_to_s3()

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=path):
        items.extend(page.get("Contents", []))

    s3.close()

    return items

def get_s3_file(bucket, filename):
    s3 = connect_to_s3()
    response = s3.get_object(
        Bucket=bucket, Key=filename
    )
    s3.close()

    return io.BytesIO(response["Body"].read())

def get_df_from_s3_csv(bucket, filename):
    csv_data = get_s3_file(bucket, filename)
    df = pd.read_csv(csv_data)
    return df

def get_df_from_s3_parquet(bucket, filename):
    csv_data = get_s3_file(bucket, filename)
    df = pd.read_parquet(csv_data)
    return df

def upload_to_s3(bucket, filename, df):
    s3 = connect_to_s3()
    s3.upload_fileobj(df, bucket, filename)
    s3.close()

def create_and_save_parquet_to_s3(bucket, filename, df):
    buffer = io.BytesIO()
    parquet_df = df.to_parquet(buffer, engine="pyarrow", index=False)
    upload_to_s3(bucket, filename, parquet_df)

def delete_s3_bucket_file(bucket, filename):
    s3 = connect_to_s3()
    s3.delete_object(Bucket=bucket, Key=filename)
    s3.close()

def copy_s3_bucket_file(bucket, old_filename, new_filename):
    s3 = connect_to_s3()
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': old_filename},
        Key=new_filename
    )
    s3.close()

def move_s3_bucket_file(bucket, old_filename, new_filename):
    copy_s3_bucket_file(bucket, old_filename, new_filename)
    delete_s3_bucket_file(bucket, old_filename)

def has_been_processed(bucket, filename):
    status = True
    try:
        get_s3_file(bucket, filename)
    except ClientError as e:
        status = False

    return status

def is_file(key, file_ext='.csv'):
    if not key.endswith('/') and key.endswith(file_ext):
        return True
    else:
        return False


