import os
import boto3
from botocore.exceptions import ClientError

# Bucket name comes from .env (passed in by docker-compose)
s3_bucket_name = os.environ["S3_BUCKET_NAME"]
warehouse_dir = f"s3a://{s3_bucket_name}/pyspark-dw-aws/warehouse/"

landing_prefix = "pyspark-dw-aws/landing"
archive_prefix = "pyspark-dw-aws/archive"
landing_extensions = ("csv", "json")

s3_client = boto3.client("s3")

# archive landing objects
def archive_landing_object(filename: str, dataset: str) -> bool:
    source_key = f"{landing_prefix}/{dataset}/{filename}"
    target_key = f"{archive_prefix}/{dataset}/{filename}"

    try:
        s3_client.copy_object(
            Bucket=s3_bucket_name,
            Key=target_key,
            CopySource={"Bucket": s3_bucket_name, "Key": source_key},
        )
        s3_client.delete_object(Bucket=s3_bucket_name, Key=source_key)
        return True

    except Exception as e:
        print(e)
        return False

# check if s3 landing file exists
def _s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise   # permission or other errors should not look like "file missing"

def s3_landing_file_exists(filename: str) -> bool:
    name = filename.strip()
    for ext in landing_extensions:
        if name.lower().endswith(f".{ext}"):
            name = name[: -(len(ext) + 1)]
            break

    subfolder = name.split("_")[0]
    s3 = boto3.client("s3")

    return any(
        _s3_key_exists(s3, s3_bucket_name, f"{landing_prefix}/{subfolder}/{name}.{ext}")
        for ext in landing_extensions
    )