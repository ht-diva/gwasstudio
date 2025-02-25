import boto3
from botocore.exceptions import NoCredentialsError

from gwasstudio.utils import parse_uri


def get_s3_client(cfg):
    """Create an S3 client with the given configuration"""
    kwargs = {
        "service_name": "s3",
        "endpoint_url": cfg.get("vfs.s3.endpoint_override"),
        "aws_access_key_id": cfg.get("vfs.s3.aws_access_key_id"),
        "aws_secret_access_key": cfg.get("vfs.s3.aws_secret_access_key"),
        "verify": cfg.get("aws_verify_ssl"),
    }
    return boto3.client(**kwargs)


def does_path_exist(bucket_name, path, s3):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path, Delimiter="/")
        return response["KeyCount"] == 1
    except Exception as e:
        raise ValueError(f"Error: {e}")


def does_uri_path_exist(uri, cfg):
    scheme, bucket_name, path = parse_uri(uri)
    if not bucket_name or not path:
        return None

    # Initialize the S3 client with your AWS credentials
    s3 = get_s3_client(cfg)

    try:
        # Check if the object exists in the bucket
        return s3.head_bucket(Bucket=bucket_name) and does_path_exist(bucket_name, path, s3)
    except NoCredentialsError as e:
        raise ValueError(f"No credentials found: {e}")
