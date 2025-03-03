import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from gwasstudio.utils import parse_uri


def get_s3_client(cfg):
    """Create an S3 client with the given configuration"""
    verify = cfg.get("vfs.s3.verify_ssl").lower() == "true"
    kwargs = {
        "service_name": "s3",
        "endpoint_url": cfg.get("vfs.s3.endpoint_override"),
        "aws_access_key_id": cfg.get("vfs.s3.aws_access_key_id"),
        "aws_secret_access_key": cfg.get("vfs.s3.aws_secret_access_key"),
        "verify": verify,
    }
    return boto3.client(**kwargs)


def create_s3_bucket(bucket_name, s3):
    """
    Create an S3 bucket if it doesn't exist.

    :param bucket_name: The name of the S3 bucket to create.
    :param s3: s3 client.
    :return: True if the bucket exists or was created successfully, False otherwise.
    """
    try:
        try:
            # Check if the bucket exists
            s3.head_bucket(Bucket=bucket_name)
        except ClientError:
            # If the bucket doesn't exist, create it
            s3.create_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        print(f"S3 client error: {e}")
        return False


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
        return create_s3_bucket(bucket_name, s3) and does_path_exist(bucket_name, path, s3)
    except NoCredentialsError as e:
        raise ValueError(f"No credentials found: {e}")
