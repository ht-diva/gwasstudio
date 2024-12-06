import urllib.parse

import boto3
from botocore.exceptions import NoCredentialsError


def parse_uri(uri):
    try:
        parsed = urllib.parse.urlparse(uri)
        scheme, netloc, path = parsed.scheme, parsed.netloc, parsed.path.strip("/")
        return scheme, netloc, path
    except ValueError as e:
        raise ValueError(f"Invalid URI: {uri}") from e


def does_uri_path_exist(uri):
    def does_path_exist(bucket_name, path):
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path, Delimiter="/")
            return response["KeyCount"] == 1
        except Exception as e:
            raise ValueError(f"Error: {e}")

    # Initialize the S3 client with your AWS credentials
    s3 = boto3.client("s3", verify=False)

    scheme, bucket_name, path = parse_uri(uri)
    if not bucket_name or not path:
        return None
    try:
        # Check if the object exists in the bucket
        return s3.head_bucket(Bucket=bucket_name) and does_path_exist(bucket_name, path)
    except NoCredentialsError as e:
        raise ValueError(f"No credentials found: {e}")
