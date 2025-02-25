import unittest

import botocore.client

from gwasstudio.utils.s3 import get_s3_client


class TestGetS3Client(unittest.TestCase):
    def test_get_s3_client(self):
        cfg = {
            "vfs.s3.endpoint_override": "s3://endpoint",
            "vfs.s3.aws_access_key_id": "key",
            "vfs.s3.aws_secret_access_key": "secret",
            "vfs.s3.verify_ssl": "true",
        }
        s3_client = get_s3_client(cfg)
        self.assertIsInstance(s3_client, botocore.client.BaseClient)

    # def test_get_s3_client_no_credentials(self):
    #     cfg = {}
    #     with self.assertRaises(NoCredentialsError):
    #         get_s3_client(cfg)
