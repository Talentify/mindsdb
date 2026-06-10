import unittest
from unittest.mock import MagicMock

from botocore.client import ClientError

from mindsdb.integrations.handlers.s3vectors_handler.s3vectors_handler import S3VectorsHandler
from mindsdb.integrations.libs.vectordatabase_handler import TableField


class TestS3VectorsHandler(unittest.TestCase):
    def setUp(self):
        self.handler = S3VectorsHandler(
            "s3vectors",
            connection_data={"vector_bucket": "test-bucket"},
        )
        self.connection = MagicMock()
        self.handler.connection = self.connection
        self.handler.is_connected = True

    def test_get_dimension_returns_index_dimension(self):
        self.connection.get_index.return_value = {
            "index": {
                "dimension": 3072,
            },
        }

        dimension = self.handler.get_dimension("test-index")

        self.assertEqual(dimension, 3072)
        self.connection.get_index.assert_called_once_with(
            vectorBucketName="test-bucket",
            indexName="test-index",
        )

    def test_create_table_ignores_existing_index_conflict_when_if_not_exists(self):
        self.connection.create_index.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "ConflictException",
                    "Message": "An index with the specified name already exists",
                },
            },
            operation_name="CreateIndex",
        )

        self.handler.create_table("test-index")

        self.connection.create_index.assert_called_once_with(
            vectorBucketName="test-bucket",
            indexName="test-index",
            dataType="float32",
            dimension=1536,
            distanceMetric="cosine",
        )

    def test_select_without_search_vector_or_id_still_fails(self):
        with self.assertRaisesRegex(Exception, "search_vector or an ID"):
            self.handler.select(
                "test-index",
                columns=[TableField.EMBEDDINGS.value],
                conditions=[],
                limit=1,
            )
