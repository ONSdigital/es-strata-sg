import json
import os
import sys
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_sns, mock_sqs
from pandas.util.testing import assert_frame_equal

import strata_period_method  # noqa E402
import strata_period_wrangler  # noqa E402

# docker issue means that this line has to be placed here.
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))


class TestStrata(unittest.TestCase):
    """
    Class testing the strata wrangler and Method.
    """

    @classmethod
    def setup_class(cls):
        """
        sets up the mock boto clients and starts the patchers.
        :return: None.

        """
        cls.mock_os_wrangler_patcher = mock.patch.dict(
            "os.environ",
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": "mock-url",
            },
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()

        cls.mock_os_method_patcher = mock.patch.dict(
            "os.environ",
            {
                "queue_url": "queue_url",
                "strata_column": "strata",
                "value_column": "Q608_total",
            },
        )
        cls.mock_os_m = cls.mock_os_method_patcher.start()

    @classmethod
    def teardown_class(cls):
        """
        stops the wrangler, method and os patchers.
        :return: None.

        """
        cls.mock_os_wrangler_patcher.stop()
        cls.mock_os_method_patcher.stop()

    def test_method(self):
        """
        mocks functionality of the method.

        :return: None
        """
        with mock.patch.dict(
            strata_period_method.os.environ,
            {"strata_column": "strata", "value_column": "Q608_total"},
        ):
            with open("tests/fixtures/strata_in.json") as file:
                json_content = json.load(file)
                
            actual_output = strata_period_method.lambda_handler(json_content, None)
            actual_output_dataframe = pd.DataFrame(actual_output)

            with open("tests/fixtures/strata_out.json") as file:
                expected_method_output = json.load(file)
                expected_output_dataframe = pd.DataFrame(expected_method_output)

            assert_frame_equal(actual_output_dataframe, expected_output_dataframe)

    @mock_sns
    def test_sns_messages(self):
        """
        Test sending sns messages to the queue.
        :return: None.
        """
        with mock.patch.dict(strata_period_wrangler.os.environ, {"arn": "test_arn"}):
            sns = boto3.client("sns", region_name="eu-west-2")
            topic = sns.create_topic(Name="test_topic")
            topic_arn = topic["TopicArn"]
            strata_period_wrangler.send_sns_message(topic_arn, "test_checkpoint")

    @mock_sqs
    def test_sqs_send_message(self):
        """
        Tests sending of sqs messages to the queue.
        :return: None.
        """
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(
            QueueName="test_queue_test.fifo", Attributes={"FifoQueue": "true"}
        )
        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        strata_period_wrangler.send_sqs_message(
            queue_url, "{'Test': 'Message'}", "test_group_id"
        )
        messages = strata_period_wrangler.get_sqs_message(queue_url)
        assert messages["Messages"][0]["Body"] == "{'Test': 'Message'}"

    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.

        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        strata_period_method.os.environ.pop("strata_column")
        response = strata_period_method.lambda_handler(
            {"RuntimeVariables": {"period": "201809"}}, {"aws_request_id": "666"}
        )
        # self.assertRaises(ValueError)
        assert response["error"].__contains__(
            """Error validating environment parameters:"""
        )

    def test_for_bad_data(self):
        response = strata_period_method.lambda_handler("", {"aws_request_id": "666"})
        assert response["error"].__contains__("""Input Error""")

    def test_strata_fail(self):
        with mock.patch.dict(
            strata_period_method.os.environ,
            {"strata_column": "strata", "value_column": "Q608_total"},
        ):
            with open("tests/fixtures/strata_in.json", "r") as file:
                content = file.read()
                dataframe_content = pd.DataFrame(json.loads(content))
                dataframe_content.drop("land_or_marine", inplace=True, axis=1)

                json_content = json.loads(dataframe_content.to_json(orient="records"))

                response = strata_period_method.lambda_handler(
                    json_content, {"aws_request_id": "666"}
                )
                assert response["error"].__contains__("""Key Error in Strata - Method""")

    def test_raise_exception_exception_method(self):
        with mock.patch.dict(
            strata_period_method.os.environ,
            {"strata_column": "strata", "value_column": "Q608_total"},
        ):
            with mock.patch("logging.Logger.info") as mocked:
                mocked.side_effect = Exception("AARRRRGHH!!")
                response = strata_period_method.lambda_handler(
                    {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
                )
                assert "success" in response
                assert response["success"] is False
                assert response["error"].__contains__("""AARRRRGHH!!""")

    # Wrangler Exception tests

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow in the wrangler raises an exception.

        :return: None.
        """
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": queue_url,
            },
        ):

            # Removing the method_name to allow for test of missing parameter
            strata_period_wrangler.os.environ.pop("method_name")
            response = strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 123, "period": "201809"}},
                {"aws_request_id": "666"},
            )

            # self.assertRaises(ValueError)
            assert response["error"].__contains__(
                """Error validating environment parameters:"""
            )

    def test_raise_exception_exception_wrangles(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {"strata_column": "strata", "value_column": "Q608_total"},
        ):
            with mock.patch("strata_period_wrangler.boto3.client") as mocked:
                mocked.side_effect = Exception("AARRRRGHH!!")
                response = strata_period_wrangler.lambda_handler(
                    {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
                )
                assert "success" in response
                assert response["success"] is False
                assert response["error"].__contains__("""AARRRRGHH!!""")

    @mock_sqs
    def test_wrangles_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": "An Invalid Queue",
            },
        ):
            response = strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    @mock_sqs
    def test_wrangles_invoke_fails(self):
        # Note, purposely not using mock_lambda so an error occurs when attempting
        # to invoke (UnrecognisedClientException)
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": queue_url,
            },
        ):
            with mock.patch("strata_period_wrangler.get_sqs_message") as mock_squeues:
                msgbody = '{"period": 201809}'
                mock_squeues.return_value = {
                    "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                }
                response = strata_period_wrangler.lambda_handler(
                    {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
                )
            assert "success" in response
            assert response["success"] is False
            print(response["error"])
            assert response["error"].__contains__("""AWS Error""")

    @mock_sqs
    @mock_lambda
    def test_wrangles_happy_path(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": "sausages",
            },
        ):
            with mock.patch("strata_period_wrangler.get_sqs_message") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open("tests/fixtures/strata_out.json", "rb") as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 5894)
                        }
                        msgbody = '{"period": 201809}'
                        mock_squeues.return_value = {
                            "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                        }
                        response = strata_period_wrangler.lambda_handler(
                            {"RuntimeVariables": {"checkpoint": 666}},
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is True

    @mock_sqs
    def test_no_data_in_queue(self):
        sqs = boto3.client("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_url(QueueName="test_queue")["QueueUrl"]
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": queue_url,
            },
        ):
            response = strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            print(response["error"])
            assert response["error"].__contains__("""no data in sqs queue""")

    @mock_sqs
    @mock_lambda
    def test_wrangles_incomplete_json(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": "sausages",
            },
        ):
            with mock.patch("strata_period_wrangler.get_sqs_message") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open("tests/fixtures/strata_out.json", "rb") as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 2)
                        }
                        msgbody = '{"period": 201809}'
                        mock_squeues.return_value = {
                            "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                        }
                        response = strata_period_wrangler.lambda_handler(
                            {"RuntimeVariables": {"checkpoint": 666}},
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert response["error"].__contains__(
                            """Incomplete Lambda response"""
                        )

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": "sausages",
            },
        ):
            with mock.patch("strata_period_wrangler.get_sqs_message") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody("{'boo':'moo':}", 2)
                    }
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = {
                        "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                    }
                    response = strata_period_wrangler.lambda_handler(
                        {"RuntimeVariables": {"checkpoint": 666}},
                        {"aws_request_id": "666"},
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert response["error"].__contains__("""Bad data""")

    @mock_sqs
    @mock_lambda
    def test_wrangler_keyerror(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "queue_url": "sausages",
            },
        ):
            with mock.patch("strata_period_wrangler.get_sqs_message") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open("tests/fixtures/strata_out.json", "rb") as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 5894)
                        }
                        msgbody = '{"period": 201809}'
                        mock_squeues.return_value = {
                            "Messages": [{"Sausages": msgbody, "ReceiptHandle": 666}]
                        }
                        response = strata_period_wrangler.lambda_handler(
                            {"RuntimeVariables": {"checkpoint": 666}},
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert response["error"].__contains__("""Key Error""")
