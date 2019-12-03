import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_s3, mock_sqs
from pandas.util.testing import assert_frame_equal

import strata_period_method
import strata_period_wrangler


class MockContext():
    aws_request_id = 666


context_object = MockContext()


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
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()

        cls.mock_os_method_patcher = mock.patch.dict(
            "os.environ",
            {
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

            actual_output = strata_period_method.\
                lambda_handler(json_content, context_object)

            actual_output_dataframe = pd.DataFrame(json.loads(actual_output['data']))

            with open("tests/fixtures/strata_out.json") as file:
                expected_method_output = json.load(file)

                expected_output_dataframe = pd.DataFrame(expected_method_output)

            assert_frame_equal(actual_output_dataframe, expected_output_dataframe)

    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.

        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        strata_period_method.os.environ.pop("strata_column")
        response = strata_period_method.lambda_handler(
            {"RuntimeVariables": {"period": "201809"}}, context_object
        )

        assert response["error"].__contains__(
            """Error validating environment parameters:"""
        )

    def test_for_bad_data(self):
        response = strata_period_method.lambda_handler("", context_object)
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
                    json_content, context_object
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
                    {"RuntimeVariables": {"checkpoint": 666}}, context_object
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
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
            },
        ):
            # Removing the method_name to allow for test of missing parameter
            strata_period_wrangler.os.environ.pop("method_name")
            response = strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 123, "period": "201809"}},
                context_object,
            )

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
                    {"RuntimeVariables": {"checkpoint": 666}}, context_object
                )
                assert "success" in response
                assert response["success"] is False
                assert response["error"].__contains__("""AARRRRGHH!!""")

    @mock_sqs
    def test_wrangles_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            response = strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, context_object
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
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            with mock.patch("strata_period_wrangler.funk.get_data") as mock_squeues:
                msgbody = '{"period": 201809}'
                mock_squeues.return_value = msgbody, 666

                response = strata_period_wrangler.lambda_handler(
                    {"RuntimeVariables": {"checkpoint": 666}}, context_object
                )
            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    @mock_sqs
    @mock_lambda
    @mock_s3
    def test_wrangles_happy_path(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="Pie")
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            with mock.patch("strata_period_wrangler.funk.get_data") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open("tests/fixtures/strata_out.json", "r") as file:
                        mock_client_object.invoke.return_value\
                            .get.return_value.read\
                            .return_value.decode.return_value = json.dumps({
                             "data": file.read(), "success": True
                            })
                        msgbody = '{"period": 201809}'
                        mock_squeues.return_value = msgbody, 666

                        response = strata_period_wrangler.lambda_handler(
                            {"RuntimeVariables": {"checkpoint": 666}},
                            context_object,
                        )
                        print(response)
                        assert "success" in response
                        assert response["success"] is True

    @mock_sqs
    @mock_lambda
    def test_wrangles_incomplete_json(self):
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            with mock.patch("strata_period_wrangler.funk.get_data") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open("tests/fixtures/strata_out.json", "rb") as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 2)
                        }
                        msgbody = '{"period": 201809}'
                        mock_squeues.return_value = msgbody,  666

                        response = strata_period_wrangler.lambda_handler(
                            {"RuntimeVariables": {"checkpoint": 666}},
                            context_object,
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
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            with mock.patch("strata_period_wrangler.funk.get_data") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody("{'boo':'moo':}", 2)
                    }
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = msgbody, 666
                    response = strata_period_wrangler.lambda_handler(
                        {"RuntimeVariables": {"checkpoint": 666}},
                        context_object,
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert response["error"].__contains__("""Bad data""")

    @mock_sqs
    @mock_lambda
    @mock_s3
    def test_wrangler_keyerror(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="Pie")
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            with mock.patch("strata_period_wrangler.funk.get_data") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open("tests/fixtures/strata_out.json", "rb") as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 5894)
                        }
                        msgbody = '{"period": 201809}'
                        mock_squeues.side_effect = KeyError("AARRRRGHH!!")
                        mock_squeues.return_value = msgbody, 666
                        response = strata_period_wrangler.lambda_handler(
                            {"RuntimeVariables": {"checkpoint": 666}},
                            context_object,
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert response["error"].__contains__("""Key Error""")

    @mock_sqs
    @mock_lambda
    @mock_s3
    def test_wrangles_method_error(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="Pie")
        with mock.patch.dict(
            strata_period_wrangler.os.environ,
            {
                "sns_topic_arn": "mock:arn",
                "checkpoint": "mock-checkpoint",
                "sqs_queue_url": "sausages",
                "method_name": "mock-name",
                "sqs_message_group_id": "mock-group-id",
                "incoming_message_group": "IIIIINNNNCOOOOMMMMIING!!!!!",
                "in_file_name": "test1.json",
                "out_file_name": "test2.json",
                "bucket_name": "Pie"
            },
        ):
            with mock.patch("strata_period_wrangler.funk.get_data") as mock_squeues:
                with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object

                    mock_client_object.invoke.return_value.get.return_value \
                        .read.return_value.decode.return_value = \
                        json.dumps({"error": "This is an error message",
                                    "success": False})
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = msgbody, 666

                    response = strata_period_wrangler.lambda_handler(
                        {"RuntimeVariables": {"checkpoint": 666}},
                        context_object,
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert response["error"].__contains__("""This is an error message""")
