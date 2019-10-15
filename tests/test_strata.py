import unittest
import unittest.mock as mock
import json

import pandas as pd
import boto3
from moto import mock_sqs, mock_sns
from pandas.util.testing import assert_frame_equal

import strata_period_wrangler
import strata_period_method


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
            'os.environ', {
                'arn': 'mock:arn',
                'checkpoint': 'mock-checkpoint',
                'method_name': 'mock-name',
                'sqs_message_group_id': 'mock-group-id',
                'queue_url': 'mock-url'
            }
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()

        cls.mock_os_method_patcher = mock.patch.dict(
            'os.environ', {
                'queue_url': 'queue_url',
                'strata_column': 'strata',
                'value_column': 'Q608_total'
            }
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

    @mock_sqs
    @mock.patch('strata_period_wrangler.boto3.client')
    def test_wrangler(self, mock_lambda):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test-queue")
        queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

        with open('tests/fixtures/enrichment_out.json') as file:
            input_data = json.load(file)

        with mock.patch('json.loads') as json_loads:
            json_loads.return_value = input_data

            strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"period": "YYYYMM"}}, None)

        with open('tests/fixtures/strata_out.json') as file:
            payload_method = json.load(file)
        # check the output file contains the expected columns and non null values
        payload_dataframe = pd.DataFrame(payload_method)
        required_columns = {'strata'}

        strata_period_wrangler.send_sqs_message(queue_url, payload_method, "test")

        self.assertTrue(required_columns.issubset(set(payload_dataframe.columns)),
                        'Strata column is not in the DataFrame')

    def test_method(self):
        """
        mocks functionality of the method.

        :return: None
        """

        with open('tests/fixtures/strata_in.json') as file:
            json_content = json.load(file)

        actual_output = strata_period_method.lambda_handler(json_content, None)
        actual_output_dataframe = pd.DataFrame(actual_output)

        with open('tests/fixtures/strata_out.json') as file:
            expected_method_output = json.load(file)
            expected_output_dataframe = pd.DataFrame(expected_method_output)

        assert_frame_equal(actual_output_dataframe, expected_output_dataframe)

    def test_wrangler_get_traceback(self):
        """
        testing the traceback function works correctly.

        :param self:
        :return: None
        """
        traceback = strata_period_wrangler._get_traceback(Exception('test exception'))
  

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
                    'arn': 'mock:arn',
                    'checkpoint': 'mock-checkpoint',
                    'method_name': 'mock-name',
                    'sqs_message_group_id': 'mock-group-id',
                    'queue_url': queue_url
                }
        ):
            # Removing the checkpoint to allow for test of missing parameter
            strata_period_wrangler.os.environ.pop("method_name")
            response = strata_period_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 123, "period": "201809"}}, None)
            # self.assertRaises(ValueError)
            assert (response['error'].__contains__(
                """ValueError: Error validating environment parameters:"""))

    @mock_sns
    def test_sns_messages(self):
        """
        Test sending sns messages to the queue.
        :return: None.
        """
        with mock.patch.dict(strata_period_wrangler.os.environ,
                             {"arn": "test_arn"}):
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
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        sqs.create_queue(QueueName="test_queue_test.fifo",
                         Attributes={'FifoQueue': 'true'})
        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        strata_period_wrangler.send_sqs_message(queue_url,
                                                "{'Test': 'Message'}",
                                                "test_group_id")
        messages = strata_period_wrangler.get_sqs_message(queue_url)
        assert messages['Messages'][0]['Body'] == "{'Test': 'Message'}"

    def test_method_get_traceback(self):
        """
        testing the traceback function works correctly.

        :param self:
        :return: None.
        """
        traceback = strata_period_method._get_traceback(Exception('test exception'))
        assert traceback == 'Exception: test exception\n'

    @mock_sqs
    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.

        :return: None.
        """
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
                strata_period_wrangler.os.environ,
                {
                    'queue_url': queue_url,
                    'strata_column': 'strata',
                    'value_column': 'Q608_total'
                }
        ):
            # Removing the strata_column to allow for test of missing parameter
            strata_period_method.os.environ.pop("strata_column")
            response = strata_period_method.lambda_handler(
                {"RuntimeVariables": {"period": "201809"}}, None)
            # self.assertRaises(ValueError)
            assert (response['error'].__contains__(
                """ValueError: Error validating environment parameters:"""))
