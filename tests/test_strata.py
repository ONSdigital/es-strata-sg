"""
Tests for Strata Module.
"""
import unittest
import unittest.mock as mock
import json
import sys, os
import pandas as pd
from pandas.util.testing import assert_frame_equal
import strata_period_wrangler
import strata_period_method
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))


class test_strata(unittest.TestCase):
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
                'function_name': 'mock-name',
                'message_group_id': 'mock-group-id',
                'period': '201809',
                'queue_url': 'mock-url'
            }
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()

        cls.mock_os_method_patcher = mock.patch.dict(
            'os.environ', {
                'queue_url': 'queue_url',
                'period_column': 'mock-period',
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

    @mock.patch('strata_period_wrangler.boto3.client')
    def test_wrangler(self, mock_lambda):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        with open('tests/enrichment_out.json') as file:
            input_data = json.load(file)

        with mock.patch('json.loads') as json_loads:
            json_loads.return_value = input_data

            strata_period_wrangler.lambda_handler({"RuntimeVariables": {"period": "YYYYMM"}}, None)

        payload = mock_lambda.return_value.invoke.call_args[1]['Payload']

        with open('tests/strata_out.json') as file:
            payload_method = json.load(file)
        # check the output file contains the expected columns and non null values
        payload_dataframe = pd.DataFrame(payload_method)
        required_columns = {'strata'}

        self.assertTrue(required_columns.issubset(set(payload_dataframe.columns)),
                        'Strata column is not in the DataFrame')

    def test_method(self):
        """
        mocks functionality of the method.

        :return: None

        """

        with open('tests/strata_in.json') as file:
            json_content = json.load(file)

        actual_output = strata_period_method.lambda_handler(json_content, None)
        print(actual_output)
        actual_output_dataframe = pd.DataFrame(actual_output)

        with open('tests/strata_out.json') as file:
            expected_method_output = json.load(file)
            expected_output_dataframe = pd.DataFrame(expected_method_output)

        assert_frame_equal(actual_output_dataframe, expected_output_dataframe)


    def test_wrangler_get_traceback(self):
        """
        testing the traceback function works correctly.

        :param self:
        :return:
        """
        traceback = strata_period_wrangler._get_traceback(Exception('test exception'))
        assert traceback == 'Exception: test exception\n'
