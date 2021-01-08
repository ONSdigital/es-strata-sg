import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.testing import assert_frame_equal

import strata_period_method as lambda_method_function
import strata_period_wrangler as lambda_wrangler_function

method_environment_variables = {
    "strata_column": "strata",
    "value_column": "Q608_total"
}

wrangler_environment_variables = {
    "bucket_name": "test_bucket",
    "method_name": "strata_period_method",
    "period_column": "period",
    "reference": "responder_id",
    "segmentation": "strata"
}

method_runtime_variables = {
    "RuntimeVariables": {
        "bpm_queue_url": "fake_queue_url",
        "current_period": "201809",
        "data": None,
        "environment": "sandbox",
        "period_column": "period",
        "reference": "responder_id",
        "region_column": "region",
        "run_id": "bob",
        "segmentation": "strata",
        "survey": "BMI_SG",
        "survey_column": "survey"
    }
}

wrangler_runtime_variables = {
    "RuntimeVariables":
        {
            "bpm_queue_url": "fake_queue_url",
            "distinct_values": ["region"],
            "environment": "sandbox",
            "in_file_name": "test_wrangler_input",
            "out_file_name": "test_wrangler_output.json",
            "period": "201809",
            "run_id": "bob",
            "sns_topic_arn": "fake_sns_arn",
            "survey": "BMI_SG",
            "survey_column": "survey",
            "total_steps": 6
        }
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################

@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
@mock.patch('strata_period_wrangler.aws_functions.send_bpm_status')
def test_client_error(mock_bpm_status, which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_method_function, method_runtime_variables,
         method_environment_variables, "strata_period_method.EnvironmentSchema",
         "'Exception'", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, "strata_period_wrangler.EnvironmentSchema",
         "'Exception", test_generic_library.wrangler_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
def test_incomplete_read_error():
    file_list = ["test_wrangler_input.json"]

    test_generic_library.incomplete_read_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "strata_period_wrangler",
                                               "IncompleteReadError")


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_method_function, method_environment_variables,
         "KeyError", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_environment_variables,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@mock_s3
def test_method_error():
    file_list = ["test_wrangler_input.json"]

    test_generic_library.wrangler_method_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "strata_period_wrangler")


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [(lambda_method_function,
      "Error validating environment param",
      test_generic_library.method_assert, {}),
     (lambda_method_function,
      "Error validating runtime param",
      test_generic_library.method_assert, method_environment_variables),
     (lambda_wrangler_function,
      "Error validating environment param",
      test_generic_library.wrangler_assert, {}),
     (lambda_wrangler_function,
      "Error validating runtime param",
      test_generic_library.wrangler_assert, wrangler_environment_variables)])
def test_value_error(which_lambda, expected_message,
                     assertion, which_environment_variables):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion,
        environment_variables=which_environment_variables)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


def test_calculate_strata():
    """
    Runs the calculate_strata function that is called by the method.
    :param None
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_method_input.json", "r") as file_1:
        file_data = file_1.read()
    input_data = pd.DataFrame(json.loads(file_data))

    produced_data = input_data.apply(
        lambda_method_function.calculate_strata,
        strata_column="strata",
        value_column="Q608_total",
        survey_column="survey",
        region_column="region",
        axis=1
    )
    produced_data = produced_data.sort_index(axis=1)

    with open("tests/fixtures/test_calculate_strata_prepared_output.json", "r") as file_2:
        file_data = file_2.read()
    prepared_data = pd.DataFrame(json.loads(file_data)).sort_index(axis=1)

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
def test_method_success():
    """
    Runs the method function.
    :param None
    :return Test Pass/Fail
    """
    with mock.patch.dict(lambda_method_function.os.environ,
                         method_environment_variables):
        with open("tests/fixtures/test_method_prepared_output.json", "r") as file_1:
            file_data = file_1.read()
        prepared_data = pd.DataFrame(json.loads(file_data)).sort_index(axis=1)

        with open("tests/fixtures/test_method_input.json", "r") as file_2:
            test_data = file_2.read()
        method_runtime_variables["RuntimeVariables"]["data"] = test_data

        output = lambda_method_function.lambda_handler(
            method_runtime_variables, test_generic_library.context_object)

        produced_data = pd.DataFrame(json.loads(output["data"])).sort_index(axis=1)

    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


def test_strata_mismatch_detector():
    """
    Runs the strata_mismatch_detector function that is called by the wrangler.
    :param None
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_method_prepared_output.json", "r") as file_1:
        test_data_in = file_1.read()
    method_data = pd.DataFrame(json.loads(test_data_in))

    produced_data, anomalies = lambda_method_function.strata_mismatch_detector(
        method_data,
        "201809", "period",
        "responder_id", "strata",
        "good_strata",
        "current_period",
        "previous_period",
        "current_strata",
        "previous_strata")
    produced_data = produced_data.sort_index(axis=1)

    with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_2:
        test_data_out = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_out))

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
def test_wrangler_success_passed():
    """
    Runs the wrangler function.
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            # Rather than mock the get/decode we tell the code that when the invoke is
            # called pass the variables to this replacement function instead.
            mock_client_object.invoke.side_effect =\
                test_generic_library.replacement_invoke

            # This stops the Error caused by the replacement function from stopping
            # the test.
            with pytest.raises(exception_classes.LambdaFailure):
                lambda_wrangler_function.lambda_handler(
                    wrangler_runtime_variables, test_generic_library.context_object
                )

    with open("tests/fixtures/test_method_input.json", "r") as file_2:
        test_data_prepared = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/test_wrangler_to_method_input.json", "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    # Compares the data.
    assert_frame_equal(produced_data, prepared_data)

    with open("tests/fixtures/test_wrangler_to_method_runtime.json", "r") as file_4:
        test_dict_prepared = file_4.read()
    produced_dict = json.loads(test_dict_prepared)

    # Ensures data is not in the RuntimeVariables and then compares.
    method_runtime_variables["RuntimeVariables"]["data"] = None
    assert produced_dict == method_runtime_variables["RuntimeVariables"]


@mock_s3
@mock.patch('strata_period_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
def test_wrangler_success_returned(mock_s3_put):
    """
    Runs the wrangler function.
    :param mock_s3_put - Replacement Function For The Data Saveing AWS Functionality.
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_method_prepared_output.json", "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("strata_period_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            mock_client_object.invoke.return_value.get.return_value.read \
                .return_value.decode.return_value = json.dumps({
                 "data": test_data_out,
                 "success": True,
                 "anomalies": "[]"
                })

            output = lambda_wrangler_function.lambda_handler(
                wrangler_runtime_variables, test_generic_library.context_object
            )

    with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_3:
        test_data_prepared = file_3.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/" +
              wrangler_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_4:
        test_data_produced = file_4.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)
