import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    period_column = fields.Str(required=True)
    reference = fields.Str(required=True)
    segmentation = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    period = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    distinct_values = fields.List(fields.String, required=True)
    sns_topic_arn = fields.Str(required=True)
    survey_column = fields.Str(required=True)


def lambda_handler(event, context):
    """
    prepares the data for the Strata method.
    - Read in data from the SQS queue.
    - Invoke the Strata Method.
    - Send data from the Strata method to the SQS queue.

    :param event:
    :param context:
    :return: string - Json string to send to the SNS topic upon completion
    """
    current_module = "Strata - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Strata")
    logger.setLevel(10)

    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Strata Wrangler Begun")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]
        # Set up clients
        var_lambda = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        period_column = environment_variables["period_column"]
        segmentation = environment_variables["segmentation"]
        reference = environment_variables["reference"]

        # Runtime Variables
        current_period = runtime_variables["period"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        region_column = runtime_variables["distinct_values"][0]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey_column = runtime_variables["survey_column"]

        logger.info("Retrieved configuration variables.")

        data_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)
        logger.info("Successfully retrieved data from s3")

        data_json = data_df.to_json(orient="records")
        json_payload = {
            "RuntimeVariables": {
                "data": data_json,
                "survey_column": survey_column,
                "region_column": region_column,
                "run_id": run_id
            }
        }
        returned_data = var_lambda.invoke(FunctionName=method_name,
                                          Payload=json.dumps(json_payload))
        logger.info("Successfully invoked method.")

        json_response = json.loads(returned_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        # Turn json back into dataframe
        output_dataframe = pd.read_json(json_response["data"], dtype=False)

        # Perform mismatch detection
        output_dataframe, anomalies = strata_mismatch_detector(
            output_dataframe,
            current_period, period_column,
            reference, segmentation,
            "good_" + segmentation,
            "current_" + period_column,
            "previous_" + period_column,
            "current_" + segmentation,
            "previous_" + segmentation)

        logger.info("Successfully saved input data")
        # Push current period data onwards
        aws_functions.save_to_s3(bucket_name, out_file_name,
                                 output_dataframe.to_json(orient="records"))
        logger.info("Successfully sent data to s3")

        anomalies = anomalies.to_json(orient="records")

        if anomalies != "[]":
            aws_functions.save_to_s3(bucket_name, "Strata_Anomalies", anomalies)
            have_anomalies = True
        else:
            have_anomalies = False

        aws_functions.send_sns_message_with_anomalies(have_anomalies, sns_topic_arn,
                                                      "Strata.")

        logger.info("Successfully sent message to sns")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True}


def strata_mismatch_detector(data, current_period, time, reference, segmentation,
                             stored_segmentation, current_time, previous_time,
                             current_segmentation, previous_segmentation):
    """
    Looks only at id and strata columns. Then drops any duplicated rows (keep=false means
    that if there is a dupe it'll drop both). If there are any rows in this DataFrame it
    shows that the reference-strata combination was unique, and therefore the strata is
    different between periods.
    :param data: The DataFrame the miss-match detection will be performed on.
    :param current_period: The current period of the run.
    :param time: Field name which is used as a gauge of time'. Added for IAC config.
    :param reference: Field name which is used as a reference for IAC.
    :param segmentation: Field name of the segmentation used for IAC.
    :param stored_segmentation: Field name of stored segmentation for IAC.
    :param current_time: Field name of the current time used for IAC.
    :param previous_time: Field name of the previous time used for IAC.
    :param current_segmentation: Field name of the current segmentation used for IAC.
    :param previous_segmentation: Field name of the current segmentation used for IAC.
    :return: Success & Error on Fail or Success, Impute and distinct_values Type: JSON
    """
    data_anomalies = data[[reference, segmentation, time]]

    data_anomalies = data_anomalies.drop_duplicates(subset=[reference, segmentation],
                                                    keep=False)

    if data_anomalies.size > 0:
        # Filter to only include data from the current period
        fix_data = data_anomalies[data_anomalies[time] == int(current_period)][
            [reference, segmentation]]
        fix_data = fix_data.rename(columns={segmentation: stored_segmentation})

        # Now merge these so that the fix_data strata is
        # added as an extra column to the input data
        data = pd.merge(data, fix_data, on=reference, how="left")

        # We should now have a good Strata column in the dataframe - mostly containing
        # null values, containing strata where there was anomoly using an apply method,
        # set strata to be the goodstrata.
        data[segmentation] = data.apply(
            lambda x: x[stored_segmentation]
            if str(x[stored_segmentation]) != "nan" else x[segmentation], axis=1)
        data = data.drop(stored_segmentation, axis=1)

        # Split on period then merge together so they're same row.
        current_period_anomalies = data_anomalies[
            data_anomalies[time] == int(current_period)].rename(
            columns={segmentation: current_segmentation, time: current_time})

        prev_period_anomalies = data_anomalies[data_anomalies[time]
                                               != int(current_period)].rename(
            columns={segmentation: previous_segmentation, time: previous_time})

        data_anomalies = pd.merge(current_period_anomalies, prev_period_anomalies,
                                  on=reference)

    return data, data_anomalies
