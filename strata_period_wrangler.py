import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Class to setup the environment variables schema.
    """

    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    period_column = fields.Str(required=True)
    segmentation = fields.Str(required=True)
    reference = fields.Str(required=True)


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

    try:
        logger.info("Strata Wrangler Begun")
        # Set up clients
        var_lambda = boto3.client("lambda", region_name="eu-west-2")

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Vaildated params")
        # Set up environment variables
        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        in_file_name = config['in_file_name']
        incoming_message_group = config['incoming_message_group']
        method_name = config['method_name']
        out_file_name = config['out_file_name']
        sns_topic_arn = config['sns_topic_arn']
        sqs_message_group_id = config['sqs_message_group_id']
        sqs_queue_url = config['sqs_queue_url']
        survey_column = event['RuntimeVariables']['survey_column']
        period_column = config['period_column']
        segmentation = config['segmentation']
        reference = config['reference']
        region_column = event['RuntimeVariables']['distinct_values'][0]
        current_period = event['RuntimeVariables']['period']
        previous_period = general_functions.calculate_adjacent_periods(current_period,
                                                                       "03")
        message_json, receipt_handle = aws_functions.get_data(sqs_queue_url,
                                                              bucket_name,
                                                              in_file_name,
                                                              incoming_message_group)

        logger.info("Successfully retrieved data from sqs")

        json_payload = {"data": json.loads(message_json),
                        "survey_column": survey_column,
                        "region_column": region_column}
        returned_data = var_lambda.invoke(FunctionName=method_name,
                                          Payload=json.dumps(json_payload))
        logger.info("Successfully invoked method.")

        json_response = json.loads(returned_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        # Turn json back into dataframe
        output_dataframe = pd.read_json(json_response['data'])

        # Perform mismatch detection
        output_dataframe, anomalies = strata_mismatch_detector(
            output_dataframe,
            current_period, period_column,
            reference, segmentation,
            "good_" + segmentation,
            "current_" + period_column,
            "previous_" + period_column,
            "current_" + segmentation,
            "previous_" + segmentation,)

        # split dataframe for current and previous
        previous_dataframe = output_dataframe[
            output_dataframe[period_column] == int(previous_period)]
        current_dataframe = output_dataframe[
            output_dataframe[period_column] == int(current_period)]

        # Save previous period data to s3 for apply to pick up later
        aws_functions.save_to_s3(bucket_name, 'prev_datafile.json',
                                 previous_dataframe.to_json(orient='records'))
        logger.info("Successfully saved input data")
        # Push current period data onwards
        aws_functions.save_data(bucket_name, out_file_name,
                                current_dataframe.to_json(orient='records'),
                                sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent data to s3")

        sqs = boto3.client("sqs", region_name="eu-west-2")

        if receipt_handle:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handle)

        logger.info("Successfully deleted input data from s3")

        aws_functions.\
            send_sns_message_with_anomalies(checkpoint,
                                            anomalies.to_json(orient='records'),
                                            sns_topic_arn,
                                            "Strata.")

        logger.info("Successfully sent message to sns")

    except AttributeError as e:
        error_message = (
                "Bad data encountered in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
                "Parameter validation error in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = (
                "AWS Error ("
                + str(e.response["Error"]["Code"])
                + ") "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
                "Key Error in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
                "Incomplete Lambda response encountered in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."
    except Exception as e:
        error_message = (
                "General Error in "
                + current_module
                + " ("
                + str(type(e))
                + ") |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}


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
    :return: Success & Error on Fail or Success, Checkpoint, Impute and distinct_values
             Type: JSON
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
        data = pd.merge(data, fix_data, on=reference, how='left')

        # We should now have a good Strata column in the dataframe - mostly containing
        # null values, containing strata where there was anomoly using an apply method,
        # set strata to be the goodstrata.
        data[segmentation] = data.apply(
            lambda x: x[stored_segmentation]
            if str(x[stored_segmentation]) != 'nan' else x[segmentation], axis=1)
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
