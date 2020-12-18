import json
import logging
import os

import boto3
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
    survey = fields.Str(required=True)
    bpm_queue_url = fields.Str(required=True)
    total_steps = fields.Str(required=True)
    environment = fields.Str(Required=True)


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
    bpm_queue_url = None
    current_step_num = "3"

    # Define run_id outside of try block
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]
        # Set up clients
        var_lambda = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        period_column = environment_variables["period_column"]
        segmentation = environment_variables["segmentation"]
        reference = environment_variables["reference"]

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        current_period = runtime_variables["period"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        region_column = runtime_variables["distinct_values"][0]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey = runtime_variables["survey"]
        survey_column = runtime_variables["survey_column"]
        total_steps = runtime_variables["total_steps"]
        environment = runtime_variables['environment']
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id,
                                                           context=context)
        raise exception_classes.LambdaFailure(error_message)

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context)

        raise exception_classes.LambdaFailure(error_message)

    try:

        logger.info("Started - retrieved configuration variables.")

        # Send start of module status to BPM.
        status = "IN PROGRESS"
        aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id,
                                      current_step_num, total_steps)

        data_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)
        logger.info("Successfully retrieved data from s3")

        data_json = data_df.to_json(orient="records")
        json_payload = {
            "RuntimeVariables": {
                "data": data_json,
                "current_period": current_period,
                "period_column": period_column,
                "segmentation": segmentation,
                "survey": survey,
                "survey_column": survey_column,
                "reference": reference,
                "region_column": region_column,
                "run_id": run_id,
                "bpm_queue_url": bpm_queue_url,
                "environment": environment
            }
        }
        returned_data = var_lambda.invoke(FunctionName=method_name,
                                          Payload=json.dumps(json_payload))
        logger.info("Successfully invoked method.")

        json_response = json.loads(returned_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        # Push current period data onwards
        aws_functions.save_to_s3(bucket_name, out_file_name, json_response["data"])
        logger.info("Successfully sent data to s3")

        anomalies = json_response["anomalies"]

        if anomalies != "[]":
            aws_functions.save_to_s3(bucket_name, "Strata_Anomalies", anomalies)
            have_anomalies = True
        else:
            have_anomalies = False
        logger.info("Successfully sent anomalies to s3")

        aws_functions.send_sns_message_with_anomalies(have_anomalies, sns_topic_arn,
                                                      "Strata.")

        logger.info("Successfully sent message to sns")

    except Exception as e:
        error_message = general_functions.handle_exception(e,
                                                           current_module,
                                                           run_id,
                                                           context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    # Send end of module status to BPM.
    status = "DONE"
    aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id,
                                  current_step_num, total_steps)
    return {"success": True}
