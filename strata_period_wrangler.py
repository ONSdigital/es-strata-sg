import logging
import os

import boto3
import marshmallow
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk


class EnvironSchema(marshmallow.Schema):
    """
    Class to setup the environment variables schema.
    """

    arn = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)
    sqs_message_group_id = marshmallow.fields.Str(required=True)
    incoming_message_group = marshmallow.fields.Str(required=True)
    file_name = marshmallow.fields.Str(required=True)
    bucket_name = marshmallow.fields.Str(required=True)


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
        arn = config["arn"]
        checkpoint = config["checkpoint"]
        queue_url = config["queue_url"]
        method_name = config["method_name"]
        sqs_message_group_id = config["sqs_message_group_id"]
        incoming_message_group = config["incoming_message_group"]
        file_name = config["file_name"]
        bucket_name = config["bucket_name"]

        message_json, receipt_handle = funk.get_data(queue_url,
                                                     bucket_name,
                                                     "enrichment_out.json",
                                                     incoming_message_group)

        logger.info("Successfully retrieved data from sqs")

        returned_data = var_lambda.invoke(
            FunctionName=method_name, Payload=message_json
        )

        json_response = returned_data.get("Payload").read().decode("UTF-8")

        logger.info("Successfully invoked lambda")

        funk.save_data(bucket_name, file_name, json_response, queue_url,
                       sqs_message_group_id)

        logger.info("Successfully sent data to sqs")

        sqs = boto3.client("sqs", region_name="eu-west-2")
        if(receipt_handle):
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        logger.info("Successfully deleted input data from sqs")

        funk.send_sns_message(checkpoint, arn, "Strata")

        logger.info("Successfully sent data to sns")

    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Parameter validation error"
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
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
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
            "Incomplete Lambda response encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = (
            "General Error in "
            + current_module
            + " ("
            + str(type(e))
            + ") |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
