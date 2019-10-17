import json
import logging
import os
import random

import boto3
import marshmallow
from botocore.exceptions import ClientError, IncompleteReadError


class EnvironSchema(marshmallow.Schema):
    """
    Class to setup the environment variables schema.
    """

    arn = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)
    sqs_message_group_id = marshmallow.fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


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

        # Reads in Data from SQS Queue
        response = get_sqs_message(queue_url)
        if "Messages" not in response:
            raise NoDataInQueueError("No Messages in queue")

        message = response["Messages"][0]
        message_json = json.loads(message["Body"])
        receipt_handle = message["ReceiptHandle"]

        logger.info("Successfully retrieved data from sqs")

        returned_data = var_lambda.invoke(
            FunctionName=method_name, Payload=json.dumps(message_json)
        )

        json_response = returned_data.get("Payload").read().decode("UTF-8")

        logger.info("Successfully invoked lambda")

        send_sqs_message(queue_url, json_response, sqs_message_group_id)

        logger.info("Successfully sent data to sqs")

        sqs = boto3.client("sqs", region_name="eu-west-2")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        logger.info("Successfully deleted input data from sqs")

        send_sns_message(arn, checkpoint)

        logger.info("Successfully sent data to sns")
    except NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
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


def send_sns_message(arn, checkpoint):
    """
    This function is responsible for sending notifications to the SNS Topic.
    Notifications will be used to relay information to the BPM.

    :param arn: The Address of the SNS topic - Type: String.
    :param checkpoint: Location of process - Type: String.
    :return:None.
    """
    sns = boto3.client("sns", region_name="eu-west-2")

    sns_message = {
        "success": True,
        "module": "Strata",
        "checkpoint": checkpoint,
        "message": "Completed Strata",
    }

    sns.publish(TargetArn=arn, Message=json.dumps(sns_message))


def send_sqs_message(queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.
    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """
    # sqs = boto3.client('sqs')
    sqs = boto3.client("sqs", region_name="eu-west-2")

    # MessageDeduplicationId is set to a random hash to overcome de-duplication,
    # otherwise modules could not be re-run in the space of 5 Minutes.
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageGroupId=output_message_id,
        MessageDeduplicationId=str(random.getrandbits(128)),
    )


def get_sqs_message(queue_url):
    """
    Retrieves message from the SQS queue.
    :param queue_url: The url of the SQS queue. - Type: String.
    :return: Message from queue - Type: String.
    """
    sqs = boto3.client("sqs", region_name="eu-west-2")
    return sqs.receive_message(QueueUrl=queue_url)
