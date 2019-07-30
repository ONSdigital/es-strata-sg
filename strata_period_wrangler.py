"""
Strata data wrangler.
"""
import json
import traceback
import os
import random
import boto3
import marshmallow


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object

    :return: string
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


class EnvironSchema(marshmallow.Schema):
    """
    Class to setup the environment variables schema.
    """
    arn = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)
    sqs_message_group_id = marshmallow.fields.Str(required=True)


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
    # Set up clients
    sqs = boto3.client('sqs', region_name='eu-west-2')
    var_lambda = boto3.client('lambda', region_name='eu-west-2')

    # period = get_environment_variable('period')
    period = event['RuntimeVariables']['period']

    try:
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")
        # Set up environment variables
        arn = config["arn"]
        checkpoint = config["checkpoint"]
        queue_url = config["queue_url"]
        method_name = config["method_name"]
        message_group_id = config["message"]

        # Reads in Data from SQS Queue
        response = get_sqs_message(queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']

        returned_data = var_lambda.invoke(FunctionName=method_name,
                                          Payload=json.dumps(message_json))
        json_response = returned_data.get('Payload').read().decode("UTF-8")

        send_sqs_message(queue_url, json_response, message_group_id)

        final_output = json.loads(json_response)
        print(final_output)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        send_sns_message(arn, checkpoint)

    except Exception as exc:
        checkpoint = config["checkpoint"]
        queue_url = config["queue_url"]
        purge = sqs.purge_queue(
            QueueUrl=queue_url
        )

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }


def send_sns_message(arn, checkpoint):
    """

    :param arn:
    :param checkpoint:
    :return:
    """
    sns = boto3.client('sns', region_name='eu-west-2')

    sns_message = {
        "success": True,
        "module": "Strata",
        "checkpoint": checkpoint,
        "message": "Completed Strata"
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def send_sqs_message(queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.
    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """
    # sqs = boto3.client('sqs')
    sqs = boto3.client('sqs', region_name='eu-west-2')

    # MessageDeduplicationId is set to a random hash to overcome de-duplication,
    # otherwise modules could not be re-run in the space of 5 Minutes.
    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128)))


def get_sqs_message(queue_url):
    """
    Retrieves message from the SQS queue.
    :param queue_url: The url of the SQS queue. - Type: String.
    :return: Message from queue - Type: String.
    """
    sqs = boto3.client('sqs', region_name='eu-west-2')
    return sqs.receive_message(QueueUrl=queue_url)

