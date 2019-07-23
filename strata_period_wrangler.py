import json
import traceback
import os
import random
import boto3


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


def get_environment_variable(variable):
    """
    obtains the environment variables and tests collection.
    :param variable:
    :return: output = varaible name
    """
    output = os.environ.get(variable, None)
    if output is None:
        raise ValueError(str(variable)+" config parameter missing.")
    return output


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
    sns = boto3.client('sns', region_name='eu-west-2')

    # Sqs
    queue_url = get_environment_variable('queue_url')
    function_name = get_environment_variable('function_name')
    message_group_id = get_environment_variable('message_group_id')

    # Sns
    arn = get_environment_variable('arn')
    checkpoint = get_environment_variable('checkpoint')

    # period = get_environment_variable('period')
    period = event['RuntimeVariables']['period']

    # checkpoint = event['data']['lambdaresult']['checkpoint']
    try:

        # Reads in Data from SQS Queue
        response = sqs.receive_message(QueueUrl=queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']

        returned_data = var_lambda.invoke(FunctionName=function_name,
                                          Payload=json.dumps(message_json))
        json_response = returned_data.get('Payload').read().decode("UTF-8")

        # MessageDeduplicationId is set to a random hash to overcome de-duplication, 
        # otherwise modules could not be re-run in the space of 5 Minutes.
        sqs.send_message(QueueUrl=queue_url, MessageBody=json_response,
                         MessageGroupId=message_group_id,
                         MessageDeduplicationId=str(random.getrandbits(128)))

        final_output = json.loads(json_response)
        print(final_output)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        send_sns_message(checkpoint)
        # checkpoint = checkpoint +1

    except Exception as exc:
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


def send_sns_message(checkpoint):
    """

    :param checkpoint:
    :return:
    """
    sns = boto3.client('sns', region_name='eu-west-2')
    checkpoint = get_environment_variable('checkpoint')
    arn = get_environment_variable('arn')

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
