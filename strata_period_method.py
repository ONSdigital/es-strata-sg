import traceback
import pandas as pd
import json
import os
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

    """
    # ENV vars

    sqs = boto3.client('sqs', region_name='eu-west-2')
    queue_url = get_environment_variable('queue_url')

    try:
        print(event)
        input_data = pd.DataFrame(event)
        #input_data = pd.read_json(event)
        #json_data = pd.readjson(input_data)

        # Possible _ under calculate
        post_strata = input_data.apply(calculate_strata, axis=1)

        json_out = post_strata.to_json(orient='records')
        strata_out = json.loads(json_out)

    except Exception as exc:
        print("Unexpected exception {}".format(_get_traceback(exc)))
        
        return {
            "success": False,
            "module": "Strata Method",
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return strata_out


def calculate_strata(row):
    """
    
    """
    period_column = get_environment_variable('period_column')
    strata_column = get_environment_variable('strata_column')
    value_column = get_environment_variable('value_column')
    row[strata_column] = ""

    if row[strata_column] == "":
        if row["land_or_marine"] == "M":
            row[strata_column] = "M"
        if row["land_or_marine"] == "L" and row[value_column] < 30000:
            row[strata_column] = "E"
        if row["land_or_marine"] == "L" and row[value_column] > 29999:
            row[strata_column] = "D"
        if row["land_or_marine"] == "L" and row[value_column] > 79999:
            row[strata_column] = "C"
        if row["land_or_marine"] == "L" and row[value_column] > 129999 and row["region"] > 9:
            row[strata_column] = "B2"
        if row["land_or_marine"] == "L" and row[value_column] > 129999 and row["region"] < 10:
            row[strata_column] = "B1"
        if row["land_or_marine"] == "L" and row[value_column] > 200000:
            row[strata_column] = "A"
    return row
