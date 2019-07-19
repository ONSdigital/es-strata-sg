import traceback
import pandas as pd
import json
import os
import boto3
#time_series_column = timeseriesperiod
#period_column = period
#strata_column = strata
#value_column = q608_total


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


def lambda_handler(event, context):
    """

    """
    # ENV vars

    sqs = boto3.client('sqs', region_name='eu-west-2')
    queue_url = os.environ['queue_url']

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
    period_column = os.environ['period_column']
    strata_column = os.environ['strata_column']
    value_column = os.environ['value_column']
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
