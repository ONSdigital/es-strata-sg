"""
Strata Period Method.
"""
import traceback
import json
import os
import pandas as pd
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
    Class to set up the environment variables schema.
    """
    strata_column = marshmallow.fields.Str(required=True)
    value_column = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda for the Strata period method.

    - Applies Calculate strata function to row of dataframe.

    :param event:
    :param context:
    :return: strata_out - json of the data with stratas added on.
    """
    try:
        # Set up Environment variables Schema.
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        print(event)
        input_data = pd.DataFrame(event)
        strata_column = config["strata_column"]
        value_column = config["value_column"]

        # Possible _ under calculate
        post_strata = input_data.apply(calculate_strata,
                                       strata_column=strata_column,
                                       value_column=value_column, axis=1)

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


def calculate_strata(row, value_column, strata_column):
    """
    Calculates the strata for the reference based on Land or Marine value, question total
    value and region.

    :param row: Row of the dataframe that is being passed into the function.
    :param value_column: Column of the dataframe containing the Q608 total.
    :param strata_column: Column of dataframe for the strata_column to be held.
    """

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
        if row["land_or_marine"] == "L" and \
                row[value_column] > 129999 and row["region"] > 9:
            row[strata_column] = "B2"
        if row["land_or_marine"] == "L" and \
                row[value_column] > 129999 and row["region"] < 10:
            row[strata_column] = "B1"
        if row["land_or_marine"] == "L" and row[value_column] > 200000:
            row[strata_column] = "A"
    return row
