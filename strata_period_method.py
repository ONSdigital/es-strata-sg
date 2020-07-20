import logging
import os

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    strata_column = fields.Str(required=True)
    value_column = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    data = fields.Str(required=True)
    current_period = fields.Str(required=True)
    period_column = fields.Str(required=True)
    reference = fields.Str(required=True)
    region_column = fields.Str(required=True)
    segmentation = fields.Str(required=True)
    survey_column = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Applies Calculate strata function to row of DataFrame.
    :param event: Event Object.
    :param context: Context object.
    :return: strata_out - Dict with "success" and "data" or "success and "error".
    """
    current_module = "Strata - Method"
    error_message = ""
    logger = logging.getLogger("Strata")
    # Define run_id outside of try block
    run_id = 0
    try:

        logger.info("Strata Method Begun")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        strata_column = environment_variables["strata_column"]
        value_column = environment_variables["value_column"]

        # Runtime Variables
        data = runtime_variables["data"]
        current_period = runtime_variables["current_period"]
        period_column = runtime_variables["period_column"]
        reference = runtime_variables["reference"]
        region_column = runtime_variables["region_column"]
        segmentation = runtime_variables["segmentation"]
        survey_column = runtime_variables["survey_column"]

        logger.info("Retrieved configuration variables.")

        input_data = pd.read_json(data, dtype=False)
        post_strata = input_data.apply(
            calculate_strata,
            strata_column=strata_column,
            value_column=value_column,
            survey_column=survey_column,
            region_column=region_column,
            axis=1,
        )
        logger.info("Successfully ran calculation")

        # Perform mismatch detection
        strata_check, anomalies = strata_mismatch_detector(
            post_strata,
            current_period, period_column,
            reference, segmentation,
            "good_" + segmentation,
            "current_" + period_column,
            "previous_" + period_column,
            "current_" + segmentation,
            "previous_" + segmentation)

        json_out = strata_check.to_json(orient="records")
        anomalies_out = anomalies.to_json(orient="records")

        final_output = {"data": json_out, "anomalies": anomalies_out}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output


def calculate_strata(row, value_column, region_column, strata_column, survey_column):
    """
    Calculates the strata for the reference based on Land or Marine value, question total
    value and region.
    :param row: Row of the dataframe that is being passed into the function.
    :param value_column: Column of the dataframe containing the Q608 total.
    :param region_column: Column name of the dataframe containing the region code.
    :param strata_column: Column of dataframe for the strata_column to be held.
    :param survey_column: Column name of the dataframe containing the survey code.
    :return: row: The calculated row including the strata.
    """
    row[strata_column] = ""

    # While Updating Tests I Couldn't Trigger This.
    # But Raised Question Of Should This Sort Of Data Validation Be Elsewhere?
    if row[value_column] is None:
        return row

    if row[strata_column] == "":
        if row[survey_column] == "076":
            row[strata_column] = "M"
        if row[survey_column] == "066" and row[value_column] < 30000:
            row[strata_column] = "E"
        if row[survey_column] == "066" and row[value_column] > 29999:
            row[strata_column] = "D"
        if row[survey_column] == "066" and row[value_column] > 79999:
            row[strata_column] = "C"
        if (
            row[survey_column] == "066"
            and row[value_column] > 129999
            and row[region_column] > 9
        ):
            row[strata_column] = "B2"
        if (
            row[survey_column] == "066"
            and row[value_column] > 129999
            and row[region_column] < 10
        ):
            row[strata_column] = "B1"
        if row[survey_column] == "066" and row[value_column] > 200000:
            row[strata_column] = "A"
    return row


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
        # null values, containing strata where there was anomaly using an apply method,
        # set strata to be the good strata.
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
