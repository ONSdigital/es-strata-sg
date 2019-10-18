# ES Strata
 This is the second module within the BMI Survey. It calculates the Strata based on the survey type(Land or Marine), total turnover and region.

## Strata Wrangler
The wrangler prepares the data from enrichment, to be processed to calculate the Strata for each reference.
The wrangler calls the Strata method to then pass the data onto the SQS queue.

## Strata Method
Name of Lambda: strata_period_method

Intro: Using the survey type (Land or Marine), total turnover and region calculates strata for each reference.

Inputs: This method will require all of the Questions columns to be on the data which is being sent to the method, mainly the Q608_total. A strata column should be created for each question in the data wrangler for correct usage of the method. The way the method is written will create the columns if they haven't been created before but for best practice create them in the data wrangler.

Outputs: A Json string which contains all the Stratas. Blah. Blah
