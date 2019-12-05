#!/usr/bin/env bash

cd strata-repository
echo Destroying serverless bundle...
serverless remove --verbose;
