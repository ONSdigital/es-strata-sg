#!/usr/bin/env bash

cd strata-deploy-repository
echo Destroying serverless bundle...
serverless remove --verbose;