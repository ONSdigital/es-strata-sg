#!/usr/bin/env bash

cd strata-deploy-repository
echo Packaging serverless bundle...
serverless package --package pkg
serverless deploy --verbose;