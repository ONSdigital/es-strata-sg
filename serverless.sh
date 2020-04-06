#!/usr/bin/env bash

# Serverless deploy
cd strata-deploy-repository
echo Installing dependancies
serverless plugin install --name serverless-pseudo-parameters
serverless plugin install --name serverless-latest-layer-version
echo Packaging serverless bundle...
serverless package --package pkg
serverless deploy --verbose;
