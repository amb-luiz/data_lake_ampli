#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

if [ -z $PROJECT_NAME ]
then
    echo "PROJECT_NAME variable needed"
    exit 1
fi

if [ -z $ENV ]
then
    echo "ENV variable needed"
    exit 1
fi

if [ -z $REGION ]
then
    echo "REGION variable needed"
    exit 1
fi

if [ -z $ACCOUNT ]
then
    echo "ACCOUNT variable needed"
    exit 1
fi
export AWS_DEFAULT_REGION=$REGION

BUCKET_ARTIFACT=s3://$PROJECT_NAME-$REGION-$ACCOUNT-$ENV-artifact

./scripts/upload-dags.sh $BUCKET_ARTIFACT
if [ $? -ne 0 ]; then echo "Error uploading DAGs to S3!"; exit 1; fi;

exit 0
