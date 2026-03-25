#!/bin/bash
set -x
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

cd $PROJECT_DIR

if ! [ -z "$AWS_PROFILE" ]
then
	PROFILE="--profile $AWS_PROFILE"
fi

BUCKET_ARTIFACT=$1
if [ -z $BUCKET_ARTIFACT ]
then
    echo "[Bucket artifact] needed!"
    exit 1
fi 

aws s3 $PROFILE rm $BUCKET_ARTIFACT/airflow/dags/pipelines --recursive
if [ $? -ne 0 ]; then echo "Error uploading DAGS to S3!"; exit 1; fi;

aws s3 $PROFILE cp dags/pipelines $BUCKET_ARTIFACT/airflow/dags/pipelines/ --recursive
if [ $? -ne 0 ]; then echo "Error uploading pipelines parameters to S3!"; exit 1; fi;

exit 0