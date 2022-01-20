#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "Please specify the number of rows to generate for the performance control"
    exit 1
fi

ROWS=$1

# The directory the script is in should be the docker project folder.
DOCKER_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# OpenRec will be creating files and folders - best to use the current user to achieve this
# Otherwise, running in docker typically means the docker account is used to own those folders.
export OPENREC_UID=${UID}
export OPENREC_GID=${GID}

# Generator is a utility to generate three-way match groups of random data.
echo "Generating a SMALL amount of data for the performance control..."
pushd $DOCKER_DIR/data
docker-compose run openrec /generator --rows $ROWS --output /data

echo "Deploying inprogress files..."
mv invoices.csv ./10_performance/inbox/10-invoices.csv.inprogress
mv payments.csv ./10_performance/inbox/10-payments.csv.inprogress
mv receipts.csv ./10_performance/inbox/10-receipts.csv.inprogress

echo "Finalising files..."
mv ./10_performance/inbox/10-invoices.csv.inprogress ./10_performance/inbox/10-invoices.csv
mv ./10_performance/inbox/10-payments.csv.inprogress ./10_performance/inbox/10-payments.csv
mv ./10_performance/inbox/10-receipts.csv.inprogress ./10_performance/inbox/10-receipts.csv

popd
echo "Done"
