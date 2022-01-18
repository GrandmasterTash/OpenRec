#!/bin/bash

# The directory the script is in should be the docker project folder.
DOCKER_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

TODO:

mkdir -p /home/stef/dev/rust/OpenRec/steward/tmp/three_way/inbox/

echo "Generating data..."
pushd /home/stef/dev/rust/OpenRec/steward/
/home/stef/dev/rust/OpenRec/target/release/generator --rows 100000

echo "Deploying inprogress files..."
pushd ./tmp
mv invoices.csv ./three_way/inbox/invoices.csv.inprogress
mv payments.csv ./three_way/inbox/payments.csv.inprogress
mv receipts.csv ./three_way/inbox/receipts.csv.inprogress

echo "Finalising files..."
pushd ./three_way/inbox
mv invoices.csv.inprogress invoices.csv
mv payments.csv.inprogress payments.csv
mv receipts.csv.inprogress receipts.csv

popd
popd
echo "Done"
