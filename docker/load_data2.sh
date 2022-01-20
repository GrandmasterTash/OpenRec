#!/bin/bash

# The directory the script is in should be the docker project folder.
DOCKER_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Copying data to .inprogress"
cp $DOCKER_DIR/../examples/data/01-payments.csv $DOCKER_DIR/data/01_basic/inbox/01-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/02-payments.csv $DOCKER_DIR/data/02_projected/inbox/02-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/03-payments.csv $DOCKER_DIR/data/03_tolerance/inbox/03-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/04-payments.csv $DOCKER_DIR/data/04_three_way/inbox/04-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/04-receipts.csv $DOCKER_DIR/data/04_three_way/inbox/04-receipts.csv.inprogress
cp $DOCKER_DIR/../examples/data/05-payments.csv $DOCKER_DIR/data/05_two_stage/inbox/05-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/07-payments-b.csv $DOCKER_DIR/data/07_unmatched/inbox/07-payments-b.csv.inprogress
cp $DOCKER_DIR/../examples/data/08-payments.csv $DOCKER_DIR/data/08_advanced_lua/inbox/08-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/20220118_041500000_changeset.json $DOCKER_DIR/data/09_changesets/inbox/
cp $DOCKER_DIR/../examples/data/11-payments.csv $DOCKER_DIR/data/11_group_dates/inbox/11-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/12-payments.csv $DOCKER_DIR/data/12_lookups/inbox/12-payments.csv.inprogress

mkdir -p $DOCKER_DIR/data/12_lookups/lookups/
cp $DOCKER_DIR/../examples/data/*FXRates.csv $DOCKER_DIR/data/12_lookups/lookups/

echo "Removing .inprogress suffix"
mv $DOCKER_DIR/data/01_basic/inbox/01-payments.csv.inprogress $DOCKER_DIR/data/01_basic/inbox/01-payments.csv
mv $DOCKER_DIR/data/02_projected/inbox/02-payments.csv.inprogress $DOCKER_DIR/data/02_projected/inbox/02-payments.csv
mv $DOCKER_DIR/data/03_tolerance/inbox/03-payments.csv.inprogress $DOCKER_DIR/data/03_tolerance/inbox/03-payments.csv
mv $DOCKER_DIR/data/04_three_way/inbox/04-payments.csv.inprogress $DOCKER_DIR/data/04_three_way/inbox/04-payments.csv
mv $DOCKER_DIR/data/04_three_way/inbox/04-receipts.csv.inprogress $DOCKER_DIR/data/04_three_way/inbox/04-receipts.csv
mv $DOCKER_DIR/data/05_two_stage/inbox/05-payments.csv.inprogress $DOCKER_DIR/data/05_two_stage/inbox/05-payments.csv
mv $DOCKER_DIR/data/07_unmatched/inbox/07-payments-b.csv.inprogress $DOCKER_DIR/data/07_unmatched/inbox/07-payments-b.csv
mv $DOCKER_DIR/data/08_advanced_lua/inbox/08-payments.csv.inprogress $DOCKER_DIR/data/08_advanced_lua/inbox/08-payments.csv
mv $DOCKER_DIR/data/11_group_dates/inbox/11-payments.csv.inprogress $DOCKER_DIR/data/11_group_dates/inbox/11-payments.csv
mv $DOCKER_DIR/data/12_lookups/inbox/12-payments.csv.inprogress $DOCKER_DIR/data/12_lookups/inbox/12-payments.csv
