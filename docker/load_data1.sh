#!/bin/bash

# The directory the script is in should be the docker project folder.
DOCKER_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Copying data to .inprogress"
cp $DOCKER_DIR/../examples/data/01-invoices.csv $DOCKER_DIR/data/01_basic/inbox/01-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/02-invoices.csv $DOCKER_DIR/data/02_projected/inbox/02-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/03-invoices.csv $DOCKER_DIR/data/03_tolerance/inbox/03-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/04-invoices.csv $DOCKER_DIR/data/04_three_way/inbox/04-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/05-invoices.csv $DOCKER_DIR/data/05_two_stage/inbox/05-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/07-invoices.csv $DOCKER_DIR/data/07_unmatched/inbox/07-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/07-payments-a.csv $DOCKER_DIR/data/07_unmatched/inbox/07-payments-a.csv.inprogress
cp $DOCKER_DIR/../examples/data/08-invoices.csv $DOCKER_DIR/data/08_advanced_lua/inbox/08-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/09-invoices.csv $DOCKER_DIR/data/09_changesets/inbox/09-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/09-payments.csv $DOCKER_DIR/data/09_changesets/inbox/09-payments.csv.inprogress
cp $DOCKER_DIR/../examples/data/11-invoices.csv $DOCKER_DIR/data/11_group_dates/inbox/11-invoices.csv.inprogress
cp $DOCKER_DIR/../examples/data/12-invoices.csv $DOCKER_DIR/data/12_lookups/inbox/12-invoices.csv.inprogress

mkdir -p $DOCKER_DIR/data/12_lookups/lookups/
cp $DOCKER_DIR/../examples/data/*FXRates.csv $DOCKER_DIR/data/12_lookups/lookups/

echo "Removing .inprogress suffix"
mv $DOCKER_DIR/data/01_basic/inbox/01-invoices.csv.inprogress $DOCKER_DIR/data/01_basic/inbox/01-invoices.csv
mv $DOCKER_DIR/data/02_projected/inbox/02-invoices.csv.inprogress $DOCKER_DIR/data/02_projected/inbox/02-invoices.csv
mv $DOCKER_DIR/data/03_tolerance/inbox/03-invoices.csv.inprogress $DOCKER_DIR/data/03_tolerance/inbox/03-invoices.csv
mv $DOCKER_DIR/data/04_three_way/inbox/04-invoices.csv.inprogress $DOCKER_DIR/data/04_three_way/inbox/04-invoices.csv
mv $DOCKER_DIR/data/05_two_stage/inbox/05-invoices.csv.inprogress $DOCKER_DIR/data/05_two_stage/inbox/05-invoices.csv
mv $DOCKER_DIR/data/07_unmatched/inbox/07-invoices.csv.inprogress $DOCKER_DIR/data/07_unmatched/inbox/07-invoices.csv
mv $DOCKER_DIR/data/07_unmatched/inbox/07-payments-a.csv.inprogress $DOCKER_DIR/data/07_unmatched/inbox/07-payments-a.csv
mv $DOCKER_DIR/data/08_advanced_lua/inbox/08-invoices.csv.inprogress $DOCKER_DIR/data/08_advanced_lua/inbox/08-invoices.csv
mv $DOCKER_DIR/data/09_changesets/inbox/09-invoices.csv.inprogress $DOCKER_DIR/data/09_changesets/inbox/09-invoices.csv
mv $DOCKER_DIR/data/09_changesets/inbox/09-payments.csv.inprogress $DOCKER_DIR/data/09_changesets/inbox/09-payments.csv
mv $DOCKER_DIR/data/11_group_dates/inbox/11-invoices.csv.inprogress $DOCKER_DIR/data/11_group_dates/inbox/11-invoices.csv
mv $DOCKER_DIR/data/12_lookups/inbox/12-invoices.csv.inprogress $DOCKER_DIR/data/12_lookups/inbox/12-invoices.csv

echo "Done. For the performance control, use './random_data.sh <num rows>' to generate and load data into the control"