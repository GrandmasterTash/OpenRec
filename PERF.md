## Performance Before
Using 09-3-Way-Performance.yaml with a --release build.
Used 250K invoices (1mil blew 12GB + 4GB swap during first trial although _may_ have used debug by mistake).

Prior to retained field overhaul, results were:-
Peak RAM : 4.16GB
Duration : 1m 7s 474ms
Output Files: -
Console output here
[2021-11-30T07:07:59Z INFO  matcher] Starting match job 1da111b6-d02f-475e-a252-f068e71ceeef
[2021-11-30T07:07:59Z INFO  matcher::folders] Using folder REC_HOME [/home/stef/dev/rust/celerity/matcher/tmp]
[2021-11-30T07:07:59Z INFO  matcher] Running charter [Three-way Performance] v1637208553000
[2021-11-30T07:07:59Z INFO  matcher::grid] Sourcing data with pattern [.*09-BIG-invoices\.csv]
[2021-11-30T07:07:59Z INFO  matcher::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-invoices.csv
[2021-11-30T07:08:00Z INFO  matcher::grid] 250000 records read from file 20211130_065025197_09-BIG-invoices.csv
[2021-11-30T07:08:00Z INFO  matcher::grid] Sourcing data with pattern [.*09-BIG-payments\.csv]
[2021-11-30T07:08:00Z INFO  matcher::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-payments.csv
[2021-11-30T07:08:01Z INFO  matcher::grid] 874288 records read from file 20211130_065025197_09-BIG-payments.csv
[2021-11-30T07:08:01Z INFO  matcher::grid] Sourcing data with pattern [.*09-BIG-receipts\.csv]
[2021-11-30T07:08:01Z INFO  matcher::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-receipts.csv
[2021-11-30T07:08:02Z INFO  matcher::grid] 874288 records read from file 20211130_065025197_09-BIG-receipts.csv
[2021-11-30T07:08:02Z INFO  matcher::instructions::project_col] Projecting column PAYMENT_AMOUNT_BASE as DECIMAL
[2021-11-30T07:08:14Z INFO  matcher::instructions::project_col] Projection took 12s 83ms for 1998576 rows (0.006ms/row)
[2021-11-30T07:08:14Z INFO  matcher] Grid Memory Size: 1GB
[2021-11-30T07:08:14Z INFO  matcher::instructions::project_col] Projecting column RECEIPT_AMOUNT_BASE as DECIMAL
[2021-11-30T07:08:26Z INFO  matcher::instructions::project_col] Projection took 12s 513ms for 1998576 rows (0.006ms/row)
[2021-11-30T07:08:26Z INFO  matcher] Grid Memory Size: 1GB
[2021-11-30T07:08:26Z INFO  matcher::instructions::project_col] Projecting column TOTAL_AMOUNT_BASE as DECIMAL
[2021-11-30T07:08:35Z INFO  matcher::instructions::project_col] Projection took 9s 106ms for 1998576 rows (0.005ms/row)
[2021-11-30T07:08:35Z INFO  matcher] Grid Memory Size: 1GB
[2021-11-30T07:08:35Z INFO  matcher::instructions::merge_col] Merging columns into AMOUNT_BASE
[2021-11-30T07:08:36Z INFO  matcher] Grid Memory Size: 1GiB
[2021-11-30T07:08:36Z INFO  matcher::instructions::merge_col] Merging columns into REFERENCE
[2021-11-30T07:08:37Z INFO  matcher] Grid Memory Size: 1GiB
[2021-11-30T07:08:37Z INFO  matcher::instructions::match_groups] Grouping by REFERENCE
[2021-11-30T07:09:07Z INFO  matcher::instructions::match_groups] Matched 249990 out of 249990 groups. Constraints took 27s 25ms (0.108ms/group)
[2021-11-30T07:09:07Z INFO  matcher] Grid Memory Size: 0B
[2021-11-30T07:09:07Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.csv] from [./tmp/matching] to [./tmp/archive]
[2021-11-30T07:09:07Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.csv] from [./tmp/matching] to [./tmp/archive]
[2021-11-30T07:09:07Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.csv] from [./tmp/matching] to [./tmp/archive]
[2021-11-30T07:09:07Z INFO  matcher] Completed match job 1da111b6-d02f-475e-a252-f068e71ceeef in 1m 7s 474ms

## Performance After
After retained field overhaul, results were: -
Peak RAM: 
Duration: 
Output Files: -