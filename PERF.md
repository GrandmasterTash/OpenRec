## Performance Original
Using 09-3-Way-Performance.yaml with a --release build.
Used 250K invoices (1mil blew 12GB + 4GB swap during first trial although _may_ have used debug by mistake).

Prior to retained field overhaul, results were:-
Peak RAM : 4.16GB
Duration : 1m 7s 474ms
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

## Performance After Retaining only Derived
After retained field overhaul, results were: -
Peak RAM: 889MB
Duration: 1m 24s 368ms
Console output here
[2021-12-03T14:00:30Z INFO  matcher] Starting match job 6ec38334-6ba4-413a-9ff0-385c7b753044
[2021-12-03T14:00:30Z INFO  matcher::folders] Using folder REC_HOME [/home/stef/dev/rust/celerity/matcher/tmp]
[2021-12-03T14:00:30Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-03T14:00:30Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-03T14:00:30Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-03T14:00:30Z INFO  matcher] Running charter [Performance] v1637208553000
[2021-12-03T14:00:30Z INFO  matcher::model::grid] Sourcing data with pattern [.*09-BIG-invoices\.csv]
[2021-12-03T14:00:30Z INFO  matcher::model::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-invoices.csv (179.83MiB)
[2021-12-03T14:00:30Z INFO  matcher::model::grid] 250000 records read from file 20211130_065025197_09-BIG-invoices.csv
[2021-12-03T14:00:30Z INFO  matcher::model::grid] Grid Memory Size: 14MB
[2021-12-03T14:00:30Z INFO  matcher::model::grid] Sourcing data with pattern [.*09-BIG-payments\.csv]
[2021-12-03T14:00:30Z INFO  matcher::model::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-payments.csv (531.37MiB)
[2021-12-03T14:00:31Z INFO  matcher::model::grid] 874288 records read from file 20211130_065025197_09-BIG-payments.csv
[2021-12-03T14:00:31Z INFO  matcher::model::grid] Grid Memory Size: 60MiB
[2021-12-03T14:00:31Z INFO  matcher::model::grid] Sourcing data with pattern [.*09-BIG-receipts\.csv]
[2021-12-03T14:00:31Z INFO  matcher::model::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-receipts.csv (550.14MiB)
[2021-12-03T14:00:33Z INFO  matcher::model::grid] 874288 records read from file 20211130_065025197_09-BIG-receipts.csv
[2021-12-03T14:00:33Z INFO  matcher::model::grid] Grid Memory Size: 107MiB
[2021-12-03T14:00:33Z INFO  matcher::instructions::project_col] Projecting column PAYMENT_AMOUNT_BASE as DECIMAL
[2021-12-03T14:00:49Z INFO  matcher::instructions::project_col] Projection took 15s 807ms for 1998576 rows (0.008ms/row)
[2021-12-03T14:00:49Z INFO  matcher] Grid Memory Size: 131MiB
[2021-12-03T14:00:49Z INFO  matcher::instructions::project_col] Projecting column RECEIPT_AMOUNT_BASE as DECIMAL
[2021-12-03T14:01:04Z INFO  matcher::instructions::project_col] Projection took 15s 581ms for 1998576 rows (0.008ms/row)
[2021-12-03T14:01:04Z INFO  matcher] Grid Memory Size: 155MiB
[2021-12-03T14:01:04Z INFO  matcher::instructions::project_col] Projecting column TOTAL_AMOUNT_BASE as DECIMAL
[2021-12-03T14:01:14Z INFO  matcher::instructions::project_col] Projection took 9s 790ms for 1998576 rows (0.005ms/row)
[2021-12-03T14:01:14Z INFO  matcher] Grid Memory Size: 161MiB
[2021-12-03T14:01:14Z INFO  matcher::instructions::merge_col] Merging columns into AMOUNT_BASE
[2021-12-03T14:01:15Z INFO  matcher::instructions::merge_col] Merging took 725ms
[2021-12-03T14:01:15Z INFO  matcher] Grid Memory Size: 216MiB
[2021-12-03T14:01:15Z INFO  matcher::instructions::merge_col] Merging columns into REFERENCE
[2021-12-03T14:01:19Z INFO  matcher::instructions::merge_col] Merging took 4s 148ms
[2021-12-03T14:01:19Z INFO  matcher] Grid Memory Size: 241MiB
[2021-12-03T14:01:19Z INFO  matcher::instructions::match_groups] Grouping by REFERENCE
[2021-12-03T14:01:54Z INFO  matcher::instructions::match_groups] Matched 249990 out of 249990 groups. Constraints took 31s 368ms (0.125ms/group)
[2021-12-03T14:01:54Z INFO  matcher] Grid Memory Size: 0B
[2021-12-03T14:01:54Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-03T14:01:54Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-03T14:01:54Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-03T14:01:54Z INFO  matcher] Completed match job 6ec38334-6ba4-413a-9ff0-385c7b753044 in 1m 24s 368ms

## Performance After Retaining Only Matching Key
After retained field overhaul, results were: -
Peak RAM: 252MB
Duration: 1m 33s 270ms
Console output here
[2021-12-07T06:16:00Z INFO  matcher] Starting match job cf23ca80-2769-4700-9175-256097492007
[2021-12-07T06:16:00Z INFO  matcher::folders] Using folder REC_HOME [/home/stef/dev/rust/celerity/matcher/tmp]
[2021-12-07T06:16:00Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-07T06:16:00Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-07T06:16:00Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-07T06:16:00Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.derived.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-07T06:16:00Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.derived.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-07T06:16:00Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.derived.csv] from [./tmp/waiting] to [./tmp/matching]
[2021-12-07T06:16:00Z INFO  matcher] Running charter [Performance] v1637208553000
[2021-12-07T06:16:00Z INFO  matcher::model::grid] Sourcing data with pattern [.*09-BIG-invoices\.csv]
[2021-12-07T06:16:00Z INFO  matcher::model::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-invoices.csv (179.83MiB)
[2021-12-07T06:16:00Z INFO  matcher::model::grid] 250000 records read from file 20211130_065025197_09-BIG-invoices.csv
[2021-12-07T06:16:00Z INFO  matcher::model::grid] Grid Memory Size: 10MB
[2021-12-07T06:16:00Z INFO  matcher::model::grid] Sourcing data with pattern [.*09-BIG-payments\.csv]
[2021-12-07T06:16:00Z INFO  matcher::model::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-payments.csv (531.37MiB)
[2021-12-07T06:16:01Z INFO  matcher::model::grid] 874288 records read from file 20211130_065025197_09-BIG-payments.csv
[2021-12-07T06:16:01Z INFO  matcher::model::grid] Grid Memory Size: 43MiB
[2021-12-07T06:16:01Z INFO  matcher::model::grid] Sourcing data with pattern [.*09-BIG-receipts\.csv]
[2021-12-07T06:16:01Z INFO  matcher::model::grid] Reading file ./tmp/matching/20211130_065025197_09-BIG-receipts.csv (550.14MiB)
[2021-12-07T06:16:01Z INFO  matcher::model::grid] 874288 records read from file 20211130_065025197_09-BIG-receipts.csv
[2021-12-07T06:16:01Z INFO  matcher::model::grid] Grid Memory Size: 76MiB
DERIVED_PATH: "./tmp/matching/20211130_065025197_09-BIG-invoices.derived.csv"
DERIVED_PATH: "./tmp/matching/20211130_065025197_09-BIG-payments.derived.csv"
DERIVED_PATH: "./tmp/matching/20211130_065025197_09-BIG-receipts.derived.csv"
[2021-12-07T06:16:59Z INFO  matcher] Grid Memory Size: 76MiB
[2021-12-07T06:16:59Z INFO  matcher] Grid Memory Size: 76MiB
[2021-12-07T06:16:59Z INFO  matcher] Grid Memory Size: 76MiB
[2021-12-07T06:16:59Z INFO  matcher] Grid Memory Size: 76MiB
[2021-12-07T06:16:59Z INFO  matcher] Grid Memory Size: 76MiB
[2021-12-07T06:16:59Z INFO  matcher::instructions::match_groups] Grouping by REFERENCE
[2021-12-07T06:17:33Z INFO  matcher::instructions::match_groups] Matched 249990 out of 249990 groups. Constraints took 31s 599ms (0.126ms/group)
[2021-12-07T06:17:33Z INFO  matcher] Grid Memory Size: 0B
[2021-12-07T06:17:33Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-07T06:17:33Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-07T06:17:33Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-07T06:17:33Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-invoices.derived.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-07T06:17:33Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-payments.derived.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-07T06:17:33Z INFO  matcher::folders] Moving file [20211130_065025197_09-BIG-receipts.derived.csv] from [./tmp/matching] to [./tmp/archive]
[2021-12-07T06:17:33Z INFO  matcher] Completed match job cf23ca80-2769-4700-9175-256097492007 in 1m 33s 270ms


## Performance After Retaining Only Compacted Matching Key
After retained field overhaul, results were: -
Peak RAM: 
Duration: 
Console output here