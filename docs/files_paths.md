# Files and Paths
Let's take a more detailled look at how files move through the various folders within base directory for a given control.

## Jetwash

TODO:

## Celerity

TODO: Feel this would look better as an image with arrows....

Let's show an example of a file being processed by Celerity and how files are created and moved/removed during the internal phases of a match job.

***Note*** In all the examples below the milliseconds have been removed from the timestamps in the filenames, simply for brevity. In reality, each filename would have 3 more digits in the timestamp representing the milliseconds.

We'll start with a simple case, two files OURS.csv and THEIRS.csv are placed in the waiting folder by Jetwash. Lets say the time is 2022-01-07T05:14:00.000Z. So our initial folder structure prior to invoking Celerity is as follows: -

| waiting                      | matching                     | unmatched         | matched           | archive/celerity |
|------------------------------|------------------------------|-------------------|-------------------|------------------|
| 20220107_051400-OURS.csv     |                              |                   |                   |                  |
| 20220107_051400-THEIRS.csv   |                              |                   |                   |                  |

The time is now 06:25 and we invoke Celerity to match the data.

Initially the files are moved into the matching folder (not the original timestamps are preseved).

| waiting                      | matching                     | unmatched         | matched           | archive/celerity |
|------------------------------|------------------------------|-------------------|-------------------|------------------|
|                              | 20220107_051400-OURS.csv     |                   |                   |                  |
|                              | 20220107_051400-THEIRS.csv   |                   |                   |                  |

Assuming there are some column projections and mergers, then during the initial stages of the match job, a derived data file is created for each corresponding sourced data file.

| waiting           | matching                             | unmatched         | matched           | archive/celerity |
|-------------------|--------------------------------------|-------------------|-------------------|------------------|
|                   | 20220107_051400-OURS.csv             |                   |                   |                  |
|                   | 20220107_051400-OURS.derived.csv     |                   |                   |                  |
|                   | 20220107_051400-THEIRS.csv           |                   |                   |                  |
|                   | 20220107_051400-THEIRS.derived.csv   |                   |                   |                  |


The OURS.csv file contains 2 transactions, but the THEIRS.csv only contains one correspondng transaction. So one pair of records can be matched and another record will be unmatched.

At the start of the matching phase things will look like this (note the matched.json file takes the timestamp from the start of the job): -

| waiting | matching                           | unmatched                                       | matched                                 | archive/celerity |
|---------|------------------------------------|-------------------------------------------------|-----------------------------------------|------------------|
|         | 20220107_051400-OURS.csv           | 20220107_051400-OURS.unmatched.csv.inprogress   | 20220107_062500.matched.json.inprogress |                  |
|         | 20220107_051400-OURS.derived.csv   | 20220107_051400-THEIRS.unmatched.csv.inprogress |                                         |                  |
|         | 20220107_051400-THEIRS.csv         |                                                 |                                         |                  |
|         | 20220107_051400-THEIRS.derived.csv |                                                 |                                         |                  |

During matching, index files are created to perform an external merge sort. Note index.sorted.1 may be repeated index.sorted.nnn depending on data volume and memory settings.

| waiting | matching                           | unmatched                                       | matched                                 | archive/celerity |
|---------|------------------------------------|-------------------------------------------------|-----------------------------------------|------------------|
|         | 20220107_051400-OURS.csv           | 20220107_051400-OURS.unmatched.csv.inprogress   | 20220107_062500.matched.json.inprogress |                  |
|         | 20220107_051400-OURS.derived.csv   | 20220107_051400-THEIRS.unmatched.csv.inprogress |                                         |                  |
|         | 20220107_051400-THEIRS.csv         |                                                 |                                         |                  |
|         | 20220107_051400-THEIRS.derived.csv |                                                 |                                         |                  |
|         | index.sorted.csv                   |                                                 |                                         |                  |
|         | index.sorted.1                     |                                                 |                                         |                  |
|         | index.unsorted.csv                 |                                                 |                                         |                  |

Towards the end of the match job, temporary files and empty unmatched files are removed and other files are finalised and archived.

| waiting     | matching      | unmatched                            | matched                        | archive/celerity             |
|-------------|---------------|--------------------------------------|--------------------------------|------------------------------|
|             |               | 20220107_051400-OURS.unmatched.csv   | 20220107_062500.matched.json   | 20220107_051400-OURS.csv     |
|             |               |                                      |                                | 20220107_051400-THEIRS.csv   |

Lets show a follow-up scenario now a few hours later, where the final THEIRS transaction is delivered.

The file arrives and our folders look like this: -

| waiting                     | matching | unmatched                          | matched                      | archive/celerity           |
|-----------------------------|----------|------------------------------------|------------------------------|----------------------------|
| 20220107_104500-THEIRS.csv  |          | 20220107_051400-OURS.unmatched.csv | 20220107_062500.matched.json | 20220107_051400-OURS.csv   |
|                             |          |                                    |                              | 20220107_051400-THEIRS.csv |

As the match job starts the new data is moved as well as all the un-matched data from any previous job.

| waiting     | matching                           | unmatched                | matched                      | archive/celerity           |
|-------------|------------------------------------|--------------------------|------------------------------|----------------------------|
|             | 20220107_104500-THEIRS.csv         |                          | 20220107_062500.matched.json | 20220107_051400-OURS.csv   |
|             | 20220107_051400-OURS.unmatched.csv |                          |                              | 20220107_051400-THEIRS.csv |

As before inprogress and index files are created during the matching job.

| waiting | matching                           | unmatched                                     | matched                                 | archive/celerity           |
|---------|------------------------------------|-----------------------------------------------|-----------------------------------------|----------------------------|
|         | 20220107_104500-THEIRS.csv         | 20220107_104500-THEIRS.unmatch.csv.inprogress | 20220107_062500.matched.json            | 20220107_051400-OURS.csv   |
|         | 20220107_104500-THEIRS.derived.csv | 20220107_051400-OURS.unmatched.csv.inprogress | 20220107_105000.matched.json.inprogress | 20220107_051400-THEIRS.csv |
|         | 20220107_051400-OURS.unmatched.csv |                                               |                                         |                            |
|         | 20220107_051400-OURS.derived.csv   |                                               |                                         |                            |
|         | index.sorted.csv                   |                                               |                                         |                            |
|         | index.sorted.1                     |                                               |                                         |                            |
|         | index.unsorted.csv                 |                                               |                                         |                            |

And finally at the end of the job, assuming all data was not matched, the folder structure will look like this: -

| waiting | matching  | unmatched | matched                        | archive/celerity           |
|---------|-----------|-----------|--------------------------------|----------------------------|
|         |           |           | 20220107_062500.matched.json   | 20220107_051400-OURS.csv   |
|         |           |           | 20220107_105000.matched.json   | 20220107_051400-THEIRS.csv |
|         |           |           |                                | 20220107_104500-THEIRS.csv |


### ChangeSets


## Sentinal

TODO: