# What is a OpenRec?

OpenRec is a reconciliation matching engine written in the Rust language. It can be used to group and match data presented to it in csv format using easy-to-configure rules. OpenRec comprises a number of modules which are intended to be used as libraries and services within your own enterprise-wide solution.

The motivation for this project was simply personal, I wished to solve the problem of writing a matching engine in a way that wouldn't consume system (memory) resources. As a result you should take note: OpenRec is a matching engine, not a full solution.

## OpenRec High-Level Features

- **Schema-less** - OpenRec will analyse incoming data at runtime to deduce each column's data-type. The only time you'll need to know a column's data-type is when using it in matching rule. Anything not involved in a rule is dynamically typed.
- **Fast** - OpenRec is written in [Rust](https://www.rust-lang.org/) an as such is very efficient. Typically matching 1-2 million transactions a minute (subject to specific hardware and configuration).
- **Lightweight** - Because OpenRec is written in Rust, it has no start-up time and a very low file size footprint. The matching engine (Celerity) uses an external merge sort algorithm which utilities disk files rather than system RAM to sort and group data. Because of this, you can easily run **any** number of transactions on a system with barely any memory requirements (GBs of transactions can typically use less than 100MB of RAM!).
- **Easy to configure** - OpenRec configuration files (called charters) use a very natural configuration structure which is easy to pick-up and the parser gives great feed-back if mistakes are made.
- **Extendible** - OpenRec configuration utilizes the Lua scripting language to derive calculated fields and evaluate matching rules. Lua is a well documented and OpenRec provides some very handy helper functions which can also be leveraged (more detail in the examples).

You should read through the OpenRec concepts section which follows before jumping in to the <b>[Getting Started]</b> section.

# OpenRec Concepts

A matching control in OpenRec is configured via a single [yaml](https://en.wikipedia.org/wiki/YAML) file called a **charter** and requires a folder structure where data will be imported and manipulated.

The current OpenRec modules are
- **Sentinal** - :warning: TODO: Sentinal doesn't exist yet. When it does, it is what detects new data-files and invokes Jetwash, then Celerity. Sentinal is also responsible for ensuring only one match job per control is invoked.
- **Jetwash** - Jetwash cleans the data, trim padded fields, converts dates to ISO8601 (RFC3339) format, adds a schema row (see [File Formats][File Formats] below) and delivers well-formatted data to Celerity.
- **Celerity** - The matching engine takes new data and combines it with previous unmatched data to evaluate groups against defined rules. Matched data is 'released' leaving only un-matched data behind.

## Data Flow

Data is imported from csv files in an inbox folder where they are initially cleansed by the Jetwash module which converts them into well-formed CSV files.

These well-formed CSV files are then used by the Celerity matching engine which group and archive data which passes certain rules. Only un-matched data will remain.

When subsequent new data arrives and goes through Jetwash, it is processed along with any previous un-matched data and new files of un-matched data are written to disk.

This unmatched data can also be modified via audited changeset files. These are files of instructions which can alter or remove un-matched data. For example, maybe a decimal point is out of place meaning a group of data won't match. A changeset can be provided to correct the value causing the data to match and be released.

### Folder Structure

Each charter will operate in it's own folder structure. These folders should be kept separate for different controls being matched. A typical folder structure looks as follows: -

```
  .
  .
  ├── control_a
  |   ├── inbox
  |   ├── archive
  |   |   ├── jetwash
  |   |   └── celerity
  |   ├── waiting
  |   ├── unmatched
  |   ├── matching
  |   └── matched
  ├── control_b
  |   ├── inbox
  .   .
  .   .
```

Files are first delivered into the **inbox** folder. If they match a specific filename pattern in the Jetwash section of the charter, then they will have any defined column mappings applied to them (data transformations).

The modified files are then renamed to include a timestamp prefix 'YYYYMMDD_HHMMSSsss_' and then moved to the **waiting** folder.

At the same time, the original file is moved to the **archive/jetwash** folder and given a timestamp prefix as well, to avoid collision with previously received files which may not have a unique name.

The Celerity engine is then invoked and will move any files which match it's configured filename patterns, into the **matching** folder and also move any prior un-matched files which match the configured filename patterns from the **unmatched** folder into the **matching** folder.

The **matching** folder is used to write new derived data and file indexes during a match job. At the end of the job, any unmatched data is written back into the **unmatched** folder and all new files that were in the **waiting** folder are moved to the **archive/celerity** folder.

The **matched** folder contains a YYYYMMDD_HHMMSSsss_matched.json file which details all the match groups made during the job.

### File Formats
Jetwash will convert most standard UTF-8 CSV file formats into a format the Celerity matching engine understands.

This is a fully quoted CSV with a column header row (Jetwash can be configured to add headers to files which don't initially have them).

Following the header row is a second row which contains the data-types of each column. Jetwash will analyse the columns as new data arrives and produce this row automatically, although it can be overridden.

The data-types used by Celerity are: -

| Data Type     | Abbreviation  | Examples                             |
| ------------- |:-------------:| ------------------------------------ |
| Boolean       | BO            | 1, 0, y, n, true, false              |
| Datetime      | DT            | 2022-01-03T18:23:00.000Z             |
| Decimal       | DE            | 1234567.8901234567                   |
| Integer       | IN            | 0000123                              |
| String        | ST            | Hello There                          |
| UUID          | ID            | 7d1c7f56-6cc2-11ec-aafd-00155dd15c90 |

A final point about the Celerity CSV file format, is that the first column will **always** be called *OpenRecStatus* and contain a single numerical digit. This is used to track a record's status (i.e. matched or unmatched).

Here is an example Celerity CSV file, 20220103_18400000_invoices.csv: -

```csv
"OpenRecStatus","Invoice No","Ref","Invoice Date","Amount"
"IN","ST","ST","DT","DE"
"0","0001","INV0001","2021-11-25T00:00:00.000Z","1050.99"
"0","0002","INV0002","2021-11-26T00:00:00.000Z","500.00"
```

## Charters

Charters are yaml configuration files used to define rules for Jetwash and Celerity. An example is shown here, don't worry about the details, that is explained in the examples folders (which you are encouraged to work through): -

```yaml
name: Example Control
description: Use this to describe the external systems being matched.
version: 1

jetwash:
  source_files:
   - pattern: ^DealExport\.csv$
     column_mappings:
      - trim: DealRef
      - dmy: EntryDate
      - dmy: DealDate
      - dmy: ValueDate
      - dmy: MaturityDate

   - pattern: ^InternalTrades\.csv$
     column_mappings:
      - dmy: settlement_date
      - dmy: value_date
      - dmy: maturity_date
      - dmy: expiry_date
      - dmy: opening_date


matching:
  source_files:
   - pattern: .*DealExport.*\.csv
     field_prefix: THEIRS

   - pattern: .*InternalTrades.*\.csv
     field_prefix: OURS

  instructions:
    - project:
        column: OUR_DEALREF
        as_a: String
        from: string.match(record["OURS.params"], ".*%s.*%s.*%s(.*)")
        when: record["META.prefix"] == "OURS"

    - merge:
        columns: ['THEIRS.DealRef', 'OUR_DEALREF']
        into: DEAL_REF

    - merge:
        columns: ['THEIRS.Principal', 'OURS.amount']
        into: AMOUNT

    - group:
        by: ['DEAL_REF']
        match_when:
         - nets_to_zero:
            column: AMOUNT
            lhs: record["META.prefix"] == "THEIRS"
            rhs: record["META.prefix"] == "OURS"

```

We'll discuss some of the concepts shown in the above example now.

TODO: Put virtual grid section in own .MD file.

### Virtual Grid

Focusing on the *matching* section of the above file, you'll see it starts with *source_files*. You can define regular expressions here to match any filenames you want to import into the system. There is no limit on the number of patterns, so you may define 3 for example, for a 3-way reconciliation.

Each pattern may match against zero or more files present in the waiting (and unmatched) folders. You can think of these files as being loaded into a single virtual memory grid (think Excel worksheet) which Celerity uses to sort and group.

Each file is given a *field_prefix* value. This value is appended to each field name to ensure it doesn't conflict with other fields from other files sharing the same name. In the example above, all fields from the DealExport.csv files will be prefixed with 'THEIRS' (so 'THEIRS.Principal' is a column for example) and all fields from the InternalTrades.csv files will be prefixed with 'OURS' (so 'OURS.maturity_date' is a column for example).

As an example, lets take an simple invoice file and a payment file (not related to the above charter - just some new files to illustrate) and put them both in a virtual grid.

#### Invoice File

The individual invoice file looks like this: -

```csv
"Ref","InvoiceDate","Amount"
"ST","DT","DE"
"INV0001","2021-11-25T00:00:00.000Z","1050.99"
"INV0002","2021-11-26T00:00:00.000Z","500.00"
```

#### Payment File

The individual payment file looks like this: -

```csv
"PaymentId","Ref","Amount","PaymentDate"
"ST","ST","DE","DT"
"P1","INV0001","50.99","2021-11-27T00:00:00.000Z"
"P2","INV0002","500.00","2021-11-27T00:00:00.000Z"
"P3","INV0001","1000.00","2021-11-28T00:00:00.000Z"
```

If we had a charter which sourced files like this: -

```yaml
matching:
  source_files:
   - pattern: .*invoices.*\.csv
     field_prefix: INV

   - pattern: .*payments.*\.csv
     field_prefix: PAY

  instructions:
    - merge:
        columns: ['INV.Amount', 'PAY.Amount']
        into: AMOUNT

    - merge:
        columns: ['INV.Ref', 'PAY.Ref']
        into: REF

    - group:
        by: ['REF']
        match_when:
          - nets_to_zero:
              column: AMOUNT
              lhs: record["META.prefix"] == "PAY"
              rhs: record["META.prefix"] == "INV"
```

Then loading these files into the virtual grid, would result in a dataset which looked like this: -

```
INV.Ref  INV.InvoiceDate           INV.Amount  PAY.PaymentId  PAY.Ref  PAY.Amount  PAY.PaymentDate
INV0001  2021-11-25T00:00:00.000Z     1050.99  -              -                 -  -
INV0002  2021-11-26T00:00:00.000Z      500.00  -              -                 -  -
-        -                                  -  P1             INV0001       50.99  2021-11-27T00:00:00.000Z
-        -                                  -  P2             INV0002      500.00  2021-11-27T00:00:00.000Z
-        -                                  -  P3             INV0001     1000.00  2021-11-28T00:00:00.000Z
```

From this stage, the charter has instructions which tell Celerity how to bring columns together so that rows can be grouped and matched.

```yaml
- merge:
    columns: ['INV.Amount', 'PAY.Amount']
    into: AMOUNT
```

After this first merge instruction, the above grid would be modified to look like the grid below. You can see the 'Amount' from both files has be merged into a single column call AMOUNT.

```
 AMOUNT  INV.Ref  INV.InvoiceDate           INV.Amount  PAY.PaymentId  PAY.Ref  PAY.Amount  PAY.PaymentDate
1050.99  INV0001  2021-11-25T00:00:00.000Z     1050.99  -              -                 -  -
 500.00  INV0002  2021-11-26T00:00:00.000Z      500.00  -              -                 -  -
  50.99  -        -                                  -  P1             INV0001       50.99  2021-11-27T00:00:00.000Z
 500.00  -        -                                  -  P2             INV0002      500.00  2021-11-27T00:00:00.000Z
1000.00  -        -                                  -  P3             INV0001     1000.00  2021-11-28T00:00:00.000Z
```

```yaml
- merge:
    columns: ['INV.Ref', 'PAY.Ref']
    into: REF
```

After the final merge REF instruction from the charter the grid will look as follows: -

```
REF       AMOUNT  INV.Ref  INV.InvoiceDate           INV.Amount  PAY.PaymentId  PAY.Ref  PAY.Amount  PAY.PaymentDate
INV0001  1050.99  INV0001  2021-11-25T00:00:00.000Z     1050.99  -              -                 -  -
INV0002   500.00  INV0002  2021-11-26T00:00:00.000Z      500.00  -              -                 -  -
INV0001    50.99  -        -                                  -  P1             INV0001       50.99  2021-11-27T00:00:00.000Z
INV0002   500.00  -        -                                  -  P2             INV0002      500.00  2021-11-27T00:00:00.000Z
INV0001  1000.00  -        -                                  -  P3             INV0001     1000.00  2021-11-28T00:00:00.000Z
```

Now we have two columns we can use to group data and test the groups are valid matches.

```yaml
- group:
    by: ['REF']
    match_when:
      - nets_to_zero:
          column: AMOUNT
          lhs: record["META.prefix"] == "PAY"
          rhs: record["META.prefix"] == "INV"
```

This final instruction does two things, it groups the data by the REF column resulting in a grid like this: -

```
REF       AMOUNT  INV.Ref  INV.InvoiceDate           INV.Amount  PAY.PaymentId  PAY.Ref  PAY.Amount  PAY.PaymentDate
INV0001  1050.99  INV0001  2021-11-25T00:00:00.000Z     1050.99  -              -                 -  -
INV0001    50.99  -        -                                  -  P1             INV0001       50.99  2021-11-27T00:00:00.000Z
INV0001  1000.00  -        -                                  -  P3             INV0001     1000.00  2021-11-28T00:00:00.000Z
-----------------------------------------------------------------------------------------------------------------------------
INV0002   500.00  INV0002  2021-11-26T00:00:00.000Z      500.00  -              -                 -  -
INV0002   500.00  -        -                                  -  P2             INV0002      500.00  2021-11-27T00:00:00.000Z
```

Then it runs one or more constraint rules to see if the group is a valid match, in this case there is only one rule, a nets_to_zero rule (other rules are covered in the examples section).

Netting-to-zero is shorthand for the following calculation (using the above example): -
```
  sum(abs(invoice amount)) - sum(abs(payment amount)) => must equal zero
```
In the example above, you can see that both groups will be matched and released from the system.

As mentioned, there are other constraint rules available, netting with tolerance and you can even write your own custom Lua constraints for scenarios where transactions may have multiple fields involved (ours/theirs flags, etc.). These are covered in more detail in the examples folder.


### Projections

### Mergers

### Grouping

### Constraints

## ChangeSets

# Getting Started

## Examples