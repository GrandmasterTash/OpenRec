# What is a OpenRec?

OpenRec is a reconciliation matching engine written in the Rust language. It can be used to group and match data presented to it in csv format using easy-to-configure rules. OpenRec comprises a number of modules which are intended to be used as libraries and services within your own enterprise-wide solution. Note: OpenRec is a matching engine, not a full solution.

## OpenRec High-Level Features

- **Schema-less** - OpenRec will analyse incoming data at runtime to deduce each column's data-type. The only time you'll need to know a column's data-type is when using it in matching rule. Anything not involved in a rule is dynamically typed.
- **Fast** - OpenRec is written in [Rust](https://www.rust-lang.org/) an as such is very efficient. Typically matching 1-2 million transactions a minute (subject to specific hardware and configuration).
- **Lightweight** - Because OpenRec is written in Rust, it has no start-up time and a very low file size footprint. The matching engine (Celerity) uses an external merge sort algorithm which utilities disk files rather than system RAM to sort and group data. Because of this, you can easily run **any** amount of transactions on a system with barely any memory requirements (< 100MB!).
- **Easy to configure** - OpenRec configuration files (called charters) use a very natural configuration structure which is easy to pick-up and the parser gives great feed-back if mistakes are made.
- **Extendible** - OpenRec configuration utilizes the Lua scripting language to derive calculated fields and evaluate matching rules. Lua is a well documented and OpenRec provides some very handy helper functions which can also be leveraged.

XXXXX
TODO: Example charter here.
XXXXX

You should read through the OpenRec concepts section which follows before jumping in to the <b>[Getting Started]</b> section.

# OpenRec Concepts

A matching system in OpenRec is configured via a single [yaml](https://en.wikipedia.org/wiki/YAML) file called a **charter** and a folder structure where data will be imported and manipulated.

The current OpenRec modules are
- Jetwash
- Celerity
- Sentinal

## Data Flow

Data is imported from csv files in an inbox folder where it is initially cleansed by the Jetwash module which converts them into well-formed CSV files.

These well-formed CSV files are then passed to the Celerity matching engine which will create new files which contain only un-matched data.

When new data arrives and goes through Jetwash, it processed along with any previous un-matched data and new files of un-matched data are written to disk.

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

Files are first delivered into the **inbox** folder. If they match a specific filename pattern in the Jetwash section of the charter, then they will have any column mappings applied to them (data transformations).

The modified files are then renamed to include a timestamp prefix 'YYYYMMDD_HHMMSSsss_' and then moved to the **waiting** folder.

At the same time, the original file is moved to the **archive/jetwash** folder and given a timestamp prefix as well. To avoid collision with previous files which may not have a unique name.

The Celerity engine is then invoked and will move any files which match it's configured filename patterns, into the **matching** folder along with any prior un-matched files in the **unmatched** folder.

The **matching** folder is used to write new derived data and file indexes during a match job. At the end of the job, any unmatched data is written back into the **unmatched** folder and all new files that were in the **waiting** folder are moved to the **archive/celerity** folder.

The **matched** folder contains a YYYYMMDD_HHMMSSsss_matched.json file which details all the match groups made during the job.

### File Formats
Jetwash will convert most standard UTF-8 CSV file formats into a format the Celerity matching engine understands.

This is a fully quoted CSV with a column header row (Jetwash can be configured to add headers to files which don't have them).

Following the header row is a second row which contains the data-types of each column. Jetwash will analyse the columns and produce this row automatically, although it can be overridden.

The data-types used by Celerity are: -

| Data Type     | Abbreviation  | Examples                             |
| ------------- |:-------------:| ------------------------------------ |
| Boolean       | BO            | 1, 0, y, n, true, false              |
| Datetime      | DT            | 2022-01-03T18:23:00.000Z             |
| Decimal       | DE            | 1234567.8901234567                   |
| Integer       | IN            | 0000123                              |
| String        | ST            | Hello There                          |
| UUID          | ID            | 7d1c7f56-6cc2-11ec-aafd-00155dd15c90 |

The final note about the Celerity CSV file format, is that the first column will **always** be called *OpenRecStatus* and contain a single numerical digit. This is used to track a records status.

An example Celerity CSV file: -

```csv
"OpenRecStatus","Invoice No","Ref","Invoice Date","Amount"
"IN","ST","ST","DT","DE"
"0","0001","INV0001","2021-11-25T00:00:00.000Z","1050.99"
"0","0002","INV0002","2021-11-26T00:00:00.000Z","500.00"
```

## Charters
High Level
## Data Sources
### Virtual Grid
### Projections
### Mergers
### Grouping
### Constraints.



## ChangeSets
## Standard Format CSVs





## Modules
### Jetwash
### Celerity
### Sentinel
