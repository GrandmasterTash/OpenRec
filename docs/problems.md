# Unknown Data Types

There are two features of OpenRec which, together, can lead to a scenario where a match job fails with a validation error and the root cause is due to an unknown data type.

The first feature lies within Jetwash. Jetwash will scan every incoming file and ascertain it's schema. This means it introspects every cell across every row and column to determine each column's data-type. This schema is then written as the 2nd header row to any file passed on to the Celerity matching engine.

However, when an inbound file contains an empty column, Jetwash cannot infer the datatype that the column would *normally* contain. In this case, the schema row will contain '??' for such a column denoting the type is not known. So long as this column is not referenced in a matching instruction, this may not, initially cause any problems.

The second feature lies within Celerity. Celerity mandates that every file loaded from a given `source_files->pattern` (config in the charter) must have the same schema - this is to ensure the virtual grid is consistent from row to row.

### Side-Effects

Let's draw-up a scenario where these two features, together, cause us an unexpected problem. Imagine the following two files are loaded and put through a match job.

```csv
"Invoice No","Ref","Invoice Date","Amount","Thing"
"0001","INV0001","2021-11-25","1050.99","11"
"0002","INV0002","2021-11-26","500.00","22"
```

```csv
"PaymentId","Ref","Amount","Payment Date"
"P1","INV0001","1050.99","25/11/2021"
```

The schemas will be analysed and appended by Jetwash and the first invoice will be matched against the payment P1 leaving the second invoice in an invoices-unmatched.csv file looking like this (the OpenRecxxx fields have been omitted): -

```csv
"Invoice No","Ref","Invoice Date","Amount","Thing"
"IN","ST","DT","DE","IN"
"0002","INV0002","2021-11-26","500.00","22"
```

Each column has a known data-type as expected which is shown in the 2nd header row - IN(teger), ST(ring), D(ate)T(ime) and DE(cimal).

Now let's say we load these two files of data, the invoice file contains a new invoice and the payments file contains a payment for the new invoice and another for our un-matched invoice.

```csv
"Invoice No","Ref","Invoice Date","Amount","Thing"
"0003","INV0003","2021-11-28","550.00",""
```

```csv
"PaymentId","Ref","Amount","Payment Date"
"P2","INV0002","500.00","26/11/2021"
"P3","INV0003","550.00","28/11/2021"
```

In this second round, the invoice 'Thing' column is empty so Jetwash won't know the data-type of this column. The file presented to Celerity will look like this: -

```csv
"Invoice No","Ref","Invoice Date","Amount","Thing"
"IN","ST","DT","DE","ST"
"0003","INV0003","2021-11-28","550.00",""
```

Notice that the 'Thing' column has been altered to a ST(ring).

Now when Celerity is invoked, the charter configuration which looks like this: -

```yaml
  source_files:
    - pattern: .*invoices.*\.csv
      field_prefix: INV
```

Will suddenly try to load the unmatched file: -

```csv
"Invoice No","Ref","Invoice Date","Amount","Thing"
"IN","ST","DT","DE","IN"
"0002","INV0002","2021-11-26","500.00","22"
```

and the new data file: -

```csv
"Invoice No","Ref","Invoice Date","Amount","Thing"
"IN","ST","DT","DE","ST"
"0003","INV0003","2021-11-28","550.00",""
```

Which will fail as the 'Thing' column has a data-type mismatch.

### Problem Prevention

Let's first look at how this problem can be prevented from occurring.

If you can identify columns of this nature at the outset (i.e. the inbound data may or may not be present), then you can set-up a column mapping in Jetwash to force the data-type of the resultant column.

```yaml
jetwash:
  source_files:
   - pattern: ^01-invoices.*\.csv$
     column_mappings:
      - map:
          column: Thing
          as_a: Integer
          from: |
            if value == nil or value == "" then
              return 0
            else
              return tonumber(value)
            end
```

This is a rather long-winded way of forcing the column to an integer using lua. So there's a better, more concise option: -

```yaml
jetwash:
  source_files:
   - pattern: ^01-invoices.*\.csv$
     column_mappings:
      - as_integer: Thing
```

Now, the column will always be presented to Celerity as an IN(teger) and the match job will fail if this is not possible (due
to an incompatible value).

### Problem Resolution - Back-out and Reloading

Not let's look at what to do, if we get into this situation - as we didn't anticipate it. If the above prevention was not in place, then we would have a match job that will always error because the data files are sat in the matching folder.

In this scenario, you can create a changeset to ignore the problematic file, run the match job to release those records.

An example changeset might look like this: -

```json
[
    {
        "id": "f3377a6c-6324-11ec-bc4d-00155ddc3e05",
        "change": {
            "type": "IgnoreFile",
            "filename": "20220118_084109873_01-invoices-b.csv"
        },
        "timestamp": "2021-12-20T06:18:00.000Z"
    }
]
```

Running a match job with a changeset in the inbox, will always apply the changeset to the data before commencing the match job. So in this case all the records from the offending file will be ignored.

Now you can amend the charter with the above solution (for example) and load the data file once again allowing the schemas to match.