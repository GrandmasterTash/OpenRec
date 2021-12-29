# jetwash
A data scrubbing and preparation component used to convert bespoke csv files into csv files for the celerity matching engine.

## TODOs
- Fixed schema or adaptive?
- Add an OpenRec_UUID column.
- Adaptive MUST have column headers already.
- Adaptive must scan entire file to ascertain schema.
- Adaptive Date recognition and conversion. Allow overrides.
- Ad-hoc column converters written in lua. eg.   s:upper() -> uppercase.
- lookup(get_me_col, where_col, equals_this), trim(), decimal(), datetime(y, m, d, h, m, s, mi) functions provided.
- Have lookup remember the last 5 values.
- section for global Lua functions - to be used in a field converter.
- Fixed will reject files that don't conform.
- Lookups from other csv files.
- Archive original files.
- Double-quote.
- Write to waiting/xxxx.inprogress then rename.
- Changeset generation for deltas - needs composite keys defining.
- Jetwash config as part of charter.

Happy path flow: -
INBOX          ORIGINAL       WAITING
Wibble                                                                    <<<< External system is writing the data file.

Wibble.ready                                                              <<<< External system renames file to indicate writing is complete.

Wibble.processing            20211222_170930000_Wibble.csv.inprogress     <<<< Jetwash is converting the data for celerity.

               Wibble        20211222_170930000_Wibble.csv                <<<< Jetwash is done. Over to Celerity now.


Ideas for config: -
---
jetwash:
  - file_pattern: '^Wibble$'
    # Optionally add a header row if the source file doesn't have headers.
    add_header_row: [ "Column1", "Column2" ]

    # TODO: Debating if the mappings aren't just replicating project logic already present. NO THEY ARE NOT
    # e.g. 201201 -> might actually mean 1st dec 2020. Important to be able to have some helper fns in Lua to constust a date from this. i.e. date('20' .. yr, mnth, day) where yr mnth day have been parsed from above.
    # TODO: Add a trim, and lookup fn to projections....

    # Map columns - only specify columns that are altered, any columns not specified are imported as-is.
    column_mappings:
       # Take a value like "28/5/2020" and turn into a date.
     - map: "Date1"
       mapping: ymd("(.*)/(.*)/(.*)")
       as_a: Datetime

       # Take a value like "28/5/2020 12:30a" and turn into a datetime.
     - map: "Date2"
       mapping: ymd_hms("(.*)/(.*)/(.*) (.*):(.*)a")
       as_a: Datetime
       # If the column exists then the mapping script is given access to the 'value' of that column for each row.
     - map: "Column1"
       # Convert whatever the source value is to upper case.
       mapping: value:ucase()
       as_a: String
     - map: "Column2"
       # Trip white space from the start and end of the value - trim is a provided helper function.
       mapping: trim(value)
       as_a: String
       # If a named column doesn't exist in the source file (or add_header_row) it is appended to the right of existing columns as a new column - these kind of mappings will be given access to the record[] Lua table.
     - map: "NewColumn_FXRate"
       # Find a value from another CSV file in the 'lookups' folder. In the example below, we are getting the exchange
       # rate for each row, using the Currency column's value. For each value, we get the value in the FXRate column
       # from the file usd_fxrates.csv. i.e. we get the rate to convert to USD and we create a new column called NewColumn_FXRate.
       # (in.csv, get_csv_col, where_record_col, is_value)
       mapping: lookup("usd_fxrates.csv", "FXRate", "Currency", record["Currency"]:ucase())
       as_a: Decimal
    # Optional key - when specified, if new records also exist in the unmatched folder (matching the record by this composite key) then the new record is dropped and a changeset is generated with any deltas to apply to the existing record to make it look like this record.
    # If multiple unmatched record are found an error occurs as the composite key is not specific enough.
    changeset_composite_keys: [ "Column1", "Column2" ]
