# jetwash
A data scrubbing and preparation component used to convert bespoke csv files into csv files for the celerity matching engine.

## TODOs
- Add an OpenRec_UUID column.
- lookup(get_me_col, where_col, equals_this), trim(), decimal(), datetime(y, m, d, h, m, s, mi) functions provided.
- section for global Lua functions - to be used in a field converter.

Happy path flow: -
INBOX          ORIGINAL       WAITING
Wibble                                                                    <<<< External system is writing the data file.

Wibble.ready                                                              <<<< External system renames file to indicate writing is complete.

Wibble.processing            20211222_170930000_Wibble.csv.inprogress     <<<< Jetwash is converting the data for celerity.

               Wibble        20211222_170930000_Wibble.csv                <<<< Jetwash is done. Over to Celerity now.

