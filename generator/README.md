## Experimentation around a Rust reconcilliation engine and gRPC server.
TODO: Tool to generate psuedo realistic CSVs.
TODO: Load files into memory with string compaction.
TODO: Schema files with keys.
TODO: Generate/apply changesets from file deltas (full file refresh or a file with just deltas).
TODO: Handle schema modifications.
TODO: Projection and grouping to find matches.
TODO: Create a gRPC server to stream data to clients and be notified of changes.
TODO: Currency matching. Requires number (trade value), currency and decimal (fx rate at instant) OR number, currency and datetime (so an external FX rate can be consulted). Leaning on making it ALWAYS the former to simplify.