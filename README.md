# S3 Bucket to Parquet

Load all json files from a s3 bucket subfolder and write them as a parquet file.

## Just4Fun Comparing Python and Rust and GO

Benchmark on reading 261 json files and writing them as a single parquet file.

|Tool|Time|
|---|---|
|Rust|19.86s|
|Python|4.65s|
|Go|3.29s|
