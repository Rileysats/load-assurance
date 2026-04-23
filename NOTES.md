row count - use metadta - s3 parquet footer has row count per file
schema - parquet footer (read schema only)
null rate / value checks - sample based - read 1-5% of rows
checksum - tricky - either sample-based hash or hash at write time and store it as metadata
