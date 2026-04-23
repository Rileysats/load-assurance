"""Generate test parquet files."""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

table = pa.table({
    "trade_id": ["T001", "T002", "T003", "T004", "T005"],
    "symbol":   ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"],
    "price":    [150.0, 200.0, 100.0, 300.0, 250.0],
    "quantity": [10, 20, 30, 40, 50],
})

Path("test_data/source").mkdir(parents=True, exist_ok=True)
Path("test_data/target").mkdir(parents=True, exist_ok=True)

pq.write_table(table, "test_data/source/data.parquet")
pq.write_table(table, "test_data/target/data.parquet")

print("Done — test_data/source/ and test_data/target/ created")