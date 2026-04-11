# Iceberg Structured Streaming Writes — Reference

Source: https://iceberg.apache.org/docs/latest/spark-structured-streaming/

## Direct Write (append mode)

```python
stream_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/path/to/checkpoints") \
    .toTable("catalog.namespace.table")
```

## foreachBatch Approach

Use when you need custom logic per micro-batch (e.g., merge/upsert):

```python
def process_batch(df, epoch_id):
    df.writeTo("catalog.namespace.table").append()

stream_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/path/to/checkpoints") \
    .start()
```

## Table Creation (Spark SQL)

```sql
CREATE TABLE IF NOT EXISTS catalog.namespace.table (
    col1 STRING NOT NULL,
    col2 BIGINT NOT NULL,
    col3 TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(col3), col1)
```

Set sort order separately:
```sql
ALTER TABLE catalog.namespace.table WRITE ORDERED BY col3 ASC
```

## Key Options

| Option | Description |
|--------|-------------|
| `fanout-enabled` | Set to `true` to avoid sorting before write (reduces latency for partitioned tables) |
| `checkpointLocation` | Required for streaming — stores offsets and state |

## Notes

- `outputMode("append")` is the standard mode for streaming writes to Iceberg
- `foreachBatch` provides at-least-once guarantees by default; use `batchId` for dedup if needed
- `toTable()` is preferred over `.option("path", ...).start()` for catalog-managed tables
- Checkpoint location should be on a persistent volume for container restart recovery
