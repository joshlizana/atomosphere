# Spark Python DataSource V2 — Streaming API Reference

Source: https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html

## DataSource Class

```python
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import StructType

class MyDataSource(DataSource):
    @classmethod
    def name(cls):
        return "my_source"

    def schema(self):
        return "col1 string, col2 long"  # or StructType

    def simpleStreamReader(self, schema: StructType):
        return MyStreamReader(schema, self.options)
```

## SimpleDataSourceStreamReader Contract

```python
class MyStreamReader(SimpleDataSourceStreamReader):
    def initialOffset(self) -> dict:
        """Return the initial start offset of the reader."""
        return {"offset": 0}

    def read(self, start: dict) -> (Iterator[Tuple], dict):
        """
        Takes start offset as input.
        Returns (iterator_of_tuples, next_offset_dict).
        Called each micro-batch.
        """
        ...

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """
        Deterministic replay between offsets.
        Called during restart or after failure.
        """
        ...

    def commit(self, end: dict) -> None:
        """
        Called when query finishes processing data before end offset.
        Use to clean up resources.
        """
        pass
```

## Registration and Usage

```python
spark.dataSource.register(MyDataSource)

query = (
    spark.readStream
    .format("my_source")
    .load()
    .writeStream
    .format("iceberg")
    .start()
)
```

## Key Contract Notes

- **Offset = dict**: Offsets are Python dicts, serialized as JSON internally
- **read() returns (Iterator[Tuple], dict)**: data rows + next offset
- **readBetweenOffsets()**: MUST return same data for same offset range (deterministic replay)
- **All classes must be pickle-serializable**: import external libs inside methods
- **SimpleStreamReader**: no partitioning, single reader — suitable for WebSocket sources
- **DataSourceStreamReader**: use for partitioned/high-throughput sources (has latestOffset + partitions)
