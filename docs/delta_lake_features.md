# Delta Lake Features - Phase 5

This document describes the Delta Lake advanced features implemented in Phase 5.

## Overview

Delta Lake provides enterprise-grade features on top of data lakes:
- **ACID Transactions**: Ensures data consistency
- **Time Travel**: Query historical versions
- **Schema Evolution**: Add columns without breaking existing queries
- **File Optimization**: OPTIMIZE and VACUUM commands

## Time Travel

### Query by Version

```python
from scripts.delta_features.delta_lake_features import time_travel_by_version

# Query version 0 (oldest/first load)
historical_df = time_travel_by_version(spark, table_path, 0)
historical_df.show()
```

### Query by Timestamp

```python
from scripts.delta_features.delta_lake_features import time_travel_by_timestamp

# Query state at specific point in time
df = time_travel_by_timestamp(spark, table_path, "2024-01-15 10:00:00")
```

### View Table History

```python
from scripts.delta_features.delta_lake_features import get_table_history

get_table_history(spark, table_path)
```

### Restore to Previous Version

```python
spark.sql(f"RESTORE TABLE delta.`{table_path}` TO VERSION AS OF {version}")
```

## ACID Transactions

Delta Lake guarantees:
- **Atomicity**: All operations succeed or none do
- **Consistency**: Valid state before and after
- **Isolation**: Concurrent reads see consistent data
- **Durability**: Committed data persists

Run the ACID demonstration:
```bash
python scripts/delta_features/delta_lake_features.py --acid
```

## MERGE Operations

Already implemented in Silver and Gold layers for upserts:

```python
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

## Schema Evolution

Add new columns without breaking existing queries:

```python
from scripts.delta_features.delta_lake_features import demonstrate_schema_evolution

demonstrate_schema_evolution(
    spark, table_path,
    new_columns={"data_quality_score": 100, "is_verified": False}
)
```

Or when writing:
```python
df.write.format("delta").option("mergeSchema", "true").save(table_path)
```

## OPTIMIZE

Compact small files for better query performance:

```bash
# Optimize all layers
make delta-optimize

# Or via Python
python scripts/delta_features/delta_lake_features.py --optimize
```

### Z-Ordering

Co-locate related data for faster scans:

```python
spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY (transaction_id, customer_id)")
```

## VACUUM

Remove old files beyond retention period. **Warning**: Disables time travel for old versions.

```bash
# VACUUM with 7-day retention (default)
python scripts/delta_features/delta_lake_features.py --vacuum

# Custom retention (e.g., 24 hours)
python scripts/delta_features/delta_lake_features.py --vacuum --retention 24
```

## Command Reference

| Command | Description |
|---------|-------------|
| `make delta-features` | Run full Delta Lake features demo |
| `make delta-optimize` | Run OPTIMIZE on all tables |
| `--time-travel` | Time travel demo only |
| `--acid` | ACID demonstration only |
| `--optimize` | OPTIMIZE only |
| `--vacuum` | VACUUM (use with caution) |
| `--retention N` | VACUUM retention in hours |

## Best Practices

1. **Time Travel**: Use for debugging, auditing, point-in-time recovery
2. **OPTIMIZE**: Run periodically (e.g., daily) after heavy writes
3. **VACUUM**: Only when you're sure you don't need old versions
4. **Schema Evolution**: Use for adding optional columns, not removing
5. **MERGE**: Prefer over overwrite for incremental updates

