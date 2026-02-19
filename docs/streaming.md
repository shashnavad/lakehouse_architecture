# Streaming Data Processing - Phase 6

This document describes the streaming data processing implementation using Kafka and Spark Structured Streaming.

## Overview

The streaming pipeline enables real-time data ingestion:
- **Kafka Producer**: Generates and sends transaction events
- **Spark Structured Streaming**: Consumes from Kafka and writes to Delta Lake
- **Watermarking**: Handles late-arriving data
- **Exactly-Once Semantics**: Ensures data consistency

## Architecture

```
┌─────────────────┐
│ Kafka Producer  │ → Generates transaction events
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Topic    │ → Message queue
└────────┬────────┘
         │
         ▼
┌──────────────────────────┐
│ Spark Structured         │ → Processes streaming data
│ Streaming Consumer       │ → Applies watermarking
└────────┬─────────────────┘
         │
         ▼
┌─────────────────┐
│ Bronze Delta    │ → Delta Lake table
│ Table           │
└─────────────────┘
```

## Components

### Kafka Producer (`scripts/streaming/kafka_producer.py`)

Generates and sends transaction events to Kafka.

**Features:**
- Real-time transaction generation
- Configurable interval and volume
- Continuous or batch mode
- Exactly-once delivery guarantees

**Usage:**
```bash
# Generate 100 transactions with 1 second interval
python scripts/streaming/kafka_producer.py --num-transactions 100 --interval 1.0

# Continuous mode (5 minutes)
python scripts/streaming/kafka_producer.py --continuous --duration 300

# Custom Kafka server
python scripts/streaming/kafka_producer.py --bootstrap-servers localhost:9092 --topic transactions
```

### Spark Structured Streaming Consumer (`scripts/streaming/streaming_consumer.py`)

Consumes from Kafka and writes to Delta Lake Bronze table.

**Features:**
- Watermarking for late-arriving data
- Exactly-once semantics via checkpoints
- Schema enforcement
- Metadata enrichment

**Usage:**
```bash
# Basic streaming consumer
python scripts/streaming/streaming_consumer.py

# Custom configuration
python scripts/streaming/streaming_consumer.py \
    --kafka-servers localhost:9092 \
    --topic transactions \
    --watermark "10 minutes" \
    --checkpoint /tmp/checkpoints

# Aggregated streaming (windowed)
python scripts/streaming/streaming_consumer.py --aggregated --window "1 minute"
```

## Watermarking

Watermarking handles late-arriving data by defining how late data can be:

```python
watermarked_df = df.withWatermark("transaction_timestamp", "10 minutes")
```

- Data arriving within watermark window is processed
- Data arriving after watermark is dropped
- Prevents unbounded state growth

## Exactly-Once Semantics

Achieved through:
1. **Checkpoint Location**: Tracks processed offsets
2. **Idempotent Writes**: Delta Lake ensures no duplicates
3. **Transactional Writes**: ACID guarantees

```python
query = df.writeStream \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .format("delta") \
    .start(table_path)
```

## Quick Start

### 1. Start Kafka

```bash
# Start Kafka and Zookeeper
docker-compose up -d kafka zookeeper

# Or start all services
make start
```

### 2. Start Producer (Terminal 1)

```bash
make kafka-producer

# Or continuous mode
make kafka-producer-continuous
```

### 3. Start Consumer (Terminal 2)

```bash
make streaming-consumer
```

### 4. Run Complete Demo

```bash
python scripts/streaming/streaming_demo.py
```

## Monitoring

### Check Kafka Topics

```bash
# List topics
docker exec lakehouse_kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec lakehouse_kafka kafka-topics --describe --topic transactions --bootstrap-server localhost:9092

# Consume messages (for testing)
docker exec lakehouse_kafka kafka-console-consumer --topic transactions --from-beginning --bootstrap-server localhost:9092
```

### Check Streaming Query Status

The Spark UI shows:
- Processing rate
- Input rate
- Batch duration
- Watermark progress

Access Spark UI: `http://localhost:4040` (when Spark is running)

## Testing

Run streaming tests:
```bash
pytest tests/test_streaming.py -v
```

## Best Practices

1. **Checkpoint Location**: Use persistent storage (not `/tmp`)
2. **Watermark Duration**: Set based on expected lateness
3. **Processing Time**: Balance latency vs throughput
4. **Kafka Configuration**: Use `acks=all` for durability
5. **Monitoring**: Track lag and processing rates

## Troubleshooting

### Kafka Connection Issues
- Verify Kafka is running: `docker ps | grep kafka`
- Check bootstrap servers: `localhost:9092`
- Verify topic exists

### Streaming Query Stops
- Check checkpoint location permissions
- Review Spark logs for errors
- Verify Delta Lake table path exists

### Late Data Not Processed
- Increase watermark duration
- Check transaction timestamps
- Verify watermark column is correct

## Next Steps

After streaming to Bronze:
1. Silver layer processes new Bronze records incrementally
2. Gold layer aggregates streaming data
3. Monitor pipeline metrics

