#!/usr/bin/env python3
"""
Streaming Demo: Complete end-to-end streaming pipeline
Demonstrates Kafka producer + Spark Structured Streaming consumer
"""

import subprocess
import time
import sys
import os
import signal

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n⚠ Stopping streaming demo...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def check_kafka_running():
    """Check if Kafka is running"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=lakehouse_kafka", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return "lakehouse_kafka" in result.stdout
    except Exception:
        return False

def main():
    """Run streaming demo"""
    print("=" * 60)
    print("STREAMING DEMO: Kafka + Spark Structured Streaming")
    print("=" * 60)
    
    # Check Kafka
    if not check_kafka_running():
        print("⚠ Kafka container not running!")
        print("   Start it with: docker-compose up -d kafka zookeeper")
        print("   Or start all services: make start")
        sys.exit(1)
    
    print("✓ Kafka is running")
    print("\nThis demo will:")
    print("1. Start Kafka producer (generates transactions)")
    print("2. Start Spark streaming consumer (writes to Bronze)")
    print("\nPress Ctrl+C to stop\n")
    
    time.sleep(2)
    
    # Start producer in background
    print("🚀 Starting Kafka producer...")
    producer_process = subprocess.Popen(
        [sys.executable, "scripts/streaming/kafka_producer.py", 
         "--num-transactions", "50", "--interval", "0.5"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    time.sleep(2)
    
    # Start consumer
    print("🚀 Starting Spark streaming consumer...")
    print("   (This will run until stopped with Ctrl+C)\n")
    
    consumer_process = subprocess.Popen(
        [sys.executable, "scripts/streaming/streaming_consumer.py"],
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    
    try:
        # Wait for processes
        producer_process.wait()
        consumer_process.wait()
    except KeyboardInterrupt:
        print("\n⚠ Stopping processes...")
        producer_process.terminate()
        consumer_process.terminate()
        producer_process.wait()
        consumer_process.wait()
        print("✓ Demo stopped")

if __name__ == "__main__":
    main()

