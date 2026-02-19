"""
Kafka Producer: Generate and send real-time transaction data to Kafka
Simulates real-time e-commerce transactions
"""

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os

# Note: We don't import ECommerceDataGenerator to avoid dependency
# Instead, we implement transaction generation inline


class TransactionKafkaProducer:
    """Produce transaction events to Kafka"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "transactions"):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        
        # Product and customer IDs for realistic data
        self.product_ids = [f"PROD_{i+1:05d}" for i in range(500)]
        self.customer_ids = [f"CUST_{i+1:05d}" for i in range(1000)]
        self.payment_methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash on Delivery"]
        self.statuses = ["Completed", "Pending", "Cancelled"]
    
    def connect(self):
        """Connect to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1  # Ensure ordering
            )
            print(f"✓ Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            raise
    
    def generate_transaction(self, transaction_id: str) -> dict:
        """Generate a single transaction event"""
        transaction_date = datetime.now()
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)
        total_amount = round(quantity * unit_price, 2)
        
        # Apply discount occasionally
        discount = 0
        if random.random() < 0.2:  # 20% chance
            discount = round(total_amount * random.uniform(0.05, 0.25), 2)
        
        final_amount = round(total_amount - discount, 2)
        
        return {
            "transaction_id": transaction_id,
            "customer_id": random.choice(self.customer_ids),
            "product_id": random.choice(self.product_ids),
            "transaction_date": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount": discount,
            "final_amount": final_amount,
            "payment_method": random.choice(self.payment_methods),
            "status": random.choice(self.statuses),
            "shipping_address": f"{random.randint(1, 999)} Main St, City {random.randint(1, 50)}"
        }
    
    def send_transaction(self, transaction: dict):
        """Send transaction to Kafka"""
        try:
            # Use transaction_id as key for partitioning
            future = self.producer.send(
                self.topic,
                key=transaction["transaction_id"],
                value=transaction
            )
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"✗ Failed to send transaction: {e}")
            return False
    
    def produce_stream(self, num_transactions: int = 100, interval: float = 1.0):
        """
        Produce a stream of transactions
        
        Args:
            num_transactions: Number of transactions to produce
            interval: Seconds between transactions
        """
        if not self.producer:
            self.connect()
        
        print(f"\n🚀 Starting to produce {num_transactions} transactions...")
        print(f"   Topic: {self.topic}")
        print(f"   Interval: {interval} seconds\n")
        
        sent = 0
        failed = 0
        
        for i in range(num_transactions):
            transaction_id = f"TXN_STREAM_{i+1:08d}"
            transaction = self.generate_transaction(transaction_id)
            
            if self.send_transaction(transaction):
                sent += 1
                if (i + 1) % 10 == 0:
                    print(f"   Sent {i + 1}/{num_transactions} transactions...")
            else:
                failed += 1
            
            # Wait before next transaction
            if i < num_transactions - 1:
                time.sleep(interval)
        
        # Flush remaining messages
        self.producer.flush()
        
        print(f"\n✓ Production complete!")
        print(f"   Sent: {sent}")
        print(f"   Failed: {failed}")
    
    def produce_continuous(self, interval: float = 1.0, duration: int = 60):
        """
        Produce transactions continuously for a specified duration
        
        Args:
            interval: Seconds between transactions
            duration: Duration in seconds
        """
        if not self.producer:
            self.connect()
        
        print(f"\n🚀 Starting continuous production...")
        print(f"   Topic: {self.topic}")
        print(f"   Interval: {interval} seconds")
        print(f"   Duration: {duration} seconds\n")
        
        start_time = time.time()
        count = 0
        
        try:
            while time.time() - start_time < duration:
                transaction_id = f"TXN_STREAM_{int(time.time())}_{count:06d}"
                transaction = self.generate_transaction(transaction_id)
                
                if self.send_transaction(transaction):
                    count += 1
                    if count % 10 == 0:
                        elapsed = time.time() - start_time
                        print(f"   Sent {count} transactions in {elapsed:.1f}s...")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("\n⚠ Production interrupted by user")
        
        finally:
            self.producer.flush()
            print(f"\n✓ Production stopped!")
            print(f"   Total sent: {count}")
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            print("✓ Producer closed")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Transaction Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="transactions", help="Kafka topic")
    parser.add_argument("--num-transactions", type=int, default=100, help="Number of transactions")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between transactions")
    parser.add_argument("--continuous", action="store_true", help="Continuous mode")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (continuous mode)")
    
    args = parser.parse_args()
    
    producer = TransactionKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    try:
        if args.continuous:
            producer.produce_continuous(interval=args.interval, duration=args.duration)
        else:
            producer.produce_stream(num_transactions=args.num_transactions, interval=args.interval)
    finally:
        producer.close()


if __name__ == "__main__":
    main()

