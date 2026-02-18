"""
Synthetic Data Generator for Lakehouse Architecture
Generates e-commerce transaction data for testing
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import os


class ECommerceDataGenerator:
    """Generate synthetic e-commerce transaction data"""
    
    def __init__(self, seed: int = 42):
        """Initialize generator with random seed"""
        np.random.seed(seed)
        random.seed(seed)
        
        # Product categories
        self.categories = [
            "Electronics", "Clothing", "Home & Garden", 
            "Books", "Sports", "Toys", "Food & Beverage"
        ]
        
        # Payment methods
        self.payment_methods = [
            "Credit Card", "Debit Card", "PayPal", 
            "Bank Transfer", "Cash on Delivery"
        ]
        
        # Customer segments
        self.customer_segments = [
            "Premium", "Standard", "Budget", "VIP"
        ]
        
        # Product names (sample)
        self.product_names = [
            "Wireless Headphones", "Laptop Stand", "Running Shoes",
            "Coffee Maker", "Desk Lamp", "Yoga Mat", "Water Bottle",
            "Backpack", "Smart Watch", "Tablet Case"
        ]
    
    def generate_customers(self, num_customers: int = 1000) -> pd.DataFrame:
        """Generate customer data"""
        customers = []
        for i in range(num_customers):
            customers.append({
                "customer_id": f"CUST_{i+1:05d}",
                "customer_name": f"Customer {i+1}",
                "email": f"customer{i+1}@example.com",
                "segment": random.choice(self.customer_segments),
                "registration_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                "country": random.choice(["USA", "UK", "Canada", "Germany", "France", "Australia"])
            })
        return pd.DataFrame(customers)
    
    def generate_products(self, num_products: int = 500) -> pd.DataFrame:
        """Generate product catalog"""
        products = []
        for i in range(num_products):
            products.append({
                "product_id": f"PROD_{i+1:05d}",
                "product_name": random.choice(self.product_names) + f" {i+1}",
                "category": random.choice(self.categories),
                "price": round(np.random.uniform(10, 500), 2),
                "cost": round(np.random.uniform(5, 300), 2),
                "stock_quantity": random.randint(0, 1000)
            })
        return pd.DataFrame(products)
    
    def generate_transactions(
        self, 
        num_transactions: int = 10000,
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        """Generate transaction data"""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=90)
        if end_date is None:
            end_date = datetime.now()
        
        transactions = []
        for i in range(num_transactions):
            transaction_date = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            
            quantity = random.randint(1, 5)
            unit_price = round(np.random.uniform(10, 500), 2)
            total_amount = round(quantity * unit_price, 2)
            
            # Apply discounts occasionally
            discount = 0
            if random.random() < 0.2:  # 20% chance of discount
                discount = round(total_amount * random.uniform(0.05, 0.25), 2)
            
            final_amount = round(total_amount - discount, 2)
            
            transactions.append({
                "transaction_id": f"TXN_{i+1:08d}",
                "customer_id": f"CUST_{random.randint(1, 1000):05d}",
                "product_id": f"PROD_{random.randint(1, 500):05d}",
                "transaction_date": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "discount": discount,
                "final_amount": final_amount,
                "payment_method": random.choice(self.payment_methods),
                "status": random.choice(["Completed", "Pending", "Cancelled", "Refunded"]),
                "shipping_address": f"{random.randint(1, 999)} Main St, City {random.randint(1, 50)}"
            })
        
        return pd.DataFrame(transactions)
    
    def save_to_files(
        self,
        output_dir: str = "data/raw",
        num_customers: int = 1000,
        num_products: int = 500,
        num_transactions: int = 10000
    ):
        """Generate and save all data files"""
        os.makedirs(output_dir, exist_ok=True)
        
        print("Generating customers...")
        customers_df = self.generate_customers(num_customers)
        customers_df.to_csv(f"{output_dir}/customers.csv", index=False)
        print(f"✓ Generated {len(customers_df)} customers")
        
        print("Generating products...")
        products_df = self.generate_products(num_products)
        products_df.to_csv(f"{output_dir}/products.csv", index=False)
        print(f"✓ Generated {len(products_df)} products")
        
        print("Generating transactions...")
        transactions_df = self.generate_transactions(num_transactions)
        transactions_df.to_csv(f"{output_dir}/transactions.csv", index=False)
        print(f"✓ Generated {len(transactions_df)} transactions")
        
        print(f"\n✓ All data files saved to {output_dir}/")
        return customers_df, products_df, transactions_df


if __name__ == "__main__":
    generator = ECommerceDataGenerator(seed=42)
    generator.save_to_files(
        output_dir="data/raw",
        num_customers=1000,
        num_products=500,
        num_transactions=10000
    )

