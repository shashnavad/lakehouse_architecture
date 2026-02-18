#!/usr/bin/env python3
"""
Main pipeline runner
Executes Bronze → Silver → Gold pipeline
"""

import sys
import os
from pathlib import Path

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent))

from bronze.ingest_raw_data import main as bronze_main
# from silver.process_silver import main as silver_main
# from gold.process_gold import main as gold_main


def main():
    """Run the complete pipeline"""
    print("=" * 60)
    print("LAKEHOUSE PIPELINE - MEDALLION ARCHITECTURE")
    print("=" * 60)
    
    try:
        # Phase 1: Bronze Layer
        print("\n[1/3] Running Bronze Layer Ingestion...")
        bronze_main()
        
        # Phase 2: Silver Layer (to be implemented)
        # print("\n[2/3] Running Silver Layer Processing...")
        # silver_main()
        
        # Phase 3: Gold Layer (to be implemented)
        # print("\n[3/3] Running Gold Layer Aggregation...")
        # gold_main()
        
        print("\n" + "=" * 60)
        print("✓ PIPELINE COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

