#!/usr/bin/env python3
"""
Simple Atlas Performance Manager
"""

import sys
import os
sys.path.insert(0, '.')

from src.utils.performance_config import get_performance_config
from src.database.sqlite_repository import SQLiteRepository

def main():
    print("🚀 Atlas Performance Filter Manager")
    print("=" * 40)
    
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "status":
        show_status()
    elif command == "cleanup":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        cleanup_old_data(days)
    elif command == "dedupe":
        remove_duplicates()
    elif command == "conservative":
        set_conservative_mode()
    elif command == "aggressive":
        set_aggressive_mode()
    elif command == "default":
        set_default_mode()
    else:
        show_help()

def show_help():
    print("Available commands:")
    print("  status      - Show current configuration and database stats")
    print("  cleanup N   - Remove records older than N days (default: 30)")
    print("  dedupe      - Remove duplicate queries")
    print("  conservative - Set conservative filtering (captures more)")
    print("  aggressive  - Set aggressive filtering (captures less)")
    print("  default     - Reset to default filtering")
    print("\nExample:")
    print("  python simple_manager.py status")
    print("  python simple_manager.py cleanup 15")

def show_status():
    print("\n📊 Current Status")
    print("-" * 20)
    
    try:
        # Configuration
        config = get_performance_config()
        thresholds = config.get_thresholds()
        print(f"Filtering mode: Default")
        print(f"Max avg time threshold: {thresholds.max_avg_elapsed_time_ms:.0f}ms")
        print(f"Max queries per collection: {thresholds.max_stored_queries_per_collection}")
        
        # Database stats
        repo = SQLiteRepository()
        stats = repo.get_database_size_info()
        print(f"\nDatabase size: {stats['database_size_mb']:.2f} MB")
        print(f"Total records: {stats['total_records']:,}")
        
        if stats['total_records'] > 0:
            avg_size_per_record = stats['database_size_mb'] / stats['total_records']
            print(f"Avg size per record: {avg_size_per_record:.3f} MB")
            
            # Growth estimation
            if stats['estimated_records_per_day'] > 0:
                daily_growth = avg_size_per_record * stats['estimated_records_per_day']
                monthly_growth = daily_growth * 30
                print(f"Estimated daily growth: {daily_growth:.2f} MB")
                print(f"Estimated monthly growth: {monthly_growth:.2f} MB")
                
                if monthly_growth > 100:
                    print("⚠️  WARNING: High growth rate! Consider aggressive filtering.")
                elif monthly_growth > 50:
                    print("⚠️  Moderate growth rate. Monitor database size.")
                else:
                    print("✅ Growth rate is acceptable.")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def cleanup_old_data(days):
    print(f"\n🧹 Cleaning up records older than {days} days")
    print("-" * 40)
    
    try:
        repo = SQLiteRepository()
        stats_before = repo.get_database_size_info()
        
        if repo.cleanup_old_metrics(days):
            stats_after = repo.get_database_size_info()
            records_removed = stats_before['total_records'] - stats_after['total_records']
            size_saved = stats_before['database_size_mb'] - stats_after['database_size_mb']
            
            print(f"✅ Cleanup completed:")
            print(f"   Records removed: {records_removed:,}")
            print(f"   Space saved: {size_saved:.2f} MB")
            print(f"   New size: {stats_after['database_size_mb']:.2f} MB")
        else:
            print("❌ Cleanup failed")
    except Exception as e:
        print(f"❌ Error: {e}")

def remove_duplicates():
    print("\n🔄 Removing duplicate queries")
    print("-" * 30)
    
    try:
        repo = SQLiteRepository()
        stats_before = repo.get_database_size_info()
        
        if repo.remove_duplicate_queries():
            stats_after = repo.get_database_size_info()
            records_removed = stats_before['total_records'] - stats_after['total_records']
            size_saved = stats_before['database_size_mb'] - stats_after['database_size_mb']
            
            print(f"✅ Deduplication completed:")
            print(f"   Duplicates removed: {records_removed:,}")
            print(f"   Space saved: {size_saved:.2f} MB")
            print(f"   Unique records: {stats_after['total_records']:,}")
        else:
            print("❌ Deduplication failed")
    except Exception as e:
        print(f"❌ Error: {e}")

def set_conservative_mode():
    print("\n🔧 Setting CONSERVATIVE filtering mode")
    print("-" * 40)
    try:
        config = get_performance_config()
        config.set_conservative_mode()
        print("✅ Conservative mode activated")
        print("   This will capture MORE potential performance issues")
        print("   Thresholds are more sensitive")
    except Exception as e:
        print(f"❌ Error: {e}")

def set_aggressive_mode():
    print("\n🔧 Setting AGGRESSIVE filtering mode")
    print("-" * 40)
    try:
        config = get_performance_config()
        config.set_aggressive_mode()
        print("✅ Aggressive mode activated")
        print("   This will capture FEWER queries (only severe issues)")
        print("   Thresholds are less sensitive")
    except Exception as e:
        print(f"❌ Error: {e}")

def set_default_mode():
    print("\n🔧 Resetting to DEFAULT filtering mode")
    print("-" * 40)
    try:
        config = get_performance_config()
        config.reset_to_defaults()
        print("✅ Default mode activated")
        print("   Balanced filtering for most use cases")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
