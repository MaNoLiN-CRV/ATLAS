#!/usr/bin/env python3
"""
Atlas Performance Filter Management Script

This script allows you to view and modify the performance filtering thresholds
to control which queries are stored in the SQLite database.
"""

import sys
import os
import argparse
import json

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.utils.performance_config import get_performance_config
from src.database.sqlite_repository import SQLiteRepository


def show_current_config():
    """Display the current performance configuration."""
    config = get_performance_config()
    summary = config.get_config_summary()
    
    print("🔧 Current Performance Filter Configuration")
    print("=" * 50)
    
    thresholds = summary['thresholds']
    
    print("\n⏱️  Time-based Thresholds:")
    print(f"  Max Average Elapsed Time: {thresholds['max_avg_elapsed_time_ms']:.0f} ms")
    print(f"  Max Total Elapsed Time:   {thresholds['max_total_elapsed_time_ms']:.0f} ms")
    print(f"  Max Average CPU Time:     {thresholds['max_avg_cpu_time_ms']:.0f} ms")
    
    print("\n💾 I/O Thresholds:")
    print(f"  Max Avg Logical Reads:    {thresholds['max_avg_logical_reads']:.0f}")
    print(f"  Max Avg Physical Reads:   {thresholds['max_avg_physical_reads']:.0f}")
    print(f"  Max Total Logical Reads:  {thresholds['max_total_logical_reads']:,}")
    
    print("\n🧠 Memory & Efficiency:")
    print(f"  Min Spills to Capture:    {thresholds['min_spills_to_capture']}")
    print(f"  Max Avg Memory Grant:     {thresholds['max_avg_grant_kb']:.0f} KB")
    print(f"  Min Buffer Hit Ratio:     {thresholds['min_buffer_hit_ratio']:.1f}%")
    print(f"  Min CPU Efficiency:       {thresholds['min_cpu_efficiency_ratio']:.1f}%")
    
    print("\n📊 Collection Limits:")
    print(f"  Max Queries per Collection: {thresholds['max_stored_queries_per_collection']}")
    print(f"  Min Execution Count:        {thresholds['min_execution_count']}")
    
    print("\n💡 Mode Suggestions:")
    for mode, description in summary['mode_suggestions'].items():
        print(f"  {mode.capitalize()}: {description}")


def show_database_stats():
    """Show current database statistics."""
    try:
        repo = SQLiteRepository()
        stats = repo.get_database_size_info()
        
        print("\n📈 Current Database Statistics")
        print("=" * 50)
        print(f"Database Size:     {stats['database_size_mb']:.2f} MB")
        print(f"Total Records:     {stats['total_records']:,}")
        print(f"Records per Day:   {stats['estimated_records_per_day']:.1f}")
        print(f"Oldest Record:     {stats['oldest_record']}")
        print(f"Newest Record:     {stats['newest_record']}")
        
        # Calculate projected growth
        if stats['estimated_records_per_day'] > 0:
            daily_mb = (stats['database_size_mb'] / max(stats['total_records'], 1)) * stats['estimated_records_per_day']
            monthly_mb = daily_mb * 30
            print(f"\n📊 Projected Growth:")
            print(f"Daily Growth:      {daily_mb:.2f} MB/day")
            print(f"Monthly Growth:    {monthly_mb:.2f} MB/month")
            
            if monthly_mb > 100:
                print("⚠️  WARNING: High growth rate detected. Consider more aggressive filtering.")
            elif monthly_mb < 10:
                print("✅ Growth rate is reasonable.")
        
    except Exception as e:
        print(f"❌ Error getting database stats: {e}")


def set_mode(mode: str):
    """Set filtering mode."""
    config = get_performance_config()
    
    if mode == "conservative":
        config.set_conservative_mode()
        print("✅ Switched to CONSERVATIVE mode (captures more potential issues)")
    elif mode == "aggressive":
        config.set_aggressive_mode()
        print("✅ Switched to AGGRESSIVE mode (captures only severe issues)")
    elif mode == "default":
        config.reset_to_defaults()
        print("✅ Reset to DEFAULT mode (balanced filtering)")
    else:
        print(f"❌ Unknown mode: {mode}")
        print("Available modes: conservative, default, aggressive")


def cleanup_database(days: int):
    """Clean up old database records."""
    try:
        repo = SQLiteRepository()
        
        # Show stats before cleanup
        stats_before = repo.get_database_size_info()
        print(f"📊 Before cleanup: {stats_before['total_records']:,} records, {stats_before['database_size_mb']:.2f} MB")
        
        # Perform cleanup
        if repo.cleanup_old_metrics(days):
            # Show stats after cleanup
            stats_after = repo.get_database_size_info()
            records_removed = stats_before['total_records'] - stats_after['total_records']
            size_saved = stats_before['database_size_mb'] - stats_after['database_size_mb']
            
            print(f"✅ Cleanup completed:")
            print(f"   Records removed: {records_removed:,}")
            print(f"   Space saved:     {size_saved:.2f} MB")
            print(f"   Current size:    {stats_after['database_size_mb']:.2f} MB")
        else:
            print("❌ Cleanup failed")
            
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")


def remove_duplicates():
    """Remove duplicate queries from database."""
    try:
        repo = SQLiteRepository()
        
        stats_before = repo.get_database_size_info()
        print(f"📊 Before deduplication: {stats_before['total_records']:,} records")
        
        if repo.remove_duplicate_queries():
            stats_after = repo.get_database_size_info()
            records_removed = stats_before['total_records'] - stats_after['total_records']
            
            print(f"✅ Deduplication completed:")
            print(f"   Duplicates removed: {records_removed:,}")
            print(f"   Records remaining:  {stats_after['total_records']:,}")
        else:
            print("❌ Deduplication failed")
            
    except Exception as e:
        print(f"❌ Error during deduplication: {e}")


def main():
    parser = argparse.ArgumentParser(description="Atlas Performance Filter Management")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Show command
    subparsers.add_parser('show', help='Show current configuration and database stats')
    
    # Mode command
    mode_parser = subparsers.add_parser('mode', help='Set filtering mode')
    mode_parser.add_argument('mode', choices=['conservative', 'default', 'aggressive'],
                           help='Filtering mode to set')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old database records')
    cleanup_parser.add_argument('--days', type=int, default=30,
                               help='Keep records newer than N days (default: 30)')
    
    # Deduplicate command
    subparsers.add_parser('dedupe', help='Remove duplicate queries from database')
    
    args = parser.parse_args()
    
    if args.command == 'show' or args.command is None:
        show_current_config()
        show_database_stats()
    elif args.command == 'mode':
        set_mode(args.mode)
        print("\n" + "="*50)
        show_current_config()
    elif args.command == 'cleanup':
        cleanup_database(args.days)
    elif args.command == 'dedupe':
        remove_duplicates()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
