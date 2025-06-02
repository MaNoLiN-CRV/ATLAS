#!/usr/bin/env python3
"""
Atlas - Database Performance Analysis Tool
Main entry point for the application
"""

import sys
import argparse
from src.core.core import Core


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Atlas - Database Performance Analysis Tool")
    
    parser.add_argument(
        "--nogui", 
        action="store_true",
        help="Run in command-line mode without GUI"
    )
    
    return parser.parse_args()


def main():
    """Main entry point for Atlas."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Initialize the Core
        print("Initializing Atlas...")
        core = Core()
        
        # Run in the appropriate mode
        if args.nogui:
            # Run in command-line mode
            print("Starting in command-line mode")
            core.run()
        else:
            # Run in GUI mode (default)
            print("Starting in GUI mode")
            core.run_gui()
            
    except KeyboardInterrupt:
        print("\nAtlas terminated by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error running Atlas: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
