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
    
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test database connection and exit"
    )
    
    return parser.parse_args()


def main():
    """Main entry point for Atlas."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Handle special test-connection mode
        if args.test_connection:
            print("Testing database connection...")
            # Run connection test and exit
            import subprocess
            subprocess.run([sys.executable, "scripts/test_connection.py"])
            return
        
        # Initialize the Core
        print("Initializing Atlas...")
        
        # Run in the appropriate mode
        if args.nogui:
            # Run in command-line mode
            core = Core.get_instance()  # Use singleton pattern
            print("Starting in command-line mode")
            core.run()
        else:
            # Check if running under Streamlit
            try:
                import streamlit as st
                if hasattr(st, 'runtime') and hasattr(st.runtime, 'exists') and st.runtime.exists():
                    # Show immediate loading screen
                    if 'app_started' not in st.session_state:
                        st.session_state['app_started'] = True
                        # Force rerun to show loading screen
                        st.rerun()
                    
                    # Get or create Core instance using singleton pattern
                    core = Core.get_instance()
                    
                    # Print debug info about session state
                    print(f"Session ID: {id(st.session_state)}")
                    print(f"Core exists in session: {'core' in st.session_state}")
                    
                    # Running under Streamlit - proceed with GUI
                    print("Starting in GUI mode")
                    core.run_gui()
                else:
                    # Not running under Streamlit - show instructions but don't exit with error
                    print("Atlas GUI requires Streamlit runtime.")
                    print("\nTo run the GUI, use:")
                    print("  streamlit run main.py")
                    print("\nOr run in command-line mode with:")
                    print("  python main.py --nogui")
                    print("\nOr test database connection with:")
                    print("  python main.py --test-connection")
                    
                    # Don't exit with error code - this is expected behavior
                    return
            except ImportError:
                print("Streamlit not available. Running in command-line mode.")
                print("To install Streamlit: pip install streamlit")
                core.run()
            
    except KeyboardInterrupt:
        print("\n\nAtlas terminated by user.")
        return
    except ImportError as e:
        print(f"Error importing required modules: {e}")
        print("Make sure all dependencies are installed: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"Error running Atlas: {e}")
        import traceback
        traceback.print_exc()
        print("\nTry running the test_connection.py script to diagnose database connection issues:")
        print("python scripts/test_connection.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
