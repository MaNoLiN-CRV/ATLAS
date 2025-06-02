#!/usr/bin/env python3
"""
Launch script for Atlas GUI
"""
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import Atlas modules
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

# Import the main module
from main import main

if __name__ == "__main__":
    # Launch Atlas in GUI mode
    sys.argv = [sys.argv[0]]  # Clear any arguments
    main()
