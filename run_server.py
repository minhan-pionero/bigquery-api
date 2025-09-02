#!/usr/bin/env python3
"""
Entry point for Multi-Platform Social Scraper BigQuery API Server
"""

import sys
import os

# Add current directory to Python path
server_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, server_dir)

# Import main function from main.py in the same directory
from main import main

if __name__ == "__main__":
    main()
