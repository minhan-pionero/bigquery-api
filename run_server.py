#!/usr/bin/env python3
"""
Entry point for Multi-Platform Social Scraper BigQuery API Server
"""

import sys
import os

server_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(server_dir))

from api.main import main

if __name__ == "__main__":
    main()
