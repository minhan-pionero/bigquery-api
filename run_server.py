#!/usr/bin/env python3
"""
Entry point for Multi-Platform Social Scraper BigQuery API Server
"""

import sys
import os
import argparse

# Add current directory to Python path
server_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, server_dir)

def main():
    """Main entry point with command line arguments"""
    parser = argparse.ArgumentParser(description='Multi-Platform Social Scraper BigQuery API Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to (default: 8000)')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for development')
    parser.add_argument('--log-level', default='info', choices=['debug', 'info', 'warning', 'error'],
                       help='Log level (default: info)')
    
    args = parser.parse_args()
    
    # Import and run the server
    from main import run_server
    run_server(
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level
    )

if __name__ == "__main__":
    main()
