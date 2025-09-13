"""
Main application file - Multi-Platform Social Scraper BigQuery API Server
"""

import logging
import os
import sys
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

# Add current directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from config.settings import SERVER_CONFIG, BIGQUERY_CONFIG, Platform
from services.bigquery_service import bigquery_service
from api import linkedin, facebook, email

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Multi-Platform Social Scraper BigQuery API",
    description="API server for Social Media Extensions to interact with BigQuery (LinkedIn, Facebook, etc.)",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=SERVER_CONFIG["cors_origins"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include router
app.include_router(linkedin.router, tags=["linkedin"])
app.include_router(facebook.router, tags=["facebook"])
app.include_router(email.router, tags=["email"])

@app.on_event("startup")
async def startup_event():
    """Initialize BigQuery when server starts"""
    logger.info("🚀 Starting Multi-Platform Social Scraper BigQuery API Server")
    
    if not bigquery_service.initialize():
        logger.error("❌ Failed to initialize BigQuery - server may not work properly")
        return
    
    if not bigquery_service.create_tables_for_all_platforms():
        logger.error("❌ Failed to create/verify tables - server may not work properly")
        return
    
    logger.info("✅ Server startup completed successfully")

@app.get("/")
async def root():
    """Health check endpoint with service information"""
    return {
        "status": "ok",
        "service": "Multi-Platform Social Scraper BigQuery API",
        "version": "2.0.0",
        "project_id": BIGQUERY_CONFIG["project_id"],
        "dataset_id": BIGQUERY_CONFIG["dataset_id"],
        "supported_platforms": [platform.value for platform in Platform],
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    try:
        # Test BigQuery connection
        query = "SELECT 1 as test"
        result = bigquery_service.query_table(query)
        
        return {
            "status": "healthy",
            "bigquery_connection": "ok",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

def main():
    """Main function to run the server with default settings"""
    run_server()

def run_server(host=None, port=None, reload=False, log_level=None):
    """Run the server with specified settings"""
    host = host or SERVER_CONFIG["host"]
    port = port or int(os.environ.get("PORT", SERVER_CONFIG["port"]))
    log_level = log_level or SERVER_CONFIG["log_level"]
    
    logger.info(f"🚀 Starting server on {host}:{port}")
    if reload:
        logger.info("🔄 Auto-reload enabled (development mode)")
    
    # Use the correct module path for uvicorn
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=reload
    )

if __name__ == "__main__":
    main()
