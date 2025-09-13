"""
API route handlers for email operations
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Body
from pydantic import BaseModel, Field

from services.email_service import email_service

logger = logging.getLogger(__name__)
router = APIRouter()

class ErrorReportRequest(BaseModel):
    """Request model for error reports"""
    title: str = Field(..., description="Error title/category (e.g., 'BigQuery API', 'Extension', 'Serp API')")
    error_message: str = Field(..., description="The detailed error message")
    extension_id: Optional[str] = Field(None, description="Extension ID that reported the error")
    platform: Optional[str] = Field(None, description="Platform where error occurred (linkedin, facebook, etc.)")
    error_type: Optional[str] = Field(None, description="Type of error (connection, api, parsing, etc.)")
    url: Optional[str] = Field(None, description="URL where error occurred")
    stack_trace: Optional[str] = Field(None, description="Stack trace of the error")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional error context")

@router.post("/email/error-report")
async def send_error_report(
    request: Request,
    error_data: ErrorReportRequest = Body(...)
):
    """
    Send error report email via Mandrill
    
    Example request:
    ```json
    {
        "title": "BigQuery API",
        "error_message": "API 接続に失敗しました。サーバーの接続状況を確認してください。",
        "extension_id": "ext_linkedin_001",
        "platform": "linkedin",
        "error_type": "connection",
        "url": "https://www.linkedin.com/search/results/people/",
        "additional_context": {
            "timestamp": "2025-09-07T10:30:00Z",
            "browser": "Chrome 118.0.0.0",
            "operation": "profile_scraping"
        }
    }
    ```
    
    Other title examples: "Extension", "Serp API", "Database Connection", etc.
    """
    try:
        # Get user agent from request headers
        # user_agent = request.headers.get("user-agent")
        user_agent = None
        
        # Prepare additional info
        additional_info = {}
        if error_data.error_type:
            additional_info["error_type"] = error_data.error_type
        if error_data.url:
            additional_info["url"] = error_data.url
        if error_data.stack_trace:
            additional_info["stack_trace"] = error_data.stack_trace
        if error_data.additional_context:
            additional_info.update(error_data.additional_context)
        
        # Add request info
        # additional_info["client_ip"] = request.client.host if request.client else "unknown"
        # additional_info["timestamp"] = datetime.now().isoformat()
        
        # Send email
        success = email_service.send_error_report(
            title=error_data.title,
            error_message=error_data.error_message,
            extension_id=error_data.extension_id,
            platform=error_data.platform,
            user_agent=user_agent,
            additional_info=additional_info
        )
        
        if success:
            logger.info(f"✅ Error report email sent successfully for extension: {error_data.extension_id}")
            return {
                "status": "success",
                "message": "Error report email sent successfully",
                "extension_id": error_data.extension_id,
                "platform": error_data.platform,
                "timestamp": datetime.now().isoformat()
            }
        else:
            logger.error(f"❌ Failed to send error report email for extension: {error_data.extension_id}")
            raise HTTPException(status_code=500, detail="Failed to send error report email")
        
    except Exception as e:
        logger.error(f"❌ Error in send_error_report endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Error sending error report: {str(e)}")

@router.post("/email/test")
async def test_email_service():
    """Test email service connectivity"""
    try:
        # Test with a simple error message
        success = email_service.send_error_report(
            title="Test System",
            error_message="This is a test error message from BigQuery API",
            extension_id="test_extension",
            platform="test",
            additional_info={
                "test": True,
                "timestamp": datetime.now().isoformat()
            }
        )
        
        if success:
            return {
                "status": "success",
                "message": "Test email sent successfully",
                "service": "mandrill",
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send test email")
            
    except Exception as e:
        logger.error(f"❌ Error in test email endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Email service test failed: {str(e)}")

@router.get("/email/status")
async def get_email_service_status():
    """Get email service status"""
    try:
        # Try to initialize the service
        is_initialized = email_service.initialize()
        
        return {
            "status": "healthy" if is_initialized else "unhealthy",
            "service": "mandrill",
            "initialized": is_initialized,
            "config": {
                "from_email": email_service.from_email,
                "from_name": email_service.from_name,
                "to_emails_count": len(email_service.to_emails),
                "api_key_configured": bool(email_service.api_key and email_service.api_key != "your-mandrill-api-key-here")
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Error checking email service status: {e}")
        return {
            "status": "error",
            "service": "mandrill",
            "initialized": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
