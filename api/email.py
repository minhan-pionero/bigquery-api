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
    platform: str = Field(..., description="Platform where error occurred (LinkedIn, Facebook)")
    method: str = Field(..., description="Method type (クローラー, 友達取得)")
    error: str = Field(..., description="The error message")

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
        "platform": "LinkedIn",
        "method": "クローラー",
        "error": "接続がタイムアウトしました。ネットワーク設定を確認してください。"
    }
    ```
    
    Other examples: platform: "Facebook", method: "友達取得"
    """
    try:
        # Send email
        success = email_service.send_error_report(
            platform=error_data.platform,
            method=error_data.method,
            error=error_data.error
        )
        
        if success:
            logger.info(f"✅ Error report email sent successfully for platform: {error_data.platform}")
            return {
                "status": "success",
                "message": "Error report email sent successfully",
                "platform": error_data.platform,
                "method": error_data.method,
                "timestamp": datetime.now().isoformat()
            }
        else:
            logger.error(f"❌ Failed to send error report email for platform: {error_data.platform}")
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
            platform="LinkedIn",
            method="クローラー", 
            error="This is a test error message from BigQuery API"
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
