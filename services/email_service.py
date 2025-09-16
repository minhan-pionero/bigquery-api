"""
Email service for sending error notifications via Mandrill
"""

import logging
import os
import mandrill
from datetime import datetime
from typing import List, Dict, Any, Optional
from config.settings import EMAIL_CONFIG

logger = logging.getLogger(__name__)

class EmailService:
    """Service for sending emails via Mandrill"""
    
    def __init__(self):
        """Initialize Mandrill client"""
        self.api_key = os.environ.get("MANDRILL_API_KEY", EMAIL_CONFIG["mandrill_api_key"])
        self.client = None
        self.from_email = EMAIL_CONFIG["from_email"]
        self.from_name = EMAIL_CONFIG["from_name"]
        self.to_emails = EMAIL_CONFIG["to_emails"]
        
    def initialize(self) -> bool:
        """Initialize Mandrill client"""
        try:
            if not self.api_key:
                logger.error("❌ Mandrill API key not configured")
                return False
            logger.info(f"api_key: {self.api_key}")
            self.client = mandrill.Mandrill(self.api_key)
            
            # Test the connection
            ping_result = self.client.users.ping()
            logger.info(f"✅ Mandrill connection successful: {ping_result}")
            return True
            
        except mandrill.Error as e:
            logger.error(f"❌ Mandrill initialization failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Email service initialization failed: {e}")
            return False
    
    def send_error_report(
        self, 
        platform: str,
        method: str,
        error: str
    ) -> bool:
        """
        Send error report email with Japanese template
        
        Args:
            platform: Platform where error occurred (LinkedIn, Facebook)
            method: Method type (クローラー, 友達取得)
            error: The error message
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            if not self.client:
                if not self.initialize():
                    return False
            
            # Build email subject and content
            email_subject = f"{platform}エラー通知"
            html_content = self._build_japanese_error_html(platform, method, error)
            text_content = self._build_japanese_error_text(platform, method, error)
            
            # Prepare recipients
            recipients = [{"email": email, "type": "to"} for email in self.to_emails]
            
            # Prepare message
            message = {
                "html": html_content,
                "text": text_content,
                "subject": email_subject,
                "from_email": self.from_email,
                "from_name": self.from_name,
                "to": recipients,
                "headers": {"Reply-To": self.from_email},
                "important": True,
                "track_opens": True,
                "track_clicks": True,
                "auto_text": True,
                "auto_html": False,
                "inline_css": True,
                "url_strip_qs": False,
                "preserve_recipients": False,
                "view_content_link": False,
                "tracking_domain": None,
                "signing_domain": None,
                "return_path_domain": None,
                "tags": ["error-report", platform.lower()],
                "subaccount": None
            }
            
            # Send email
            result = self.client.messages.send(message=message, async_send=False)
            
            # Check results
            for res in result:
                if res["status"] in ["sent", "queued", "scheduled"]:
                    logger.info(f"✅ Error report email sent to {res['email']}: {res['status']}")
                else:
                    logger.warning(f"⚠️ Error report email failed for {res['email']}: {res['status']} - {res.get('reject_reason', 'Unknown')}")
            
            return any(res["status"] in ["sent", "queued", "scheduled"] for res in result)
            
        except mandrill.Error as e:
            logger.error(f"❌ Mandrill error while sending email: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error sending error report email: {e}")
            return False
    
    def _build_japanese_error_html(
        self, 
        platform: str,
        method: str,
        error: str
    ) -> str:
        """Build Japanese HTML email content"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>{platform}エラー通知</title>
        </head>
        <body style="font-family: 'Hiragino Sans', 'Meiryo', 'MS PGothic', Arial, sans-serif; margin: 20px; background-color: #f5f5f5;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h2 style="color: #d32f2f; margin-bottom: 20px;">{platform}の{method}でエラーが発生しました</h2>
                
                <div style="background-color: #ffebee; padding: 15px; border-left: 4px solid #d32f2f; margin-bottom: 20px;">
                    <h3 style="margin: 0 0 10px 0; color: #d32f2f;">エラー内容</h3>
                    <div style="margin: 0; padding: 10px 0; border-top: 1px solid #d32f2f; border-bottom: 1px solid #d32f2f;">
                        ========================
                    </div>
                    <p style="margin: 10px 0; font-family: monospace; background-color: #fff; padding: 10px; border-radius: 4px; white-space: pre-wrap;">
                        {error}
                    </p>
                    <div style="margin: 0; padding: 10px 0; border-top: 1px solid #d32f2f; border-bottom: 1px solid #d32f2f;">
                        ========================
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _build_japanese_error_text(
        self, 
        platform: str,
        method: str,
        error: str
    ) -> str:
        """Build Japanese plain text email content"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        text = f"""{platform}の{method}でエラーが発生しました

エラー内容
========================
{error}
========================
        """
        
        return text.strip()

# Global email service instance
email_service = EmailService()
