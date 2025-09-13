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
        title: str,
        error_message: str, 
        extension_id: Optional[str] = None,
        platform: Optional[str] = None,
        user_agent: Optional[str] = None,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send error report email
        
        Args:
            title: Error title/category (e.g., 'BigQuery API', 'Extension', 'Serp API')
            error_message: The detailed error message
            extension_id: ID of the extension that reported the error
            platform: Platform where error occurred (linkedin, facebook, etc.)
            user_agent: User agent string
            additional_info: Additional error context
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        try:
            if not self.client:
                if not self.initialize():
                    return False
            
            # Build email subject
            email_subject = f"{title} Error"
            if platform:
                email_subject += f" - {platform.title()}"
            if extension_id:
                email_subject += f" ({extension_id})"
                
            html_content = self._build_error_html(
                title, error_message, extension_id, platform, user_agent, additional_info
            )
            
            text_content = self._build_error_text(
                title, error_message, extension_id, platform, user_agent, additional_info
            )
            
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
                "tags": ["error-report", platform or "unknown"],
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
    
    def _build_error_html(
        self, 
        title: str,
        error_message: str, 
        extension_id: Optional[str],
        platform: Optional[str],
        user_agent: Optional[str],
        additional_info: Optional[Dict[str, Any]]
    ) -> str:
        """Build HTML email content"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>{title} Error Report</title>
        </head>
        <body style="font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h2 style="color: #d32f2f; margin-bottom: 20px;">{title} Error Report</h2>
                
                <div style="background-color: #ffebee; padding: 15px; border-left: 4px solid #d32f2f; margin-bottom: 20px;">
                    <h3 style="margin: 0 0 10px 0; color: #d32f2f;">Error Message:</h3>
                    <p style="margin: 0; font-family: monospace; background-color: #fff; padding: 10px; border-radius: 4px;">
                        {error_message}
                    </p>
                </div>
                
                <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                    <tr style="background-color: #f5f5f5;">
                        <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Timestamp</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{timestamp}</td>
                    </tr>
        """
        
        if platform:
            html += f"""
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Platform</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{platform.title()}</td>
                    </tr>
            """
        
        if extension_id:
            html += f"""
                    <tr style="background-color: #f5f5f5;">
                        <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Extension ID</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{extension_id}</td>
                    </tr>
            """
        
        if user_agent:
            html += f"""
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">User Agent</td>
                        <td style="padding: 10px; border: 1px solid #ddd; word-break: break-all;">{user_agent}</td>
                    </tr>
            """
        
        html += """
                </table>
        """
        
        if additional_info:
            html += """
                <h3 style="color: #333; margin-bottom: 10px;">Additional Information:</h3>
                <div style="background-color: #f5f5f5; padding: 15px; border-radius: 4px; margin-bottom: 20px;">
            """
            for key, value in additional_info.items():
                html += f"""
                    <p style="margin: 5px 0;"><strong>{key.replace('_', ' ').title()}:</strong> {value}</p>
                """
            html += """
                </div>
            """
        
        html += """
                <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px;">
                    <p>This error report was automatically generated by the Extension Error.</p>
                    <p>Please investigate and resolve the issue as soon as possible.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _build_error_text(
        self, 
        title: str,
        error_message: str, 
        extension_id: Optional[str],
        platform: Optional[str],
        user_agent: Optional[str],
        additional_info: Optional[Dict[str, Any]]
    ) -> str:
        """Build plain text email content"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        text = f"""
{title} Error Report
{'=' * (len(title) + 13)}

Error Message:
{error_message}

Details:
--------
Timestamp: {timestamp}
"""
        
        if platform:
            text += f"Platform: {platform.title()}\n"
        if extension_id:
            text += f"Extension ID: {extension_id}\n"
        if user_agent:
            text += f"User Agent: {user_agent}\n"
        
        if additional_info:
            text += "\nAdditional Information:\n"
            text += "-" * 25 + "\n"
            for key, value in additional_info.items():
                text += f"{key.replace('_', ' ').title()}: {value}\n"
        
        text += """
--
This error report was automatically generated by the Extension Error.
Please investigate and resolve the issue as soon as possible.
        """
        
        return text.strip()

# Global email service instance
email_service = EmailService()
