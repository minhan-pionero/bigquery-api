"""
Configuration settings for Multi-Platform Social Scraper API
"""

from enum import Enum

class Platform(str, Enum):
    """Supported platforms"""
    LINKEDIN = "linkedin"
    FACEBOOK = "facebook"

# BigQuery configuration
BIGQUERY_CONFIG = {
    "project_id": "compass-ml-dev",
    "dataset_id": "compass_crawling"
}

# Table mapping for each platform
TABLE_MAPPING = {
    Platform.LINKEDIN: {
        "profiles": "profiles_linkedin",
        "urls": "profile_urls_linkedin",
        "keywords": "keywords_linkedin",
    },
    Platform.FACEBOOK: {
        "profiles": "profiles_facebook",
        "urls": "profile_urls_facebook",
        "seed_urls": "seed_urls_facebook",
        "urls_v1": "profile_urls_facebook_v1",
        "seed_urls_v1": "seed_urls_facebook_v1",
    },
}


# Server configuration
SERVER_CONFIG = {
    "host": "0.0.0.0",
    "port": 8000,
    "cors_origins": ["*"],  # Restrict this in production
    "log_level": "info"
}

# Service Account configuration for Compute Engine
SERVICE_ACCOUNT_CONFIG = {
    "metadata_url": 'http://metadata.google.internal/computeMetadata/v1/',
    "metadata_headers": {'Metadata-Flavor': 'Google'},
    "service_account": 'default'
}

# Email configuration (Mandrill)
EMAIL_CONFIG = {
    "mandrill_api_key": "md-aeuZd69-RPEp1xvBojBk7Q",  # Set in environment variable
    "from_email": "compass-dev@hajimari.inc",
    "from_name": "compass-dev-team",
    "to_emails": [
        "nguyenminhan0402@gmail.com",
        # "trunganh.nguyen_pt@pionero.io"
    ],
}
