"""
Database schemas for different platforms
"""

from google.cloud import bigquery
from .settings import Platform

def get_linkedin_profile_schema():
    """Schema for LinkedIn user profiles"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("about", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("profile_image", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("birthday", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("websites", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField(
            "experiences", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("company", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("logo", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("employees", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("start", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("end", "INTEGER", mode="NULLABLE"),
                    ]),
                ]),
                bigquery.SchemaField("date", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("start", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
                    ]),
                    bigquery.SchemaField("end", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
                    ]),
                ]),
                bigquery.SchemaField("profile_positions", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("date", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("start", "RECORD", mode="NULLABLE", fields=[
                            bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                            bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
                            bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
                        ]),
                        bigquery.SchemaField("end", "RECORD", mode="NULLABLE", fields=[
                            bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                            bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
                            bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
                        ]),
                    ]),
                    bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("employment_type", "STRING", mode="NULLABLE")   
                ]),
            ]
        ),
        bigquery.SchemaField("skills", "STRING", mode="REPEATED"),
        bigquery.SchemaField(
            "educations", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("date", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("start", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
                    ]),
                    bigquery.SchemaField("end", "RECORD", mode="NULLABLE", fields=[
                        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("month", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("day", "INTEGER", mode="NULLABLE"),
                    ]),
                ]),
                bigquery.SchemaField("school", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("logo", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
                ]),
                bigquery.SchemaField("degree_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("field_of_study", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("grade", "STRING", mode="NULLABLE"),
            ]
        ),
        bigquery.SchemaField(
            "languages", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("proficiency", "STRING"),
            ]
        ),
        bigquery.SchemaField(
            "posts", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("content", "STRING"),
                bigquery.SchemaField("time", "STRING"),
                bigquery.SchemaField("num_reactions", "INTEGER"),
                bigquery.SchemaField("num_comments", "INTEGER"),
            ]
        ),
        # Timestamp fields
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

def get_facebook_profile_schema():
    """Schema for Facebook user profiles"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),  # Unique account identifier (username from URL)
        bigquery.SchemaField("friend_lists", "STRING", mode="REPEATED"),  # Danh sách account_id của friends
        bigquery.SchemaField("username", "STRING", mode="REQUIRED"),  # Display name shown on profile (người dùng nhập)
        bigquery.SchemaField("profile_image", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("about", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("websites", "STRING", mode="REPEATED"),
        bigquery.SchemaField(
            "experiences", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("time", "STRING"),
                bigquery.SchemaField("description", "STRING"), 
            ]
        ),
        bigquery.SchemaField(
            "educations", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("time", "STRING"),
            ]
        ),
        bigquery.SchemaField("languages", "STRING", mode="REPEATED"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("birthday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hometown", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "posts", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("content", "STRING"),
                bigquery.SchemaField("time", "STRING"),
            ]
        ),
        # bigquery.SchemaField("status", "STRING", mode="NULLABLE"),  # pending/processing/completed/failed
        # bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

def get_linkedin_profile_url_schema():
    """Schema for LinkedIn URL"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),  # pending/processing/completed/failed
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        # Timestamp fields
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

def get_facebook_profile_url_schema():
    """Schema for Facebook Profile URLs - individual profile URLs to crawl"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),  # Account ID từ URL profile
        bigquery.SchemaField("seed_url_id", "STRING", mode="NULLABLE"),  # ID từ seed_urls (null nếu từ friend list)
        bigquery.SchemaField("parent_account_id", "STRING", mode="NULLABLE"),  # Account ID của user cha
        bigquery.SchemaField("crawl_depth", "INTEGER", mode="NULLABLE"),  # Độ sâu crawl
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),  # Profile URL đầy đủ
        bigquery.SchemaField("source_type", "STRING", mode="NULLABLE"),  # "follower" hoặc "friend"
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),  # pending/processing/completed/failed
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

def get_linkedin_keyword_schema():
    """Schema for LinkedIn keywords"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("extension_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("start", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

def get_facebook_seed_url_schema():
    """Schema for Facebook URL followers - seed URLs to start crawling from"""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),  # URL seed (followers page)
        bigquery.SchemaField("seed_url_id", "STRING", mode="NULLABLE"),  # ID từ seed_urls (null nếu từ friend list)
        bigquery.SchemaField("extension_id", "STRING", mode="NULLABLE"),  # ID của extension đang sử dụng
        bigquery.SchemaField("max_profiles", "INTEGER", mode="NULLABLE"),  # Max profiles to extract (default 200)
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),  # pending/processing/completed/failed
        bigquery.SchemaField("processed_count", "INTEGER", mode="NULLABLE"),  # Số profile đã xử lý
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

# Schema mapping for easy access
SCHEMA_MAPPING = {
    Platform.LINKEDIN: {
        "profiles": get_linkedin_profile_schema,
        "urls": get_linkedin_profile_url_schema,
        "keywords": get_linkedin_keyword_schema
    },
    Platform.FACEBOOK: {
        "profiles": get_facebook_profile_schema,
        "urls": get_facebook_profile_url_schema,
        "seed_urls": get_facebook_seed_url_schema
    }
}

def get_schema(platform: Platform, table_type: str):
    """Get schema for a specific platform and table type"""
    if platform not in SCHEMA_MAPPING:
        raise ValueError(f"Unsupported platform: {platform}")
    
    if table_type not in SCHEMA_MAPPING[platform]:
        raise ValueError(f"Unsupported table type: {table_type}")
    
    return SCHEMA_MAPPING[platform][table_type]()
