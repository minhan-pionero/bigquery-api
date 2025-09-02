import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

from config.settings import Platform

def convert_datetime_to_iso(value):
    """Convert datetime object to ISO string for BigQuery compatibility"""
    if isinstance(value, datetime):
        return value.isoformat()
    return value

def ensure_datetime_serializable(data: Dict[str, Any]) -> Dict[str, Any]:
    """Keep datetime objects as-is for BigQuery (BigQuery client handles datetime serialization)"""
    result = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            # Keep datetime objects as-is for BigQuery
            result[key] = value
        elif isinstance(value, list):
            # Handle list of records (like experiences, posts, etc.)
            result[key] = []
            for item in value:
                if isinstance(item, dict):
                    result[key].append(ensure_datetime_serializable(item))
                else:
                    result[key].append(item)
        elif isinstance(value, dict):
            result[key] = ensure_datetime_serializable(value)
        else:
            result[key] = value
    return result

def convert_datetime_for_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert datetime objects to ISO strings and ensure ID fields are strings for insert_rows_json compatibility"""
    result = {}
    for key, value in data.items():
        if value is None:
            # Keep None as None (will become null in JSON)
            result[key] = None
        elif isinstance(value, datetime):
            # Convert datetime to ISO string for JSON serialization
            result[key] = value.isoformat()
        elif isinstance(value, list):
            # Handle list of records (like experiences, posts, etc.)
            result[key] = []
            for item in value:
                if isinstance(item, dict):
                    result[key].append(convert_datetime_for_json(item))
                elif isinstance(item, datetime):
                    result[key].append(item.isoformat())
                elif item is None:
                    # Skip None values in lists to avoid BigQuery issues
                    continue
                else:
                    result[key].append(item)
        elif isinstance(value, dict):
            result[key] = convert_datetime_for_json(value)
        elif isinstance(value, (int, float, bool)):
            # Keep numeric and boolean values as-is
            result[key] = value
        elif isinstance(value, str):
            # Keep strings as-is but ensure they're not empty if required
            result[key] = value
        else:
            # Convert other types to string as fallback
            result[key] = str(value) if value is not None else None
    return result

def convert_batch_datetime_for_json(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert datetime objects to ISO strings for a batch of data for insert_rows_json"""
    return [convert_datetime_for_json(data) for data in data_list]

def transform_linkedin_profile(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform LinkedIn profile data to BigQuery format - minimal processing"""
    
    # Simply ensure required fields and add timestamps
    transformed = profile_data.copy()
    
    # Ensure ID exists
    if "id" not in transformed:
        transformed["id"] = str(uuid.uuid4())
    
    # Add timestamps if not present (as datetime object for BigQuery TIMESTAMP)
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed:
        transformed["created_at"] = current_time
    if "updated_at" not in transformed:
        transformed["updated_at"] = current_time
    
    # Handle posts timestamp conversion
    if "posts" in transformed and isinstance(transformed["posts"], list):
        for post in transformed["posts"]:
            if isinstance(post, dict) and "time" in post:
                # If timestamp is already ISO string, keep it; if datetime, convert it
                if isinstance(post["time"], str):
                    # Validate and potentially reformat ISO string
                    try:
                        datetime.fromisoformat(post["time"].replace('Z', '+00:00'))
                    except ValueError:
                        # If not valid ISO, set to current time
                        post["time"] = current_time
                elif isinstance(post["time"], datetime):
                    post["time"] = post["time"].isoformat()
                else:
                    # If neither string nor datetime, set to current time
                    post["time"] = current_time

    # Ensure all datetime fields are serializable
    transformed = ensure_datetime_serializable(transformed)
    
    return transformed

def transform_facebook_profile(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform Facebook profile data to BigQuery format - minimal processing"""
    
    # Simply ensure required fields and add timestamps
    transformed = profile_data.copy()
    
    # Ensure ID exists
    if "id" not in transformed:
        transformed["id"] = str(uuid.uuid4())
    
    # Ensure all ID fields are strings (BigQuery STRING columns)
    if "account_id" in transformed and transformed["account_id"] is not None:
        transformed["account_id"] = str(transformed["account_id"])
    if "parent_account_id" in transformed and transformed["parent_account_id"] is not None:
        transformed["parent_account_id"] = str(transformed["parent_account_id"])
    
    # Clean and validate nested data structures
    if "experiences" in transformed and transformed["experiences"] is not None:
        if not isinstance(transformed["experiences"], list):
            transformed["experiences"] = []
        else:
            # Ensure each experience has proper structure
            valid_experiences = []
            for exp in transformed["experiences"]:
                if isinstance(exp, dict):
                    clean_exp = {
                        "title": str(exp.get("title", "")) if exp.get("title") else "",
                        "time": str(exp.get("time", "")) if exp.get("time") else ""
                    }
                    valid_experiences.append(clean_exp)
            transformed["experiences"] = valid_experiences
    else:
        transformed["experiences"] = []
    
    if "educations" in transformed and transformed["educations"] is not None:
        if not isinstance(transformed["educations"], list):
            transformed["educations"] = []
        else:
            # Ensure each education has proper structure
            valid_educations = []
            for edu in transformed["educations"]:
                if isinstance(edu, dict):
                    clean_edu = {
                        "title": str(edu.get("title", "")) if edu.get("title") else "",
                        "time": str(edu.get("time", "")) if edu.get("time") else ""
                    }
                    valid_educations.append(clean_edu)
            transformed["educations"] = valid_educations
    else:
        transformed["educations"] = []
    
    if "posts" in transformed and transformed["posts"] is not None:
        if not isinstance(transformed["posts"], list):
            transformed["posts"] = []
        else:
            # Ensure each post has proper structure
            valid_posts = []
            for post in transformed["posts"]:
                if isinstance(post, dict):
                    clean_post = {
                        "content": str(post.get("content", "")) if post.get("content") else "",
                        "time": str(post.get("time", "")) if post.get("time") else ""
                    }
                    valid_posts.append(clean_post)
            transformed["posts"] = valid_posts
    else:
        transformed["posts"] = []
    
    # Ensure friend_lists is list of strings
    if "friend_lists" in transformed and transformed["friend_lists"] is not None:
        if not isinstance(transformed["friend_lists"], list):
            transformed["friend_lists"] = []
        else:
            # Ensure all friend IDs are strings
            transformed["friend_lists"] = [str(friend_id) for friend_id in transformed["friend_lists"] if friend_id is not None]
    else:
        transformed["friend_lists"] = []
    
    # Ensure languages is list of strings
    if "languages" in transformed and transformed["languages"] is not None:
        if not isinstance(transformed["languages"], list):
            transformed["languages"] = []
        else:
            transformed["languages"] = [str(lang) for lang in transformed["languages"] if lang is not None]
    else:
        transformed["languages"] = []
    
    # Ensure websites is list of strings
    if "websites" in transformed and transformed["websites"] is not None:
        if not isinstance(transformed["websites"], list):
            transformed["websites"] = []
        else:
            transformed["websites"] = [str(website) for website in transformed["websites"] if website is not None]
    else:
        transformed["websites"] = []
    
    # Ensure required fields exist and are not empty
    if "account_id" not in transformed or not transformed["account_id"]:
        raise ValueError("account_id is required for Facebook profiles")
    
    if "username" not in transformed or not transformed["username"]:
        raise ValueError("username is required for Facebook profiles")
    
    # Ensure nullable string fields are not None
    string_fields = ["username", "gender", "birthday", 
                     "current_city", "hometown", "email", "phone_number", "status"]
    for field in string_fields:
        if field in transformed:
            if transformed[field] is None:
                transformed[field] = ""
            else:
                transformed[field] = str(transformed[field])
    
    # Handle "about" field specially as it might be an object
    if "about" in transformed:
        if transformed["about"] is None:
            transformed["about"] = ""
        elif isinstance(transformed["about"], dict):
            # If about is an object, convert it to a string representation or extract relevant data
            transformed["about"] = str(transformed["about"]) if transformed["about"] else ""
        elif isinstance(transformed["about"], list):
            # If about is a list, join it or convert to string
            transformed["about"] = " ".join(str(item) for item in transformed["about"]) if transformed["about"] else ""
        else:
            transformed["about"] = str(transformed["about"])
    
    # Ensure integer fields are properly set
    if "crawl_depth" in transformed:
        if transformed["crawl_depth"] is None:
            transformed["crawl_depth"] = 1
        else:
            transformed["crawl_depth"] = int(transformed["crawl_depth"])
    
    # Add timestamps if not present (as datetime object for BigQuery TIMESTAMP)
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed:
        transformed["created_at"] = current_time
    if "updated_at" not in transformed:
        transformed["updated_at"] = current_time
    
    # Ensure all datetime fields are serializable
    transformed = ensure_datetime_serializable(transformed)
    
    return transformed

def transform_linkedin_url(url_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform LinkedIn URL data to BigQuery format - minimal processing"""
    
    # Simply ensure required fields and add timestamps
    transformed = url_data.copy()
    
    # Ensure required fields exist
    if "id" not in transformed:
        transformed["id"] = str(uuid.uuid4())
    if "status" not in transformed:
        transformed["status"] = "pending"
    # derive account_id from url if not present
    if "account_id" not in transformed and transformed.get("url"):
        try:
            parts = transformed["url"].rstrip('/').split('/')
            if 'in' in parts:
                idx = parts.index('in')
                if idx + 1 < len(parts):
                    transformed["account_id"] = parts[idx + 1].split('?')[0].split('#')[0]
        except Exception:
            pass
    # Add timestamps if not present (as datetime object for BigQuery TIMESTAMP)
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed:
        transformed["created_at"] = current_time
    if "updated_at" not in transformed:
        transformed["updated_at"] = current_time
    
    # Ensure all datetime fields are serializable
    transformed = ensure_datetime_serializable(transformed)
    
    return transformed

def transform_facebook_url(url_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform Facebook URL data to BigQuery format - minimal processing"""
    
    # Simply ensure required fields and add timestamps
    transformed = url_data.copy()
    
    # Ensure required fields exist
    if "id" not in transformed:
        transformed["id"] = str(uuid.uuid4())
    if "status" not in transformed:
        transformed["status"] = "pending"
    
    # Add timestamps if not present (as datetime object for BigQuery TIMESTAMP)
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed:
        transformed["created_at"] = current_time
    if "updated_at" not in transformed:
        transformed["updated_at"] = current_time
    
    # Ensure all datetime fields are serializable
    transformed = ensure_datetime_serializable(transformed)
    
    return transformed

def transform_linkedin_keyword(data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform LinkedIn keyword data to BigQuery format - minimal processing"""
    
    transformed = data.copy()
    
    # Ensure ID exists
    if "id" not in transformed:
        transformed["id"] = str(uuid.uuid4())
    
    # Set default status if not present
    if "status" not in transformed:
        transformed["status"] = "pending"
    if "start" not in transformed:
        transformed["start"] = 0
    # Add timestamps if not present (as datetime object for BigQuery TIMESTAMP)
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed:
        transformed["created_at"] = current_time
    if "updated_at" not in transformed:
        transformed["updated_at"] = current_time
    
    # Ensure all datetime fields are serializable
    transformed = ensure_datetime_serializable(transformed)
    
    return transformed

def transform_facebook_seed_url(data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform Facebook URL follower data to BigQuery format - minimal processing"""
    
    transformed = data.copy()
    
    # Ensure ID exists
    if "id" not in transformed:
        transformed["id"] = str(uuid.uuid4())
    
    # Ensure all ID fields are strings (BigQuery STRING columns)  
    if "id" in transformed and transformed["id"] is not None:
        transformed["id"] = str(transformed["id"])
    
    # Set default status if not present
    if "status" not in transformed:
        transformed["status"] = "pending"
    
    # Set default max_profiles if not present
    if "max_profiles" not in transformed:
        transformed["max_profiles"] = 100
    
    # Add timestamps if not present (as datetime object for BigQuery TIMESTAMP)
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed:
        transformed["created_at"] = current_time
    if "updated_at" not in transformed:
        transformed["updated_at"] = current_time
    
    # Ensure all datetime fields are serializable
    transformed = ensure_datetime_serializable(transformed)
    
    return transformed

# Transformation mapping
TRANSFORM_MAPPING = {
    Platform.LINKEDIN: {
        "profiles": transform_linkedin_profile,
        "urls": transform_linkedin_url,
        "keywords": transform_linkedin_keyword
    },
    Platform.FACEBOOK: {
        "profiles": transform_facebook_profile,
        "urls": transform_facebook_url,
        "seed_urls": transform_facebook_seed_url
    }
}

def transform_data(platform: Platform, table_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform data for a specific platform and table type - minimal processing"""
    if platform not in TRANSFORM_MAPPING:
        raise ValueError(f"Unsupported platform: {platform}")
    
    if table_type not in TRANSFORM_MAPPING[platform]:
        raise ValueError(f"Unsupported table type: {table_type}")
    
    transformer = TRANSFORM_MAPPING[platform][table_type]
    transformed_data = transformer(data)
    
    # Ensure timestamps are present (minimal overhead) as datetime objects
    current_time = datetime.now(timezone.utc)
    if "created_at" not in transformed_data:
        transformed_data["created_at"] = current_time
    if "updated_at" not in transformed_data:
        transformed_data["updated_at"] = current_time
    
    # Final check to ensure all datetime objects are serializable
    transformed_data = ensure_datetime_serializable(transformed_data)
        
    return transformed_data

def transform_batch_data(platform: Platform, table_type: str, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform a batch of data - optimized for performance"""
    # Use list comprehension for better performance
    current_time = datetime.now(timezone.utc)
    transformer = TRANSFORM_MAPPING[platform][table_type]
    
    result = []
    for data in data_list:
        transformed = transformer(data)
        # Ensure timestamps exist as datetime objects
        if "created_at" not in transformed:
            transformed["created_at"] = current_time
        if "updated_at" not in transformed:
            transformed["updated_at"] = current_time
            
        # Final check to ensure all datetime objects are serializable
        transformed = ensure_datetime_serializable(transformed)
        result.append(transformed)
    
    return result

def validate_transformed_data(platform: Platform, table_type: str, data: Dict[str, Any]) -> bool:
    """Validate that transformed data has all required fields - minimal validation"""
    
    # Basic required fields
    required_fields = ["id", "created_at", "updated_at"]
    
    # Add table-specific required fields
    if table_type == "urls":
        required_fields.extend(["url", "status"])
    elif table_type == "profiles":
        # For profiles, just ensure ID and timestamps exist
        # Frontend should provide complete data
        pass
    elif table_type == "keywords":
        # For keywords, ensure ID, status, and timestamps exist
        pass
    
    # Check if all required fields exist
    for field in required_fields:
        if field not in data or data[field] is None:
            return False
            
    return True

def test_transformer_with_sample_data():
    """Test transformer with sample data from extension"""
    
    sample_profile = {
        "id": "test-user",
        "name": "Test User",
        "title": "Software Engineer",
        "location": "Tokyo, Japan",
        "profile_url": "https://www.linkedin.com/in/test-user",
        "profile_image": "https://example.com/image.jpg",
        "about": "Test about section",
        "email": "test@example.com",
        "experiences": [
            {
                "company": "Test Company",
                "position": "Software Engineer",
                "start_date": "Jan 2020",
                "end_date": "Present",
                "duration": "3 years 8 months"
            }
        ],
        "skills": [
            {
                "name": "Python",
                "endorsements": 10
            }
        ],
        "educations": [
            {
                "school": "Test University",
                "degree": "Computer Science",
                "start_date": "2016",
                "end_date": "2020"
            }
        ],
        "languages": [
            {
                "language": "English",
                "proficiency": "Native"
            }
        ],
        "posts": [
            {
                "content": "Test post content",
                "timestamp": "2024-01-01T10:00:00.000Z"
            }
        ],
        "keyword_source": "test keyword",
        "scraped_at": "2024-01-01T10:00:00.000Z"
    }
    
    try:
        transformed = transform_linkedin_profile(sample_profile)
        print("✅ Transformer test successful")
        print(f"Transformed data keys: {list(transformed.keys())}")
        return True
    except Exception as e:
        print(f"❌ Transformer test failed: {e}")
        return False

def ensure_minimal_data(platform: Platform, table_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure minimal required data exists without heavy transformation"""
    
    # Use the appropriate transformer (now minimal)
    transformed = transform_data(platform, table_type, data)
    
    # Validate result
    if not validate_transformed_data(platform, table_type, transformed):
        raise ValueError(f"Data validation failed for {platform.value} {table_type}")
    
    return transformed
