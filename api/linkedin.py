import logging
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Path, Query, Body
from config.settings import Platform, TABLE_MAPPING
from services.bigquery_service import bigquery_service
from utils.transformers import transform_data


logger = logging.getLogger(__name__)
router = APIRouter()

# ================================
# KEYWORDS ENDPOINTS
# ================================
@router.get("/keywords/linkedin/pending")
async def get_pending_keywords(
    limit: int = Query(default=100, ge=1, le=1000),
    extension_id: Optional[str] = Query(default=None, description="Extension ID to filter keywords")
):
    """Get pending/processing keywords for a platform, optionally filtered by extension_id"""
    try:
        keywords = bigquery_service.get_pending_keywords(Platform.LINKEDIN, limit, extension_id)
        return {
            "status": "success",
            "keywords": keywords,  # list of {id, keyword, status, start, extension_id}
            "count": len(keywords)
        }
    except Exception as e:
        logger.error(f"Error getting pending linkedin keywords: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/keywords/linkedin")
async def insert_keyword(
    payload: Dict[str, Any] = ...
):
    """Insert new keywords (single keyword or array of keywords)"""
    try:
        # Support both single keyword and array of keywords
        keywords_to_insert = []
        
        if "keyword" in payload:
            # Single keyword (backward compatibility)
            if not payload.get("keyword"):
                raise HTTPException(status_code=400, detail="keyword is required")
            keywords_to_insert.append(payload["keyword"])
            
        elif "keywords" in payload:
            # Array of keywords (new feature)
            if not payload.get("keywords") or not isinstance(payload["keywords"], list):
                raise HTTPException(status_code=400, detail="keywords must be a non-empty array")
            keywords_to_insert = payload["keywords"]
            
        else:
            raise HTTPException(status_code=400, detail="Either 'keyword' or 'keywords' is required")
        
        # Remove empty keywords and duplicates
        keywords_to_insert = list(set([k.strip() for k in keywords_to_insert if k and k.strip()]))
        
        if not keywords_to_insert:
            raise HTTPException(status_code=400, detail="No valid keywords provided")
        
        # Create keyword data for all keywords
        keywords_data = []
        for keyword in keywords_to_insert:
            keyword_data = {
                "id": str(uuid.uuid4()),
                "keyword": f'{keyword} site:linkedin.com',
                "start": 0,
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
            keywords_data.append(keyword_data)
        
        # Transform and insert all keywords
        transformed_data = [transform_data(Platform.LINKEDIN, "keywords", kw_data) for kw_data in keywords_data]
        bigquery_service.insert_if_not_exists(
            Platform.LINKEDIN,
            "keywords",
            transformed_data,
            "keyword"
        )

        return {
            "status": "success", 
            "message": f"Successfully inserted {len(keywords_data)} keywords", 
            "count": len(keywords_data),
            "keywords": [{"id": kw["id"], "keyword": kw["keyword"]} for kw in keywords_data]
        }
    except Exception as e:
        logger.error(f"Error inserting keywords: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/keywords/linkedin/processing") 
async def update_keyword_to_processing(
    payload: Dict[str, Any] = Body(...)
):
    """Update keyword to processing status and set extension_id in one call"""
    try:
        keyword_id = payload.get("id")
        extension_id = payload.get("extension_id")
        if not keyword_id or not extension_id:
            logger.error(f"❌ Missing required fields - keyword_id: {keyword_id}, extension_id: {extension_id}")
            raise HTTPException(status_code=400, detail="id and extension_id are required")
        
        update_fields = {
            "id": keyword_id,
            "status": "processing",
            "extension_id": extension_id,
            "processed_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        logger.error(f"Calling upsert_data with fields: {update_fields}")
        bigquery_service.upsert_data(Platform.LINKEDIN, "keywords", update_fields, "id")
        logger.error(f"Successfully updated keyword {keyword_id} to processing")
        return {"status": "success", "id": keyword_id, "extension_id": extension_id, "new_status": "processing"}
    except Exception as e:
        logger.error(f"❌ Error updating keyword to processing for linkedin: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/keywords/linkedin/current_start")
async def update_keyword_current_start(
    payload: Dict[str, Any] = ...
):
    """Update currentStart for a keyword by id"""
    try:
        keyword_id = payload.get("id")
        current_start = payload.get("current_start")
        if not keyword_id or current_start is None:
            raise HTTPException(status_code=400, detail="id and current_start are required")
        
        update_fields = {
            "id": keyword_id,
            "start": int(current_start),
            "updated_at": datetime.now(timezone.utc)
        }

        bigquery_service.upsert_data(Platform.LINKEDIN, "keywords", update_fields, "id")
        return {"status": "success", "id": keyword_id, "current_start": current_start}
    except Exception as e:
        logger.error(f"❌ Error updating current_start for linkedin keyword: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/keywords/linkedin/extension_id")
async def update_keyword_extension_id(
    payload: Dict[str, Any] = ...
):
    """Update extension_id for a keyword by id"""
    try:
        keyword_id = payload.get("id")
        extension_id = payload.get("extension_id")
        if not keyword_id or not extension_id:
            raise HTTPException(status_code=400, detail="id and extension_id are required")
        
        update_fields = {
            "id": keyword_id,
            "extension_id": extension_id,
            "updated_at": datetime.now(timezone.utc)
        }
        
        bigquery_service.upsert_data(Platform.LINKEDIN, "keywords", update_fields, "id")
        return {"status": "success", "id": keyword_id, "extension_id": extension_id}
    except Exception as e:
        logger.error(f"❌ Error updating extension_id for linkedin keyword: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/keywords/linkedin/{keyword_id}")
async def update_keyword(
    keyword_id: str = Path(..., description="Keyword ID"),
    payload: Dict[str, Any] = Body(...)
):
    """Update fields for a keyword by id in BigQuery"""
    try:
        # Cho phép update các field này
        allowed_fields = {"status", "start"}
        update_fields = {}

        # Lọc field hợp lệ
        for field in allowed_fields:
            if field in payload:
                update_fields[field] = payload[field]

        # Nếu status = processing → update thêm processed_at
        if payload.get("status") == "processing":
            update_fields["processed_at"] = datetime.now(timezone.utc)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        # Luôn update updated_at
        update_fields["updated_at"] = datetime.now(timezone.utc)
        update_fields["id"] = keyword_id  # Ensure id is included for upsert

        bigquery_service.upsert_data(Platform.LINKEDIN, "keywords", update_fields, "id")

        return {
            "status": "success",
            "message": f"Keyword {keyword_id} updated successfully",
            "keyword_id": keyword_id,
            "updated_fields": list(update_fields.keys())
        }

    except Exception as e:
        logger.error(f"Error updating linkedin keyword {keyword_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ================================
# LINKEDIN PROFILE URL ENDPOINTS
# ================================
@router.post("/linkedin/profile_url/batch")
async def insert_urls_batch(
    urls_data: List[Dict[str, Any]] = ...
):
    """Insert multiple URLs into the queue for processing"""
    try:
        if not urls_data:
            raise HTTPException(status_code=400, detail="No URLs provided")
        
        # Get table name for this platform
        table_name = TABLE_MAPPING[Platform.LINKEDIN]["urls"]

        # Transform all URLs
        rows_data = [transform_data(Platform.LINKEDIN, "urls", url_data) for url_data in urls_data]

        # Insert into BigQuery
        bigquery_service.insert_if_not_exists(Platform.LINKEDIN, "urls", rows_data, unique_key="account_id")

        logger.info(f"{Platform.LINKEDIN.value.title()} batch inserted {len(urls_data)} URLs")
        return {
            "status": "success",
            "message": f"{Platform.LINKEDIN.value.title()} batch inserted {len(urls_data)} URLs successfully",
            "count": len(urls_data),
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"Error in {Platform.LINKEDIN.value.title()} URLs batch insert: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/linkedin/profile_url/keyword/{keyword_id}")
async def get_urls_by_keyword(
    keyword_id: str = Path(..., description="Keyword ID to filter URLs")
):
    """Query URLs from BigQuery for specific platform and keyword ID"""
    try:
        # Get table name for this platform
        table_name = TABLE_MAPPING[Platform.LINKEDIN]["urls"]

        query = f"""
        SELECT 
            id, url, status, keyword_id
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}`
        WHERE keyword_id = '{keyword_id}'
        ORDER BY created_at ASC
        """
        results = bigquery_service.query_table(query)

        urls = []
        for row in results:
            urls.append({
                "id": row.id,
                "url": row.url,
                "status": row.status,
                "keyword_id": row.keyword_id,
            })

        return {
            "status": "success",
            "urls": urls,
            "count": len(urls),
            "table": table_name,
            "filter_keyword_id": keyword_id
        }

    except Exception as e:
        logger.error(f"Error querying {Platform.LINKEDIN.value.title()} URLs by keyword {keyword_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/linkedin/profile_url/{url_id}")
async def update_url_status(
    url_id: str = Path(..., description="URL ID to update"),
    payload: Dict[str, Any] = ...
):
    """Update URL status and processing information"""
    try:
        # Cho phép update các field này
        allowed_fields = {"status"}
        update_fields = {}

        # Lọc field hợp lệ
        for field in allowed_fields:
            if field in payload:
                update_fields[field] = payload[field]

        # Nếu status = processing → update thêm processed_at
        if payload.get("status") == "processing":
            update_fields["processed_at"] = datetime.now(timezone.utc)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        # Luôn update updated_at
        update_fields["updated_at"] = datetime.now(timezone.utc)
        update_fields["id"] = url_id  # Ensure id is included for upsert

        bigquery_service.upsert_data(Platform.LINKEDIN, "urls", update_fields, "id")
        
        logger.info(f"Updated linkedin URL {url_id}")
        return {
            "status": "success",
            "message": f"URL {url_id} updated successfully",
            "url_id": url_id,
            "updated_fields": list(update_fields.keys())
        }
        
    except Exception as e:
        logger.error(f"Error updating linkedin URL {url_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ================================
# LINKEDIN PROFILE ENDPOINTS
# ================================
@router.post("/linkedin/profile")
async def insert_profile(
    profile_data: Dict[str, Any] = ...
):
    """Insert a single profile into BigQuery for specific platform"""
    try:
        # Get table name for this platform
        table_name = TABLE_MAPPING[Platform.LINKEDIN]["profiles"]
        
        # Transform profile data
        row_data = transform_data(Platform.LINKEDIN, "profiles", profile_data)

        bigquery_service.upsert_data(Platform.LINKEDIN, "profiles", row_data, "account_id")

        logger.info(f"{Platform.LINKEDIN.value.title()} profile inserted: {profile_data.get('name', 'Unknown')}")
        return {
            "status": "success",
            "message": f"{Platform.LINKEDIN.value.title()} profile inserted successfully",
            "id": row_data["id"],
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"Error inserting linkedin profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/linkedin/profile/batch")
async def insert_profiles_batch(
    platform: Platform = Path(..., description="Platform name (linkedin/facebook)"),
    profiles_data: List[Dict[str, Any]] = ...
):
    """Insert multiple profiles into BigQuery for specific platform"""
    try:
        if not profiles_data:
            raise HTTPException(status_code=400, detail="No profiles provided")
        
        # Get table name for this platform
        table_name = TABLE_MAPPING[platform]["profiles"]
        
        # Transform all profiles
        rows_data = [transform_data(platform, "profiles", profile) for profile in profiles_data]
        
        # Insert into BigQuery
        success, errors = bigquery_service.insert_rows(platform, "profiles", rows_data)
        
        if not success:
            logger.error(f"BigQuery batch insert errors: {errors}")
            raise HTTPException(status_code=500, detail=f"Batch insert failed: {errors}")
        
        logger.info(f"{platform.value.title()} batch inserted {len(profiles_data)} profiles")
        return {
            "status": "success",
            "message": f"{platform.value.title()} batch inserted {len(profiles_data)} profiles successfully",
            "count": len(profiles_data),
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"Error in {platform.value} batch insert: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/linkedin/profile")
async def get_profiles(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0)
):
    """Query profiles from BigQuery for specific platform"""
    try:
        # Get table name for this platform
        table_name = TABLE_MAPPING[Platform.LINKEDIN]["profiles"]

        query = f"""
        SELECT 
            id, username, title, location, updated_at
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}`
        WHERE platform = '{Platform.LINKEDIN.value}'
        ORDER BY updated_at DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = bigquery_service.query_table(query)
        
        profiles = []
        for row in results:
            profiles.append({
                "id": row.id,
                "username": row.username,
                "title": row.title,
                "location": row.location,
            })
        
        return {
            "status": "success",
            "profiles": profiles,
            "count": len(profiles),
            "limit": limit,
            "offset": offset,
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"Error querying linkedin profiles: {e}")
        raise HTTPException(status_code=500, detail=str(e))
