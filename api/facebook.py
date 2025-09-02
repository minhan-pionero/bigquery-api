"""
API route handlers for Facebook crawling operations
"""

import logging
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Path, Query

from config.settings import Platform, TABLE_MAPPING
from services.bigquery_service import bigquery_service
from utils.transformers import transform_data

def validate_facebook_profile_data(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and clean Facebook profile data before processing"""
    # Ensure required fields exist
    if not profile_data.get("account_id"):
        raise ValueError("account_id is required")
    
    # Clean potential problematic fields
    cleaned_data = profile_data.copy()
    
    # Clean up None/null string values  
    for field_name in ["parent_account_id", "account_id", "username"]:
        if field_name in cleaned_data:
            value = cleaned_data[field_name]
            # Convert string representations of None/null to actual None
            if value in ["None", "null", "undefined", ""]:
                cleaned_data[field_name] = None
            elif field_name == "parent_account_id" and value is None:
                # Explicitly keep None for parent_account_id when it should be None
                cleaned_data[field_name] = None
    
    # Ensure nested structures are properly formatted
    for field_name, expected_type in [
        ("experiences", list),
        ("educations", list), 
        ("posts", list),
        ("friend_lists", list),
        ("languages", list),
        ("websites", list)
    ]:
        if field_name in cleaned_data:
            if cleaned_data[field_name] is None:
                cleaned_data[field_name] = []
            elif not isinstance(cleaned_data[field_name], expected_type):
                logger.warning(f"Field {field_name} is not a {expected_type.__name__}, converting")
                cleaned_data[field_name] = []
    
    # Remove any fields with problematic values
    problematic_keys = []
    for key, value in cleaned_data.items():
        if isinstance(value, (set, tuple)):  # Convert sets/tuples to lists
            cleaned_data[key] = list(value)
        elif callable(value):  # Remove function references
            problematic_keys.append(key)
    
    for key in problematic_keys:
        del cleaned_data[key]
        logger.warning(f"Removed problematic field: {key}")
    
    return cleaned_data

logger = logging.getLogger(__name__)
router = APIRouter()

# ================================
# URL FOLLOWER ENDPOINTS
# ================================
@router.get("/facebook/seed-urls/pending")
async def get_pending_seed_urls(
    extension_id: str = Query(..., description="Extension ID to filter URL followers"),
    limit: int = Query(default=10, ge=1, le=100)
):
    """Get pending URL followers for processing (including processing status for this extension)"""
    try:
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["seed_urls"]
        
        query = f"""
        SELECT id, url, max_profiles, status, extension_id
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}`
        WHERE (
            (status = 'pending' OR status IS NULL) 
            OR 
            (status = 'processing' AND extension_id = '{extension_id}')
        )
        ORDER BY 
            CASE WHEN status = 'processing' AND extension_id = '{extension_id}' THEN 0 ELSE 1 END,
            created_at ASC
        LIMIT {limit}
        """
        
        results = bigquery_service.query_table(query)
        
        seed_urls = []
        for row in results:
            seed_urls.append({
                "id": row.id,
                "url": row.url,
                "max_profiles": row.max_profiles if hasattr(row, 'max_profiles') else 200,
                "status": getattr(row, 'status', 'pending'),
                "extension_id": getattr(row, 'extension_id', None)
            })
        
        return {
            "status": "success",
            "pending_seed_urls": seed_urls,
            "count": len(seed_urls),
            "extension_id": extension_id
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting pending Facebook URL followers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/facebook/seed-urls/all")
async def get_all_seed_urls(
    status: Optional[str] = Query(None, description="Filter by status (pending, processing, completed, failed)"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    """Get all URL followers with optional status filter"""
    try:
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["seed_urls"]
        
        # Build WHERE clause
        where_clause = ""
        if status:
            where_clause = f"WHERE status = '{status}'"
        
        query = f"""
        SELECT id, url, max_profiles, status, extension_id, created_at, updated_at, processed_count
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}`
        {where_clause}
        ORDER BY created_at DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = bigquery_service.query_table(query)
        
        seed_urls = []
        for row in results:
            seed_urls.append({
                "id": row.id,
                "url": row.url,
                "max_profiles": row.max_profiles if hasattr(row, 'max_profiles') else 200,
                "status": getattr(row, 'status', 'pending'),
                "extension_id": getattr(row, 'extension_id', None),
                "processed_count": getattr(row, 'processed_count', 0),
                "created_at": row.created_at.isoformat() if hasattr(row, 'created_at') and row.created_at else None,
                "updated_at": row.updated_at.isoformat() if hasattr(row, 'updated_at') and row.updated_at else None
            })
        
        return {
            "status": "success",
            "seed_urls": seed_urls,
            "count": len(seed_urls),
            "filter_status": status,
            "pagination": {
                "limit": limit,
                "offset": offset
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting all Facebook URL followers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/facebook/seed-urls")
async def create_seed_url(
    payload: Dict[str, Any] = ...
):
    """Create a new URL follower"""
    try:
        # Validate required fields
        if "seed_urls" not in payload:
            raise HTTPException(status_code=400, detail="Seed URLs are required")

        # Validate URL format
        seed_urls = payload["seed_urls"]
        if not isinstance(seed_urls, list) or not all(isinstance(url, str) for url in seed_urls):
            raise HTTPException(status_code=400, detail="Seed URLs must be a list of strings")

        for url in seed_urls:
            if not url.startswith("https://www.facebook.com/") or not url.endswith("/followers"):
                raise HTTPException(status_code=400, detail="Each URL must be a Facebook followers URL (e.g., https://www.facebook.com/username/followers)")

        # Create new URL follower
        seed_urls_data = []
        for url in seed_urls:
            seed_url_data = {
                "id": str(uuid.uuid4()),
                "url": url,
                "max_profiles": 100,
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "processed_count": 0
            }

            seed_urls_data.append(seed_url_data)

        # Transform and insert data
        transformed_data = [transform_data(Platform.FACEBOOK, "seed_urls", seed_url) for seed_url in seed_urls_data]
        bigquery_service.insert_if_not_exists(
            Platform.FACEBOOK, 
            "seed_urls", 
            transformed_data, 
            "url"
        )
        
        logger.info(f"✅ Created new Facebook URL follower: {url}")
        return {
            "status": "success",
            "message": "URL follower created successfully",
            "seed_url": {
                "id": seed_url_data["id"],
                "url": url,
                "max_profiles": max_profiles,
                "status": "pending"
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error creating Facebook URL follower: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/facebook/seed-urls/{seed_url_id}/status")
async def update_seed_url_status(
    seed_url_id: str = Path(..., description="URL Follower ID to update"),
    payload: Dict[str, Any] = ...
):
    """Update URL follower status"""
    try:
        allowed_fields = {"status", "processed_count"}
        update_fields = {}

        for field in allowed_fields:
            if field in payload:
                update_fields[field] = payload[field]

        if payload.get("status") == "processing":
            update_fields["processed_at"] = datetime.now(timezone.utc)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        update_fields["updated_at"] = datetime.now(timezone.utc)
        update_fields["id"] = seed_url_id

        bigquery_service.upsert_data(Platform.FACEBOOK, "seed_urls", update_fields, "id")
        
        logger.info(f"✅ Updated Facebook URL follower {seed_url_id}")
        return {
            "status": "success",
            "message": f"URL follower {seed_url_id} updated successfully",
            "seed_url_id": seed_url_id,
            "updated_fields": list(update_fields.keys())
        }
        
    except Exception as e:
        logger.error(f"❌ Error updating Facebook URL follower {seed_url_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/facebook/seed-urls/{seed_url_id}/extension")
async def update_seed_url_extension_id(
    seed_url_id: str = Path(..., description="URL Follower ID to update"),
    payload: Dict[str, Any] = ...
):
    """Update URL follower extension ID (claim ownership)"""
    try:
        if "extension_id" not in payload:
            raise HTTPException(status_code=400, detail="extension_id is required")
        
        update_fields = {
            "extension_id": payload["extension_id"],
            "updated_at": datetime.now(timezone.utc),
            "id": seed_url_id
        }

        bigquery_service.upsert_data(Platform.FACEBOOK, "seed_urls", update_fields, "id")
        
        logger.info(f"✅ Updated Facebook URL follower {seed_url_id} extension_id to {payload['extension_id']}")
        return {
            "status": "success",
            "message": f"URL follower {seed_url_id} extension_id updated successfully",
            "seed_url_id": seed_url_id,
            "extension_id": payload["extension_id"]
        }
        
    except Exception as e:
        logger.error(f"❌ Error updating Facebook URL follower extension_id {seed_url_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ================================
# FACEBOOK PROFILE URL ENDPOINTS
# ================================

@router.post("/facebook/profile_url/batch")
async def insert_profile_urls_batch(profile_urls_data: List[Dict[str, Any]]):
    """Insert multiple profile URLs into the queue for processing"""
    try:
        if not profile_urls_data:
            raise HTTPException(status_code=400, detail="No profile URLs provided")
        
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["urls"]
        logger.info(f"[DEBUG] profile_urls_data: {profile_urls_data}")
        # Transform all profile URLs
        rows_data = [transform_data(Platform.FACEBOOK, "urls", url_data) for url_data in profile_urls_data]
        logger.info(f"[DEBUG] Profile URLs to insert: {rows_data}")
        
        # Insert into BigQuery
        bigquery_service.insert_if_not_exists(Platform.FACEBOOK, "urls", rows_data, unique_key="account_id")
        
        logger.info(f"✅ Facebook profile URLs batch inserted {len(profile_urls_data)} URLs")
        return {
            "status": "success",
            "message": f"Facebook profile URLs batch inserted {len(profile_urls_data)} URLs successfully",
            "count": len(profile_urls_data),
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"❌ Error in Facebook profile URLs batch insert: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/facebook/profile_url/pending")
async def get_pending_profile_urls(
    limit: int = Query(default=200, ge=1, le=500),
    crawl_depth: Optional[int] = Query(default=None, description="Filter by crawl depth"),
    seed_url_id: Optional[str] = Query(default=None, description="Filter by URL follower ID")
):
    """Get pending profile URLs for processing"""
    try:
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["urls"]
        
        where_clause = "WHERE status = 'pending' OR status IS NULL"
        if crawl_depth is not None:
            where_clause += f" AND crawl_depth = {crawl_depth}"
        if seed_url_id is not None:
            where_clause += f" AND seed_url_id = '{seed_url_id}'"
        
        query = f"""
        SELECT id, account_id, url, parent_account_id, crawl_depth, source_type, seed_url_id
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}`
        {where_clause}
        ORDER BY crawl_depth ASC, created_at ASC
        LIMIT {limit}
        """
        
        results = bigquery_service.query_table(query)
        
        profile_urls = []
        for row in results:
            profile_urls.append({
                "id": row.id,
                "account_id": row.account_id,
                "url": row.url,
                "parent_account_id": getattr(row, 'parent_account_id', None),
                "crawl_depth": getattr(row, 'crawl_depth', 1),
                "source_type": getattr(row, 'source_type', 'follower'),
                "seed_url_id": getattr(row, 'seed_url_id', None)
            })
        
        return {
            "status": "success",
            "data": profile_urls,  # Changed from pending_profile_urls to data to match frontend
            "count": len(profile_urls),
            "filter_crawl_depth": crawl_depth,
            "filter_seed_url_id": seed_url_id
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting pending Facebook profile URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/facebook/profile_url/pending_and_processing")
async def get_pending_and_processing_profile_urls(
    limit: int = Query(default=200, ge=1, le=500),
    crawl_depth: Optional[int] = Query(default=None, description="Filter by crawl depth"),
    seed_url_id: Optional[str] = Query(default=None, description="Filter by URL follower ID")
):
    """Get pending AND processing profile URLs for processing (resume functionality)
    
    Priority order:
    1. Lower crawl_depth first (depth 1 before depth 2, etc.)
    2. 'processing' status first (resume interrupted work)
    3. 'pending' status second (new work)
    4. Oldest created_at first
    """
    try:
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["urls"]
        
        where_clause = "WHERE (status = 'pending' OR status = 'processing' OR status = 'failed' OR status = 'skipped' OR status IS NULL)"
        if crawl_depth is not None:
            where_clause += f" AND crawl_depth = {crawl_depth}"
        if seed_url_id is not None:
            where_clause += f" AND seed_url_id = '{seed_url_id}'"
        
        query = f"""
        SELECT id, account_id, url, parent_account_id, crawl_depth, source_type, seed_url_id, status
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}`
        {where_clause}
        ORDER BY 
            crawl_depth ASC,
            CASE 
                WHEN status = 'processing' THEN 0 
                WHEN status = 'failed' THEN 1
                WHEN status = 'skipped' THEN 2
                WHEN status = 'pending' OR status IS NULL THEN 3
                ELSE 4
            END,
            created_at ASC
        LIMIT {limit}
        """
        
        results = bigquery_service.query_table(query)
        
        profile_urls = []
        for row in results:
            profile_urls.append({
                "id": row.id,
                "account_id": row.account_id,
                "url": row.url,
                "parent_account_id": getattr(row, 'parent_account_id', None),
                "crawl_depth": getattr(row, 'crawl_depth', 1),
                "source_type": getattr(row, 'source_type', 'follower'),
                "seed_url_id": getattr(row, 'seed_url_id', None),
                "status": getattr(row, 'status', 'pending')  # Include status for resume logic
            })
        
        return {
            "status": "success",
            "data": profile_urls,
            "count": len(profile_urls),
            "filter_crawl_depth": crawl_depth,
            "filter_seed_url_id": seed_url_id,
            "note": "Includes both pending and processing status profiles. Processing profiles are prioritized for resume functionality."
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting pending and processing Facebook profile URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/facebook/profile_url/{profile_url_id}/status")
async def update_profile_url_status(
    profile_url_id: str = Path(..., description="Profile URL ID to update"),
    payload: Dict[str, Any] = ...
):
    """Update profile URL status"""
    try:
        allowed_fields = {"status"}
        update_fields = {}

        for field in allowed_fields:
            if field in payload:
                update_fields[field] = payload[field]

        if payload.get("status") == "processing":
            update_fields["processed_at"] = datetime.now(timezone.utc)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        update_fields["updated_at"] = datetime.now(timezone.utc)
        update_fields["id"] = profile_url_id

        bigquery_service.upsert_data(Platform.FACEBOOK, "urls", update_fields, "id")
        
        logger.info(f"✅ Updated Facebook profile URL {profile_url_id}")
        return {
            "status": "success",
            "message": f"Profile URL {profile_url_id} updated successfully",
            "profile_url_id": profile_url_id,
            "updated_fields": list(update_fields.keys())
        }
        
    except Exception as e:
        logger.error(f"❌ Error updating Facebook profile URL {profile_url_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ================================
# FACEBOOK PROFILE ENDPOINTS
# ================================

@router.post("/facebook/profile")
async def insert_facebook_profile(profile_data: Dict[str, Any]):
    """Insert a Facebook profile and create profile URLs from friend list"""
    try:
        logger.info(f"[DEBUG] Received profile_data: {profile_data}")
        # Log incoming data for debugging
        logger.info(f"[DEBUG] Received profile_data keys: {list(profile_data.keys())}")
        
        # Validate and clean data
        cleaned_profile_data = validate_facebook_profile_data(profile_data)
        logger.info(f"[DEBUG] Cleaned profile_data keys: {list(cleaned_profile_data.keys())}")
        
        # Insert profile
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["profiles"]
        row_data = transform_data(Platform.FACEBOOK, "profiles", cleaned_profile_data)

        bigquery_service.insert_if_not_exists(Platform.FACEBOOK, "profiles", [row_data], unique_key="account_id")
        
        # Create profile URLs from friend list if exists
        friend_urls_created = 0
        if profile_data.get("friend_lists"):
            friend_urls = []
            current_depth = profile_data.get("crawl_depth", 1)
            next_depth = current_depth + 1
            
            # Only create URLs if we haven't reached max depth (assuming CRAWL_DEPTH = 3)
            if next_depth <= 3:  # CRAWL_DEPTH from config
                for friend_account_id in profile_data["friend_lists"]:
                    friend_url_data = {
                        "account_id": friend_account_id,
                        "url": f"https://www.facebook.com/{friend_account_id}",
                        "parent_account_id": profile_data["account_id"],
                        "crawl_depth": next_depth,
                        "source_type": "friend",
                        "status": "pending"
                    }
                    friend_urls.append(transform_data(Platform.FACEBOOK, "urls", friend_url_data))
                
                # Batch insert friend URLs
                if friend_urls:
                    bigquery_service.insert_if_not_exists(Platform.FACEBOOK, "urls", friend_urls, unique_key="account_id")
                    friend_urls_created = len(friend_urls)
        
        logger.info(f"✅ Facebook profile inserted: {profile_data.get('username', 'Unknown')} + {friend_urls_created} friend URLs")
        return {
            "status": "success",
            "message": "Facebook profile and friend URLs inserted successfully",
            "profile_id": row_data["id"],
            "friend_urls_created": friend_urls_created,
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"❌ Error inserting Facebook profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/facebook/profile/batch")  
async def insert_facebook_profiles_batch(profiles_data: List[Dict[str, Any]]):
    """Insert multiple Facebook profiles and create profile URLs from their friend lists"""
    try:
        if not profiles_data:
            raise HTTPException(status_code=400, detail="No profiles provided")
        
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["profiles"]
        
        # Transform all profiles
        rows_data = [transform_data(Platform.FACEBOOK, "profiles", profile) for profile in profiles_data]
        
        # Insert profiles
        success, errors = bigquery_service.insert_rows(Platform.FACEBOOK, "profiles", rows_data)
        
        if not success:
            logger.error(f"❌ BigQuery batch profile insert errors: {errors}")
            raise HTTPException(status_code=500, detail=f"Batch profile insert failed: {errors}")
        
        # Create friend URLs from all profiles
        all_friend_urls = []
        for profile_data in profiles_data:
            if profile_data.get("friend_lists"):
                current_depth = profile_data.get("crawl_depth", 1)
                next_depth = current_depth + 1
                
                # Only create URLs if we haven't reached max depth
                if next_depth <= 3:  # CRAWL_DEPTH from config
                    for friend_account_id in profile_data["friend_lists"]:
                        friend_url_data = {
                            "account_id": friend_account_id,
                            "url": f"https://www.facebook.com/{friend_account_id}",
                            "parent_account_id": profile_data["account_id"],
                            "crawl_depth": next_depth,
                            "source_type": "friend",
                            "status": "pending"
                        }
                        all_friend_urls.append(transform_data(Platform.FACEBOOK, "urls", friend_url_data))
        
        # Batch insert all friend URLs
        friend_urls_created = 0
        if all_friend_urls:
            bigquery_service.insert_if_not_exists(Platform.FACEBOOK, "urls", all_friend_urls, unique_key="account_id")
            friend_urls_created = len(all_friend_urls)
        
        logger.info(f"✅ Facebook batch inserted {len(profiles_data)} profiles + {friend_urls_created} friend URLs")
        return {
            "status": "success",
            "message": f"Facebook batch inserted {len(profiles_data)} profiles and {friend_urls_created} friend URLs successfully",
            "profiles_count": len(profiles_data),
            "friend_urls_created": friend_urls_created,
            "table": table_name
        }
        
    except Exception as e:
        logger.error(f"❌ Error in Facebook batch profile insert: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ================================
# STATISTICS & MONITORING
# ================================

@router.get("/facebook/stats")
async def get_facebook_crawl_stats():
    """Get Facebook crawling statistics"""
    try:
        seed_url_table = TABLE_MAPPING[Platform.FACEBOOK]["seed_urls"]
        profile_url_table = TABLE_MAPPING[Platform.FACEBOOK]["urls"] 
        profile_table = TABLE_MAPPING[Platform.FACEBOOK]["profiles"]
        
        # URL Follower stats
        seed_url_query = f"""
        SELECT status, extension_id, COUNT(*) as count
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{seed_url_table}`
        GROUP BY status, extension_id
        ORDER BY status, extension_id
        """
        seed_url_results = bigquery_service.query_table(seed_url_query)
        seed_url_stats = {}
        for row in seed_url_results:
            status = row.status or 'pending'
            extension_id = row.extension_id or 'unassigned'
            if status not in seed_url_stats:
                seed_url_stats[status] = {}
            seed_url_stats[status][extension_id] = row.count
        
        # Profile URL stats  
        profile_url_query = f"""
        SELECT status, crawl_depth, COUNT(*) as count
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{profile_url_table}`
        GROUP BY status, crawl_depth
        ORDER BY crawl_depth
        """
        profile_url_results = bigquery_service.query_table(profile_url_query)
        
        profile_url_stats = {}
        for row in profile_url_results:
            depth = row.crawl_depth or 1
            status = row.status or 'pending'
            if depth not in profile_url_stats:
                profile_url_stats[depth] = {}
            profile_url_stats[depth][status] = row.count
            
        # Profile stats
        profile_query = f"""
        SELECT status, crawl_depth, COUNT(*) as count
        FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{profile_table}`
        GROUP BY status, crawl_depth
        ORDER BY crawl_depth  
        """
        profile_results = bigquery_service.query_table(profile_query)
        
        profile_stats = {}
        for row in profile_results:
            depth = row.crawl_depth or 1
            status = row.status or 'pending'
            if depth not in profile_stats:
                profile_stats[depth] = {}
            profile_stats[depth][status] = row.count
        
        return {
            "status": "success",
            "seed_urls": seed_url_stats,
            "profile_urls": profile_url_stats,
            "profiles": profile_stats
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting Facebook crawl stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/facebook/profile/completed")
async def get_completed_profiles_by_seed_url(
    seed_url_id: Optional[str] = Query(None, description="URL follower ID to filter by")
):
    """Get completed profiles by URL follower ID for lineage tracing"""
    try:
        table_name = TABLE_MAPPING[Platform.FACEBOOK]["profiles"]
        
        if seed_url_id:
            # Join facebook_profile with facebook_profile_url to get lineage information
            # This approach uses proper relational joins instead of duplicating data
            profile_url_table = TABLE_MAPPING[Platform.FACEBOOK]["urls"]
            
            query = f"""
            WITH direct_profile_urls AS (
                -- Direct profile URLs from URL follower (depth 1)
                SELECT pu.account_id, pu.parent_account_id, pu.crawl_depth, 
                       pu.seed_url_id, pu.status as url_status
                FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{profile_url_table}` pu
                WHERE pu.seed_url_id = '{seed_url_id}'
                  AND pu.status = 'completed'
            ),
            all_lineage_profile_urls AS (
                -- Include direct profile URLs
                SELECT * FROM direct_profile_urls
                
                UNION ALL
                
                -- Include friend profile URLs (depth 2+) whose parent is in our lineage
                SELECT pu.account_id, pu.parent_account_id, pu.crawl_depth, 
                       pu.seed_url_id, pu.status as url_status
                FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{profile_url_table}` pu
                WHERE pu.status = 'completed'
                  AND pu.parent_account_id IS NOT NULL
                  AND pu.parent_account_id IN (
                      SELECT account_id FROM direct_profile_urls
                  )
            )
            SELECT lpu.account_id, lpu.parent_account_id, lpu.crawl_depth, lpu.seed_url_id,
                   p.username, p.profile_image, p.created_at as processed_at
            FROM all_lineage_profile_urls lpu
            INNER JOIN `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}` p
            ON lpu.account_id = p.account_id
            ORDER BY lpu.crawl_depth, p.created_at
            """
            
            query_params = []
        else:
            # Get all completed profiles if no seed_url_id specified (using JOIN)
            profile_url_table = TABLE_MAPPING[Platform.FACEBOOK]["urls"]
            
            query = f"""
            SELECT pu.account_id, pu.parent_account_id, pu.crawl_depth, pu.seed_url_id,
                   p.username, p.profile_image, p.created_at as processed_at
            FROM `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{profile_url_table}` pu
            INNER JOIN `{bigquery_service.project_id}.{bigquery_service.dataset_id}.{table_name}` p
            ON pu.account_id = p.account_id
            WHERE pu.status = 'completed'
            ORDER BY pu.crawl_depth, p.created_at
            """
            query_params = []
        
        # Execute query
        if query_params:
            job_config = bigquery_service.client.QueryJobConfig(query_parameters=query_params)
            results = bigquery_service.client.query(query, job_config=job_config).result()
        else:
            results = bigquery_service.query_table(query)
        
        # Convert results to list of dictionaries
        completed_profiles = []
        for row in results:
            profile_data = {
                "account_id": row.account_id,
                "parent_account_id": row.parent_account_id,
                "crawl_depth": row.crawl_depth,
                "status": "completed",  # All profiles returned are completed
                "seed_url_id": row.seed_url_id,
                "processed_at": row.processed_at.isoformat() if row.processed_at else None,
                "username": row.username,
                "profile_image": row.profile_image
            }
            completed_profiles.append(profile_data)
        
        logger.info(f"✅ Found {len(completed_profiles)} completed profiles for URL follower: {seed_url_id}")
        return {
            "status": "success", 
            "data": completed_profiles,
            "count": len(completed_profiles),
            "seed_url_id": seed_url_id
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting completed profiles by URL follower {seed_url_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
