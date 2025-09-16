"""
External API service for SERP API and ProAPIs integration
"""

import os
import logging
import httpx
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urlencode
from config.settings import SERP_API_CONFIG, PROAPIS_CONFIG

logger = logging.getLogger(__name__)

class ExternalAPIService:
    def __init__(self):
        self.serp_api_key = os.environ.get("SERPAPI_KEY", SERP_API_CONFIG["api_key"])
        self.proapis_key = os.environ.get("PROAPIS_KEY", PROAPIS_CONFIG["api_key"])
        
        if not self.serp_api_key:
            logger.warning("SERPAPI_KEY not found in environment variables")
        if not self.proapis_key:
            logger.warning("PROAPIS_KEY not found in environment variables")

    def normalize_linkedin_url(self, url: str) -> str:
        """Normalize LinkedIn URL to standard format"""
        try:
            # Decode URL if encoded
            from urllib.parse import unquote
            decoded_url = unquote(url)
            logger.debug(f"URL decoded: {url} -> {decoded_url}")
            
            # Parse URL and extract username
            parsed = urlparse(decoded_url)
            path_parts = parsed.path.split('/')
            
            # Find 'in' segment in path
            if 'in' in path_parts:
                in_index = path_parts.index('in')
                if in_index + 1 < len(path_parts):
                    username = path_parts[in_index + 1]
                    # Remove query params and fragments
                    username = username.split('?')[0].split('#')[0].rstrip('/')
                    
                    normalized_url = f"https://www.linkedin.com/in/{username}"
                    if normalized_url != url:
                        logger.debug(f"URL normalized: {url} -> {normalized_url}")
                    return normalized_url
            
            logger.warning(f"Invalid LinkedIn URL format: {decoded_url}")
            return url
            
        except Exception as e:
            logger.error(f"Error normalizing URL {url}: {e}")
            return url

    async def search_linkedin_profiles(self, keyword: str, start: int = 0) -> Dict[str, Any]:
        """Search LinkedIn profiles using SERP API"""
        try:
            if not self.serp_api_key:
                raise ValueError("SERP API key is not configured")
            
            # Build search parameters
            params = {
                "api_key": self.serp_api_key,
                "engine": "google",
                "q": keyword,
                "location": SERP_API_CONFIG["default_location"],
                "google_domain": SERP_API_CONFIG["default_domain"],
                "gl": SERP_API_CONFIG["default_gl"],
                "hl": SERP_API_CONFIG["default_hl"],
                "num": str(SERP_API_CONFIG["max_results_per_call"]),
                "start": str(start)
            }
            
            api_url = f"{SERP_API_CONFIG['base_url']}?{urlencode(params)}"
            logger.info(f"Calling SERP API with keyword: {keyword}, start: {start}")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(api_url, timeout=30.0)
                
                if not response.is_success:
                    raise Exception(f"SERP API request failed: {response.status_code} {response.text}")
                
                data = response.json()
                
                if not data.get("organic_results"):
                    logger.info(f"No organic results found for keyword: {keyword}")
                    return {
                        "success": True,
                        "profiles": [],
                        "total_found": 0,
                        "keyword": keyword,
                        "start": start
                    }
                
                # Filter and normalize LinkedIn profile URLs
                profile_urls = []
                for result in data["organic_results"]:
                    link = result.get("link", "")
                    if link and "linkedin.com/in/" in link:
                        normalized_url = self.normalize_linkedin_url(link)
                        if normalized_url:
                            profile_urls.append({
                                "url": normalized_url,
                                "title": result.get("title", ""),
                                "snippet": result.get("snippet", "")
                            })
                
                logger.info(f"Found {len(profile_urls)} LinkedIn profiles for keyword: {keyword}")
                return {
                    "success": True,
                    "profiles": profile_urls,
                    "total_found": len(profile_urls),
                    "keyword": keyword,
                    "start": start
                }
                
        except Exception as e:
            logger.error(f"SERP API error for keyword '{keyword}': {e}")
            return {
                "success": False,
                "error": str(e),
                "profiles": [],
                "total_found": 0,
                "keyword": keyword,
                "start": start
            }

    def extract_profile_id(self, profile_url: str) -> Optional[str]:
        """Extract LinkedIn profile ID from URL"""
        try:
            parsed_url = urlparse(profile_url)
            path_parts = parsed_url.path.split('/')
            
            if 'in' in path_parts:
                in_index = path_parts.index('in')
                if in_index + 1 < len(path_parts):
                    return path_parts[in_index + 1]
            
            raise ValueError('Invalid LinkedIn URL format')
            
        except Exception as e:
            logger.error(f"Error extracting profile ID from {profile_url}: {e}")
            return None

    async def get_profile_details(self, profile_url: str) -> Dict[str, Any]:
        """Get profile details from ProAPIs"""
        try:
            if not self.proapis_key:
                raise ValueError("ProAPIs key is not configured")
            
            profile_id = self.extract_profile_id(profile_url)
            if not profile_id:
                raise ValueError("Could not extract profile ID from URL")
            
            logger.info(f"Fetching profile details for: {profile_id}")
            
            headers = {
                'Content-Type': 'application/json',
                'X-Api-Key': self.proapis_key
            }
            
            payload = {
                "profile_id": profile_id,
                "profile_type": "personal",
                "bypass_cache": True,
                "related_profiles": True,
                "network_info": True,
                "contact_info": True,
                "verifications_info": True
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{PROAPIS_CONFIG['base_url']}/profile-details",
                    headers=headers,
                    json=payload,
                    timeout=30.0
                )
                
                if not response.is_success:
                    error_text = response.text
                    raise Exception(f"ProAPIs request failed: {response.status_code} {response.reason_phrase} - {error_text}")
                
                data = response.json()
                logger.info(f"Profile details fetched successfully for: {profile_id}")
                
                return {
                    "success": True,
                    "data": data,
                    "entity_urn": data.get("entity_urn") or data.get("profile_id"),
                    "profile_url": profile_url
                }
                
        except Exception as e:
            logger.error(f"Error fetching profile details for {profile_url}: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": None,
                "entity_urn": None,
                "profile_url": profile_url
            }

    async def get_profile_activities(self, entity_urn: str) -> Dict[str, Any]:
        """Get profile activities from ProAPIs"""
        try:
            if not self.proapis_key:
                raise ValueError("ProAPIs key is not configured")
            
            if not entity_urn:
                raise ValueError("Entity URN is required")
            
            logger.info(f"Fetching activities for entity: {entity_urn}")
            
            headers = {
                'X-Api-Key': self.proapis_key
            }
            
            params = {
                'profile_id': entity_urn,
                'activity_type': 'posts'
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{PROAPIS_CONFIG['base_url']}/people-activities/list-activities",
                    headers=headers,
                    params=params,
                    timeout=30.0
                )
                
                if not response.is_success:
                    error_text = response.text
                    raise Exception(f"ProAPIs request failed: {response.status_code} {response.reason_phrase} - {error_text}")
                
                data = response.json()
                logger.info(f"Activities fetched successfully for: {entity_urn}")
                
                return {
                    "success": True,
                    "data": data,
                    "entity_urn": entity_urn
                }
                
        except Exception as e:
            logger.error(f"Error fetching activities for {entity_urn}: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": None,
                "entity_urn": entity_urn
            }

    async def get_full_profile(self, profile_url: str) -> Dict[str, Any]:
        """Get complete profile data (details + activities)"""
        try:
            logger.info(f"Starting full profile fetch for: {profile_url}")
            
            # Step 1: Get profile details
            profile_result = await self.get_profile_details(profile_url)
            
            if not profile_result["success"]:
                return {
                    "success": False,
                    "error": f"Failed to get profile details: {profile_result['error']}",
                    "profile_data": None,
                    "activities_data": None,
                    "profile_url": profile_url
                }
            
            profile_data = profile_result["data"]
            entity_urn = profile_result["entity_urn"]
            
            # Step 2: Get profile activities if entity_urn is available
            activities_data = None
            if entity_urn:
                logger.info(f"Found entity URN: {entity_urn}, fetching activities...")
                activities_result = await self.get_profile_activities(entity_urn)
                
                if activities_result["success"]:
                    activities_data = activities_result["data"]
                else:
                    logger.warning(f"Failed to fetch activities: {activities_result['error']}")
                    # Don't fail the whole request if activities fail
            else:
                logger.info("No entity URN found, skipping activities fetch")
            
            logger.info(f"Full profile fetch completed for: {profile_url}")
            
            return {
                "success": True,
                "profile_data": profile_data,
                "activities_data": activities_data,
                "entity_urn": entity_urn,
                "profile_url": profile_url,
                "error": None
            }
            
        except Exception as e:
            logger.error(f"Error in get_full_profile for {profile_url}: {e}")
            return {
                "success": False,
                "error": str(e),
                "profile_data": None,
                "activities_data": None,
                "profile_url": profile_url
            }


# Create global instance
external_api_service = ExternalAPIService()