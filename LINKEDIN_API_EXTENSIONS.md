# LinkedIn API Extensions Documentation

This document describes the new secure API endpoints for SERP API and ProAPIs integration.

## Overview

Instead of calling external APIs directly from the extension, these endpoints provide a secure server-side proxy that handles all external API calls with proper authentication and error handling.

## Environment Variables Required

Before using these endpoints, set the following environment variables:

```bash
export SERPAPI_KEY="your_serpapi_key_here"
export PROAPIS_KEY="your_proapis_key_here"
```

## New Endpoints

### 1. SERP API Search

**Endpoint:** `GET /linkedin/serp`

**Description:** Search LinkedIn profiles using SERP API

**Parameters:**
- `keyword` (required): Search keyword
- `start` (optional, default=0): Start index for pagination

**Example Request:**
```bash
curl "http://localhost:8000/linkedin/serp?keyword=machine%20learning%20engineer%20site:linkedin.com&start=0"
```

**Example Response:**
```json
{
  "status": "success",
  "keyword": "machine learning engineer site:linkedin.com",
  "start": 0,
  "profiles": [
    {
      "url": "https://www.linkedin.com/in/john-doe",
      "title": "Machine Learning Engineer at Tech Corp",
      "snippet": "Experienced ML engineer with expertise in..."
    }
  ],
  "total_found": 10
}
```

### 2. Profile Details

**Endpoint:** `GET /linkedin/profile/details`

**Description:** Get detailed profile information from ProAPIs

**Parameters:**
- `profile_url` (required): LinkedIn profile URL

**Example Request:**
```bash
curl "http://localhost:8000/linkedin/profile/details?profile_url=https://www.linkedin.com/in/john-doe"
```

**Example Response:**
```json
{
  "status": "success",
  "profile_url": "https://www.linkedin.com/in/john-doe",
  "entity_urn": "urn:li:fsd_profile:ACoAAABCDEF",
  "data": {
    "firstName": "John",
    "lastName": "Doe",
    "headline": "Machine Learning Engineer",
    "summary": "...",
    "experience": [...],
    "education": [...]
  }
}
```

### 3. Profile Activities

**Endpoint:** `GET /linkedin/profile/activities`

**Description:** Get profile activities/posts from ProAPIs

**Parameters:**
- `entity_urn` (required): LinkedIn profile entity URN

**Example Request:**
```bash
curl "http://localhost:8000/linkedin/profile/activities?entity_urn=urn:li:fsd_profile:ACoAAABCDEF"
```

**Example Response:**
```json
{
  "status": "success",
  "entity_urn": "urn:li:fsd_profile:ACoAAABCDEF",
  "data": {
    "activities": [
      {
        "activityType": "POST",
        "text": "Excited to share...",
        "publishedAt": "2024-01-15T10:00:00Z"
      }
    ]
  }
}
```

### 4. Full Profile

**Endpoint:** `GET /linkedin/profile/full`

**Description:** Get complete profile data (details + activities) in one call

**Parameters:**
- `profile_url` (required): LinkedIn profile URL

**Example Request:**
```bash
curl "http://localhost:8000/linkedin/profile/full?profile_url=https://www.linkedin.com/in/john-doe"
```

**Example Response:**
```json
{
  "status": "success",
  "profile_url": "https://www.linkedin.com/in/john-doe",
  "entity_urn": "urn:li:fsd_profile:ACoAAABCDEF",
  "profile_data": {
    "firstName": "John",
    "lastName": "Doe",
    "headline": "Machine Learning Engineer",
    "experience": [...],
    "education": [...]
  },
  "activities_data": {
    "activities": [...]
  }
}
```

## Migration Guide for Extension

### Before (Direct API Calls)
```javascript
// Extension directly calling external APIs
const serpResults = await getSerpApiResults(keyword, start);
const profileDetails = await getProfileDetails(profileUrl);
const activities = await getProfileActivities(entityUrn);
```

### After (Secure API Calls)
```javascript
// Extension calling secure API endpoints
const serpResults = await fetch(`${API_BASE}/linkedin/serp?keyword=${keyword}&start=${start}`);
const profileDetails = await fetch(`${API_BASE}/linkedin/profile/details?profile_url=${profileUrl}`);
const activities = await fetch(`${API_BASE}/linkedin/profile/activities?entity_urn=${entityUrn}`);

// Or get everything in one call
const fullProfile = await fetch(`${API_BASE}/linkedin/profile/full?profile_url=${profileUrl}`);
```

## Error Handling

All endpoints return standardized error responses:

```json
{
  "detail": "Error message here"
}
```

Common error scenarios:
- Missing API keys (500 - Internal Server Error)
- Invalid parameters (400 - Bad Request)
- External API failures (500 - Internal Server Error)
- Network timeouts (500 - Internal Server Error)

## Security Benefits

1. **API Key Protection**: API keys are stored securely on the server, not exposed in the extension
2. **Rate Limiting**: Server can implement rate limiting and caching
3. **Error Handling**: Centralized error handling and logging
4. **Monitoring**: Server-side logging for debugging and monitoring
5. **CORS Control**: Proper CORS configuration for web security

## Testing

Use the provided test script:

```bash
python test_linkedin_endpoints.py
```

Make sure to set environment variables before testing with real API keys.