"""
Test script for the new LinkedIn API endpoints
"""

import asyncio
import httpx

# Test configuration
BASE_URL = "http://localhost:8000"

async def test_serp_api():
    """Test SERP API endpoint"""
    print("Testing SERP API endpoint...")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BASE_URL}/linkedin/serp",
                params={
                    "keyword": "machine learning engineer site:linkedin.com",
                    "start": 0
                }
            )
            
            print(f"SERP API Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Found {data['total_found']} profiles")
                print(f"First profile URL: {data['profiles'][0]['url'] if data['profiles'] else 'None'}")
            else:
                print(f"Error: {response.text}")
                
        except Exception as e:
            print(f"SERP API Error: {e}")

async def test_profile_details():
    """Test ProAPIs profile details endpoint"""
    print("\nTesting ProAPIs profile details endpoint...")
    
    test_url = "https://www.linkedin.com/in/example-profile"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BASE_URL}/linkedin/profile/details",
                params={"profile_url": test_url}
            )
            
            print(f"Profile Details Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Entity URN: {data.get('entity_urn', 'None')}")
                print(f"Profile URL: {data.get('profile_url')}")
            else:
                print(f"Error: {response.text}")
                
        except Exception as e:
            print(f"Profile Details Error: {e}")

async def test_profile_activities():
    """Test ProAPIs profile activities endpoint"""
    print("\nTesting ProAPIs profile activities endpoint...")
    
    test_urn = "example-entity-urn"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BASE_URL}/linkedin/profile/activities",
                params={"entity_urn": test_urn}
            )
            
            print(f"Profile Activities Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Entity URN: {data.get('entity_urn')}")
                print(f"Activities data available: {bool(data.get('data'))}")
            else:
                print(f"Error: {response.text}")
                
        except Exception as e:
            print(f"Profile Activities Error: {e}")

async def test_full_profile():
    """Test ProAPIs full profile endpoint"""
    print("\nTesting ProAPIs full profile endpoint...")
    
    test_url = "https://www.linkedin.com/in/example-profile"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BASE_URL}/linkedin/profile/full",
                params={"profile_url": test_url}
            )
            
            print(f"Full Profile Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Entity URN: {data.get('entity_urn', 'None')}")
                print(f"Profile data available: {bool(data.get('profile_data'))}")
                print(f"Activities data available: {bool(data.get('activities_data'))}")
            else:
                print(f"Error: {response.text}")
                
        except Exception as e:
            print(f"Full Profile Error: {e}")

async def main():
    """Run all tests"""
    print("=== LinkedIn API Endpoints Test ===")
    print("Note: Set SERPAPI_KEY and PROAPIS_KEY environment variables for actual testing")
    print()
    
    await test_serp_api()
    await test_profile_details()
    await test_profile_activities()
    await test_full_profile()
    
    print("\n=== Test completed ===")

if __name__ == "__main__":
    asyncio.run(main())