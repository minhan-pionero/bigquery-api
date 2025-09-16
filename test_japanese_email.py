"""
Test script for Japanese email template
"""

import requests
import json

# Test data for Japanese email template
test_data = {
    "platform": "LinkedIn",
    "method": "クローラー", 
    "error": "接続がタイムアウトしました。ネットワーク設定を確認してください。"
}

test_data_facebook = {
    "platform": "Facebook",
    "method": "友達取得",
    "error": "友達データの取得に失敗しました。APIキーを確認してください。"
}

def test_japanese_email_api():
    """Test the Japanese email template"""
    url = "http://localhost:8000/email/error-report"
    
    print("Testing LinkedIn クローラー error...")
    try:
        response = requests.post(url, json=test_data, timeout=30)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Error testing LinkedIn: {e}")
    
    print("\n" + "="*50 + "\n")
    
    print("Testing Facebook 友達取得 error...")
    try:
        response = requests.post(url, json=test_data_facebook, timeout=30)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Error testing Facebook: {e}")

if __name__ == "__main__":
    test_japanese_email_api()