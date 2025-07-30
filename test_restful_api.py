#!/usr/bin/env python3
"""
Test script for RESTful API endpoints
"""

import requests
import json
from api_client import SurveyAPIClient

def print_json(data, title="Response"):
    """Pretty print JSON data"""
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=2))
    print("=" * (len(title) + 8))

def test_restful_api():
    """Test the RESTful API endpoints"""
    base_url = "http://localhost:8000"
    
    print("🚀 Testing RESTful API Endpoints")
    
    # Test 1: Get all surveys
    print("\n1. Testing /api/surveys endpoint...")
    response = requests.get(f"{base_url}/api/surveys")
    if response.status_code == 200:
        print_json(response.json(), "All Surveys")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Test 2: Get surveys with status filter
    print("\n2. Testing /api/surveys with status filter...")
    response = requests.get(f"{base_url}/api/surveys?status=active")
    if response.status_code == 200:
        print_json(response.json(), "Active Surveys")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Test 3: Get responses (similar to PLFS example)
    print("\n3. Testing /api/responses (like PLFS data)...")
    response = requests.get(f"{base_url}/api/responses?gender=Female&age_group=25-34&location=New York")
    if response.status_code == 200:
        data = response.json()
        print(f"Found {data.get('filtered_count', 0)} responses matching filters:")
        print(f"- Gender: Female")
        print(f"- Age Group: 25-34") 
        print(f"- Location: New York")
        print_json(data, "Filtered Responses")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Test 4: Get demographics with multiple filters
    print("\n4. Testing /api/demographics with filters...")
    response = requests.get(f"{base_url}/api/demographics?gender=Female&education_level=Bachelor's")
    if response.status_code == 200:
        print_json(response.json(), "Demographics - Female with Bachelor's")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Test 5: Get analytics summary
    print("\n5. Testing /api/analytics/summary...")
    response = requests.get(f"{base_url}/api/analytics/summary?survey_id=1")
    if response.status_code == 200:
        print_json(response.json(), "Analytics Summary")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Test 6: Get filter options
    print("\n6. Testing /api/filters/options...")
    response = requests.get(f"{base_url}/api/filters/options")
    if response.status_code == 200:
        print_json(response.json(), "Available Filter Options")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Test 7: Pagination example
    print("\n7. Testing pagination...")
    response = requests.get(f"{base_url}/api/responses?limit=3&offset=0")
    if response.status_code == 200:
        data = response.json()
        print(f"Showing {data.get('filtered_count', 0)} of {data.get('total_count', 0)} total responses")
        print(f"Pagination: limit=3, offset=0, has_more={data.get('pagination', {}).get('has_more', False)}")
        print_json(data, "Paginated Responses")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    print("\n✅ RESTful API tests completed!")
    print("\n📚 Example URLs you can try:")
    print("• GET /api/responses?location=Maharashtra&gender=female&age_group=25-34")
    print("• GET /api/surveys?status=active&created_after=2024-01-01")
    print("• GET /api/demographics?gender=female&education_level=Bachelor's")
    print("• GET /api/analytics/summary?survey_id=1&location=New York")

if __name__ == "__main__":
    test_restful_api()
