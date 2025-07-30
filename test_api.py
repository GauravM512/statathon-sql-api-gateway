#!/usr/bin/env python3
"""
Test script to demonstrate the Survey Data API Gateway with sqlglot integration
"""

import requests
import json
from api_client import SurveyAPIClient

def print_json(data, title="Response"):
    """Pretty print JSON data"""
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=2))
    print("=" * (len(title) + 8))

def test_api():
    """Test the API Gateway functionality"""
    client = SurveyAPIClient("http://localhost:8000")
    
    print("🚀 Testing Survey Data API Gateway with sqlglot")
    
    # Test 1: Root endpoint
    print("\n1. Testing root endpoint...")
    try:
        response = requests.get("http://localhost:8000/")
        print_json(response.json(), "API Information")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: Query analysis (new feature)
    print("\n2. Testing query analysis...")
    test_query = """
    SELECT 
        s.survey_name,
        q.question_text,
        AVG(r.answer_numeric) as avg_rating,
        COUNT(*) as response_count
    FROM surveys s
    JOIN questions q ON s.survey_id = q.survey_id
    JOIN responses r ON q.question_id = r.question_id
    WHERE r.answer_numeric IS NOT NULL
    GROUP BY s.survey_id, q.question_id
    ORDER BY avg_rating DESC
    """
    
    analysis = client.analyze_query(test_query)
    print_json(analysis, "Query Analysis")
    
    # Test 3: Query execution with analysis
    print("\n3. Testing query execution...")
    simple_query = "SELECT * FROM surveys"
    result = client.execute_query(simple_query)
    print_json(result, "Query Execution Result")
    
    # Test 4: Security test (should fail)
    print("\n4. Testing security (should reject non-SELECT)...")
    malicious_query = "UPDATE surveys SET status = 'deleted'"
    security_result = client.execute_query(malicious_query)
    print_json(security_result, "Security Test Result")
    
    # Test 5: Database exploration
    print("\n5. Testing database exploration...")
    databases = client.list_databases()
    print_json(databases, "Available Databases")
    
    tables = client.list_tables("survey.db")
    print_json(tables, "Database Tables")
    
    # Test 6: Sample data
    print("\n6. Testing sample data retrieval...")
    sample = client.get_sample_data("survey.db", "surveys", 3)
    print_json(sample, "Sample Survey Data")
    
    # Test 7: Complex query with joins
    print("\n7. Testing complex join query...")
    join_query = """
    SELECT 
        s.survey_name,
        d.age_group,
        d.gender,
        r.answer_text,
        r.answer_numeric
    FROM surveys s
    JOIN responses r ON s.survey_id = r.survey_id
    LEFT JOIN demographics d ON r.respondent_id = d.respondent_id
    WHERE s.survey_name = 'Customer Satisfaction Survey'
    LIMIT 5
    """
    
    join_result = client.execute_query(join_query)
    print_json(join_result, "Complex Join Query Result")
    
    print("\n✅ All API tests completed successfully!")
    print("\n🔗 Access the interactive API documentation at: http://localhost:8000/docs")
    print("🔗 Access the alternative docs at: http://localhost:8000/redoc")

if __name__ == "__main__":
    test_api()
