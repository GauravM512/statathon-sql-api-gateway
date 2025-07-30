#!/usr/bin/env python3
"""
Comprehensive example showing both SQL and RESTful approaches
"""

import requests
import json
from api_client import SurveyAPIClient

def print_json(data, title="Response"):
    """Pretty print JSON data"""
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=2))
    print("=" * (len(title) + 8))

def comprehensive_demo():
    """Demonstrate both SQL and RESTful approaches"""
    client = SurveyAPIClient("http://localhost:8000")
    
    print("🚀 Survey Data API Gateway - Comprehensive Demo")
    print("📊 Showing both SQL and RESTful approaches for the same data")
    
    # Approach 1: SQL Query
    print("\n" + "="*60)
    print("APPROACH 1: Using SQL Queries")
    print("="*60)
    
    sql_query = """
    SELECT 
        s.survey_name,
        COUNT(*) as response_count,
        AVG(CASE WHEN r.answer_numeric IS NOT NULL THEN r.answer_numeric END) as avg_rating
    FROM surveys s
    LEFT JOIN responses r ON s.survey_id = r.survey_id
    WHERE s.status = 'active'
    GROUP BY s.survey_id, s.survey_name
    ORDER BY response_count DESC
    """
    
    print(f"SQL Query:\n{sql_query}")
    
    # Analyze the query first
    analysis = client.analyze_query(sql_query)
    print(f"\n📈 Query Analysis:")
    print(f"- Tables used: {analysis.get('analysis', {}).get('tables', [])}")
    print(f"- Has joins: {analysis.get('analysis', {}).get('has_joins', False)}")
    print(f"- Has aggregations: {analysis.get('analysis', {}).get('has_aggregations', False)}")
    
    # Execute the query
    sql_result = client.execute_query(sql_query)
    print_json(sql_result, "SQL Query Result")
    
    # Approach 2: RESTful API
    print("\n" + "="*60)
    print("APPROACH 2: Using RESTful API")
    print("="*60)
    
    # Get the same data using RESTful endpoints
    print("RESTful Request: GET /api/surveys?status=active")
    restful_result = client.get_surveys(status="active")
    print_json(restful_result, "RESTful API Result")
    
    # Show filtering capabilities
    print("\n" + "="*60)
    print("DEMOGRAPHIC FILTERING EXAMPLES")
    print("="*60)
    
    # Get filter options
    print("Getting available filter options...")
    filter_options = client.get_filter_options()
    
    available_genders = filter_options.get('filter_options', {}).get('genders', [])
    available_age_groups = filter_options.get('filter_options', {}).get('age_groups', [])
    available_locations = filter_options.get('filter_options', {}).get('locations', [])
    
    print(f"Available Genders: {available_genders}")
    print(f"Available Age Groups: {available_age_groups}")
    print(f"Available Locations: {available_locations}")
    
    # Example: Get analytics summary with demographic filtering
    print(f"\n📊 Analytics Summary for Survey ID 1:")
    analytics = client.get_analytics_summary(survey_id=1)
    print_json(analytics, "Analytics Summary")
    
    print("\n" + "="*60)
    print("COMPARISON: SQL vs RESTful")
    print("="*60)
    
    print("✅ SQL Approach:")
    print("  • Full flexibility for complex queries")
    print("  • Query analysis and formatting")
    print("  • Security validation with sqlglot")
    print("  • Advanced joins and aggregations")
    
    print("\n✅ RESTful Approach:")
    print("  • Easy filtering with query parameters")
    print("  • Built-in pagination")
    print("  • Standardized response format")
    print("  • Perfect for web applications and mobile apps")
    print("  • Example: /api/responses?location=Maharashtra&gender=female&age_group=25-34")
    
    print("\n🎯 Use Cases:")
    print("• SQL: Complex analysis, data science, custom reports")
    print("• RESTful: Web apps, mobile apps, standard filtering, dashboards")
    
    print(f"\n🌐 Interactive Documentation: http://localhost:8000/docs")
    print(f"🔧 Alternative Documentation: http://localhost:8000/redoc")

if __name__ == "__main__":
    comprehensive_demo()
