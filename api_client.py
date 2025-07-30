import requests
import json

class SurveyAPIClient:
    """Client class to interact with the Survey Data API Gateway"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
    
    def execute_query(self, query: str, database: str = "survey.db"):
        """Execute a SQL query and return results"""
        response = requests.post(
            f"{self.base_url}/query",
            json={"query": query, "database": database}
        )
        return response.json()
    
    def analyze_query(self, query: str, database: str = "survey.db"):
        """Analyze a SQL query without executing it"""
        response = requests.post(
            f"{self.base_url}/analyze",
            json={"query": query, "database": database}
        )
        return response.json()
    
    def list_databases(self):
        """List all available databases"""
        response = requests.get(f"{self.base_url}/databases")
        return response.json()
    
    def list_tables(self, database: str):
        """List all tables in a database"""
        response = requests.get(f"{self.base_url}/tables/{database}")
        return response.json()
    
    def get_sample_data(self, database: str, table: str, limit: int = 10):
        """Get sample data from a table"""
        response = requests.get(f"{self.base_url}/sample/{database}/{table}?limit={limit}")
        return response.json()
    
    def get_table_schema(self, database: str, table: str):
        """Get schema information for a table"""
        response = requests.get(f"{self.base_url}/schema/{database}/{table}")
        return response.json()
    
    # RESTful API methods
    def get_surveys(self, status=None, created_after=None, created_before=None, limit=100, offset=0):
        """Get surveys with optional filtering"""
        params = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if created_after:
            params["created_after"] = created_after
        if created_before:
            params["created_before"] = created_before
            
        response = requests.get(f"{self.base_url}/api/surveys", params=params)
        return response.json()
    
    def get_responses(self, survey_id=None, survey_name=None, age_group=None, gender=None, 
                     location=None, education_level=None, income_range=None, 
                     response_after=None, response_before=None, limit=100, offset=0):
        """Get responses with comprehensive filtering"""
        params = {"limit": limit, "offset": offset}
        if survey_id:
            params["survey_id"] = survey_id
        if survey_name:
            params["survey_name"] = survey_name
        if age_group:
            params["age_group"] = age_group
        if gender:
            params["gender"] = gender
        if location:
            params["location"] = location
        if education_level:
            params["education_level"] = education_level
        if income_range:
            params["income_range"] = income_range
        if response_after:
            params["response_after"] = response_after
        if response_before:
            params["response_before"] = response_before
            
        response = requests.get(f"{self.base_url}/api/responses", params=params)
        return response.json()
    
    def get_demographics(self, age_group=None, gender=None, education_level=None, 
                        income_range=None, location=None, limit=100, offset=0):
        """Get demographics with filtering"""
        params = {"limit": limit, "offset": offset}
        if age_group:
            params["age_group"] = age_group
        if gender:
            params["gender"] = gender
        if education_level:
            params["education_level"] = education_level
        if income_range:
            params["income_range"] = income_range
        if location:
            params["location"] = location
            
        response = requests.get(f"{self.base_url}/api/demographics", params=params)
        return response.json()
    
    def get_analytics_summary(self, survey_id=None, age_group=None, gender=None, location=None):
        """Get analytics summary with filtering"""
        params = {}
        if survey_id:
            params["survey_id"] = survey_id
        if age_group:
            params["age_group"] = age_group
        if gender:
            params["gender"] = gender
        if location:
            params["location"] = location
            
        response = requests.get(f"{self.base_url}/api/analytics/summary", params=params)
        return response.json()
    
    def get_filter_options(self):
        """Get available filter options"""
        response = requests.get(f"{self.base_url}/api/filters/options")
        return response.json()

# Example usage and common queries
def example_queries():
    """Example queries for the survey database"""
    
    queries = {
        "all_surveys": "SELECT * FROM surveys",
        
        "customer_satisfaction_responses": """
            SELECT 
                s.survey_name,
                q.question_text,
                r.answer_text,
                r.answer_numeric,
                d.age_group,
                d.gender,
                d.location
            FROM responses r
            JOIN surveys s ON r.survey_id = s.survey_id
            JOIN questions q ON r.question_id = q.question_id
            LEFT JOIN demographics d ON r.respondent_id = d.respondent_id
            WHERE s.survey_name = 'Customer Satisfaction Survey'
        """,
        
        "average_ratings": """
            SELECT 
                s.survey_name,
                q.question_text,
                AVG(r.answer_numeric) as average_rating,
                COUNT(*) as response_count
            FROM responses r
            JOIN surveys s ON r.survey_id = s.survey_id
            JOIN questions q ON r.question_id = q.question_id
            WHERE r.answer_numeric IS NOT NULL
            GROUP BY s.survey_id, q.question_id
            ORDER BY average_rating DESC
        """,
        
        "demographics_summary": """
            SELECT 
                age_group,
                gender,
                COUNT(*) as count
            FROM demographics
            GROUP BY age_group, gender
            ORDER BY count DESC
        """,
        
        "response_trends": """
            SELECT 
                DATE(r.response_date) as response_date,
                COUNT(*) as daily_responses
            FROM responses r
            GROUP BY DATE(r.response_date)
            ORDER BY response_date
        """
    }
    
    return queries

if __name__ == "__main__":
    # Example usage
    client = SurveyAPIClient()
    
    print("Available example queries:")
    queries = example_queries()
    for name, query in queries.items():
        print(f"\n{name}:")
        print(query)
        
        # Demonstrate query analysis
        print(f"\nAnalyzing query: {name}")
        try:
            analysis = client.analyze_query(query)
            if analysis.get('is_valid_select'):
                print("✓ Valid SELECT query")
                print(f"Tables: {analysis['analysis'].get('tables', [])}")
                print(f"Has joins: {analysis['analysis'].get('has_joins', False)}")
                print(f"Has aggregations: {analysis['analysis'].get('has_aggregations', False)}")
            else:
                print("✗ Invalid or non-SELECT query")
        except Exception as e:
            print(f"Error analyzing query: {e}")
        print("-" * 50)
