# Survey Data API Gateway

A REST API Gateway built with FastAPI to run SQL queries on survey datasets and retrieve results in JSON format. Enhanced with **sqlglot** for advanced SQL parsing, analysis, and validation, plus comprehensive **RESTful API endpoints** for filtered data access.

## Features

- **Advanced SQL Parsing**: Uses sqlglot for robust SQL parsing and validation
- **RESTful API Layer**: Query parameter-based filtering for easy data access
- **Demographic Filtering**: Filter by age, gender, location, education, income
- **Query Analysis**: Automatic analysis of SQL queries (tables, columns, joins, aggregations)
- **Query Formatting**: Pretty-print and format SQL queries
- **Secure SQL Query Execution**: Execute SELECT queries on survey databases with enhanced security
- **JSON Response Format**: All results returned in user-friendly JSON format with query metadata
- **Pagination Support**: Built-in pagination for large datasets
- **Analytics Endpoints**: Summary statistics and analytics with filtering
- **Database Exploration**: List databases, tables, and schema information
- **Sample Data Access**: Quick access to sample data from any table
- **CORS Support**: Cross-origin requests enabled for web applications
- **Comprehensive Error Handling**: Detailed error messages with query analysis
- **Query Validation**: Only SELECT queries allowed with sqlglot-powered validation

## Quick Start

### 1. Setup Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Setup database and environment
python run.py
```

### 2. Start the Server

```bash
# Start the FastAPI server
python run.py start

# Or use uvicorn directly
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 3. Access the API

- **API Documentation**: http://localhost:8000/docs
- **Interactive API**: http://localhost:8000/redoc
- **Base URL**: http://localhost:8000

## API Endpoints

### Core Endpoints

### Core Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API information and available endpoints |
| POST | `/query` | Execute SQL queries with analysis |
| POST | `/analyze` | Analyze SQL queries without execution |
| GET | `/databases` | List available databases |
| GET | `/tables/{database}` | List tables in a database |
| GET | `/sample/{database}/{table}` | Get sample data from a table |
| GET | `/schema/{database}/{table}` | Get table schema information |

### RESTful API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/surveys` | Get surveys with filtering (status, date range) |
| GET | `/api/responses` | Get responses with demographic filtering |
| GET | `/api/demographics` | Get demographic data with filtering |
| GET | `/api/analytics/summary` | Get analytics summary with filtering |
| GET | `/api/filters/options` | Get available filter options for all fields |

### RESTful API Query Parameters

**Surveys Endpoint (`/api/surveys`)**:
- `status`: Filter by survey status (active, completed)
- `created_after`: Filter surveys created after date (YYYY-MM-DD)
- `created_before`: Filter surveys created before date (YYYY-MM-DD)
- `limit`: Number of records to return (default: 100, max: 1000)
- `offset`: Number of records to skip (default: 0)

**Responses Endpoint (`/api/responses`)**:
- `survey_id`: Filter by survey ID
- `survey_name`: Filter by survey name (partial match)
- `age_group`: Filter by age group (e.g., '25-34', '35-44')
- `gender`: Filter by gender
- `location`: Filter by location/state (partial match)
- `education_level`: Filter by education level
- `income_range`: Filter by income range
- `response_after`: Filter responses after date (YYYY-MM-DD)
- `response_before`: Filter responses before date (YYYY-MM-DD)
- `limit`: Number of records to return (default: 100, max: 1000)
- `offset`: Number of records to skip (default: 0)

**Demographics Endpoint (`/api/demographics`)**:
- `age_group`: Filter by age group
- `gender`: Filter by gender
- `education_level`: Filter by education level
- `income_range`: Filter by income range
- `location`: Filter by location (partial match)
- `limit`: Number of records to return (default: 100, max: 1000)
- `offset`: Number of records to skip (default: 0)

**Analytics Summary Endpoint (`/api/analytics/summary`)**:
- `survey_id`: Filter by survey ID
- `age_group`: Filter by age group
- `gender`: Filter by gender
- `location`: Filter by location (partial match)

### Example Requests

### Example Requests

#### Get Filter Options (NEW!)

```bash
curl "http://localhost:8000/api/filters/options"
```

#### Get Active Surveys (NEW!)

```bash
curl "http://localhost:8000/api/surveys?status=active&limit=10"
```

#### Get Filtered Survey Responses (Like PLFS Data) (NEW!)

```bash
# Similar to: api/plfs/data?state=Maharashtra&gender=female&age=15-29
curl "http://localhost:8000/api/responses?location=Maharashtra&gender=Female&age_group=25-34&limit=50"
```

#### Get Demographics with Filtering (NEW!)

```bash
curl "http://localhost:8000/api/demographics?gender=Female&education_level=Bachelor's"
```

#### Get Analytics Summary (NEW!)

```bash
curl "http://localhost:8000/api/analytics/summary?survey_id=1&gender=Female"
```

#### Analyze a Query

```bash
curl -X POST "http://localhost:8000/analyze" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "SELECT s.survey_name, AVG(r.answer_numeric) FROM surveys s JOIN responses r ON s.survey_id = r.survey_id GROUP BY s.survey_name",
       "database": "survey.db"
     }'
```

#### Execute a Query

```bash
curl -X POST "http://localhost:8000/query" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "SELECT * FROM surveys",
       "database": "survey.db"
     }'
```

#### Get Sample Data

```bash
curl "http://localhost:8000/sample/survey.db/surveys?limit=5"
```

#### List Tables

```bash
curl "http://localhost:8000/tables/survey.db"
```

## Database Schema

The sample database includes the following tables:

### surveys
- `survey_id`: Primary key
- `survey_name`: Name of the survey
- `description`: Survey description
- `created_date`: Creation date
- `status`: Survey status (active/completed)

### questions
- `question_id`: Primary key
- `survey_id`: Foreign key to surveys
- `question_text`: The question text
- `question_type`: Type of question (rating, text, yes_no, etc.)
- `required`: Whether the question is required

### responses
- `response_id`: Primary key
- `survey_id`: Foreign key to surveys
- `question_id`: Foreign key to questions
- `respondent_id`: Identifier for the respondent
- `answer_text`: Text answer
- `answer_numeric`: Numeric answer (for ratings)
- `response_date`: When the response was submitted

### demographics
- `respondent_id`: Primary key
- `age_group`: Age group of respondent
- `gender`: Gender
- `education_level`: Education level
- `income_range`: Income range
- `location`: Geographic location

## Example Queries

### Customer Satisfaction Analysis

```sql
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
ORDER BY average_rating DESC;
```

### Demographics Summary

```sql
SELECT 
    age_group,
    gender,
    COUNT(*) as count
FROM demographics
GROUP BY age_group, gender
ORDER BY count DESC;
```

### Response Trends

```sql
SELECT 
    DATE(r.response_date) as response_date,
    COUNT(*) as daily_responses
FROM responses r
GROUP BY DATE(r.response_date)
ORDER BY response_date;
```

## Python Client

Use the included Python client to interact with the API:

```python
from api_client import SurveyAPIClient

client = SurveyAPIClient()

# Get available filter options
filter_options = client.get_filter_options()
print("Available Filters:", filter_options)

# Get surveys with filtering
active_surveys = client.get_surveys(status="active", limit=10)
print("Active Surveys:", active_surveys)

# Get responses with demographic filtering (like PLFS example)
filtered_responses = client.get_responses(
    gender="Female", 
    age_group="25-34", 
    location="New York",
    limit=20
)
print("Filtered Responses:", filtered_responses)

# Get analytics summary
analytics = client.get_analytics_summary(survey_id=1, gender="Female")
print("Analytics Summary:", analytics)

# Execute a query
result = client.execute_query("SELECT * FROM surveys")
print("Query Results:", result)

# Analyze a query before execution
analysis = client.analyze_query("SELECT s.survey_name, AVG(r.answer_numeric) FROM surveys s JOIN responses r ON s.survey_id = r.survey_id GROUP BY s.survey_name")
print("Query Analysis:", analysis)

# Get sample data
sample = client.get_sample_data("survey.db", "surveys", limit=5)
print(sample)

# List databases
databases = client.list_databases()
print(databases)
```

## Response Format

### Query Execution Response

All query execution responses include analysis metadata:

```json
{
  "success": true,
  "data": [
    {
      "survey_id": 1,
      "survey_name": "Customer Satisfaction Survey",
      "description": "Annual customer satisfaction survey",
      "created_date": "2024-01-15",
      "status": "active"
    }
  ],
  "columns": ["survey_id", "survey_name", "description", "created_date", "status"],
  "row_count": 1,
  "query": "SELECT * FROM surveys WHERE survey_id = 1",
  "analysis": {
    "query_type": "Select",
    "tables": ["surveys"],
    "columns": ["survey_id", "survey_name", "description", "created_date", "status"],
    "has_joins": false,
    "has_aggregations": false,
    "has_subqueries": false
  }
}
```

### Query Analysis Response

The `/analyze` endpoint returns detailed query information:

```json
{
  "query": "SELECT s.survey_name, AVG(r.answer_numeric) FROM surveys s JOIN responses r ON s.survey_id = r.survey_id GROUP BY s.survey_name",
  "is_valid_select": true,
  "analysis": {
    "query_type": "Select",
    "tables": ["surveys", "responses"],
    "columns": ["survey_name", "answer_numeric", "survey_id"],
    "has_joins": true,
    "has_aggregations": true,
    "has_subqueries": false
  },
  "formatted_query": "SELECT\n  s.survey_name,\n  AVG(r.answer_numeric)\nFROM surveys AS s\nJOIN responses AS r\n  ON s.survey_id = r.survey_id\nGROUP BY\n  s.survey_name"
}
```

## Error Handling

Errors are returned in a consistent format:

```json
{
  "success": false,
  "error": "Database error: no such table: invalid_table",
  "query": "SELECT * FROM invalid_table"
}
```

## Security Features

- **Advanced Query Validation**: Uses sqlglot to parse and validate SQL queries
- **Query Type Detection**: Automatically detects and allows only SELECT statements
- **SQL Injection Prevention**: Robust parsing prevents malicious SQL injection
- **Database Validation**: Validates database existence before querying
- **Error Sanitization**: Sanitized error messages to prevent information leakage
- **CORS Configuration**: Configurable CORS settings for web security
- **Query Analysis**: Provides detailed analysis of query structure and complexity

## Development

### Project Structure

```
statathon/
├── main.py              # FastAPI application
├── setup_database.py    # Database setup script
├── api_client.py        # Python client for API
├── run.py              # Startup script
├── requirements.txt     # Python dependencies
├── README.md           # This file
└── data/               # Database files
    └── survey.db       # Sample survey database
```

### Adding New Databases

1. Place your SQLite database files in the `data/` directory
2. The API will automatically detect and make them available
3. Use the `/databases` endpoint to verify they're available

### Extending the API

The FastAPI application is modular and can be easily extended with:

- Additional endpoints for specific analysis
- Authentication and authorization
- Rate limiting
- Caching
- Additional database backends (PostgreSQL, MySQL, etc.)

## License

This project is open source and available under the MIT License.
