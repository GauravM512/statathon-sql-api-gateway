from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import sqlite3
import pandas as pd
import json
import os
from contextlib import contextmanager
import sqlglot
from sqlglot import parse_one, ParseError
from sqlglot.expressions import Select, Table, Column, Join, Anonymous, Subquery
import numpy as np

def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization"""
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj

def safe_dataframe_to_dict(df):
    """Safely convert DataFrame to dict with proper type conversion"""
    # Replace numpy NaN with None and convert to native Python types
    df_clean = df.replace({np.nan: None})
    records = df_clean.to_dict('records')
    
    # Convert numpy types to native Python types
    clean_records = []
    for record in records:
        clean_record = {}
        for key, value in record.items():
            if isinstance(value, (np.bool_, bool)):
                clean_record[str(key)] = bool(value)
            elif isinstance(value, (np.integer, int)):
                clean_record[str(key)] = int(value)
            elif isinstance(value, (np.floating, float)):
                clean_record[str(key)] = float(value)
            elif value is None:
                clean_record[str(key)] = None
            else:
                clean_record[str(key)] = str(value) if value is not None else None
        clean_records.append(clean_record)
    
    return clean_records
from typing import List, Dict, Any, Optional
import sqlite3
import pandas as pd
import json
import os
from contextlib import contextmanager

app = FastAPI(
    title="Survey Data API Gateway",
    description="REST API Gateway to run SQL queries on survey datasets and retrieve results in JSON format",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class QueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    database: str = Field(default="survey.db", description="Database name to query")
    
class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    row_count: Optional[int] = None
    error: Optional[str] = None
    query: str
    analysis: Optional[Dict[str, Any]] = None

class DatabaseInfo(BaseModel):
    database: str
    tables: List[Dict[str, Any]]

class SurveyDataResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    total_count: int
    filtered_count: int
    filters_applied: Dict[str, Any]
    pagination: Dict[str, Any]

class DemographicFilters(BaseModel):
    age_group: Optional[str] = None
    gender: Optional[str] = None
    education_level: Optional[str] = None
    income_range: Optional[str] = None
    location: Optional[str] = None

# Database connection context manager
@contextmanager
def get_db_connection(db_name: str):
    """Context manager for database connections"""
    db_path = os.path.join("data", db_name)
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f"Database {db_name} not found")
    
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()

def analyze_query(query: str) -> Dict[str, Any]:
    """Analyze SQL query using sqlglot to extract metadata"""
    try:
        parsed = parse_one(query, dialect="sqlite")
        
        analysis = {
            "query_type": type(parsed).__name__,
            "tables": [],
            "columns": [],
            "has_joins": False,
            "has_aggregations": False,
            "has_subqueries": False
        }
        
        # Extract table names
        for table in parsed.find_all(Table):
            if table.name:
                analysis["tables"].append(table.name)
        
        # Extract column names
        for column in parsed.find_all(Column):
            if column.name:
                analysis["columns"].append(column.name)
        
        # Check for joins
        analysis["has_joins"] = len(list(parsed.find_all(Join))) > 0
        
        # Check for aggregations
        agg_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT']
        for func in parsed.find_all(Anonymous):
            if func.this.upper() in agg_functions:
                analysis["has_aggregations"] = True
                break
        
        # Check for subqueries
        analysis["has_subqueries"] = len(list(parsed.find_all(Subquery))) > 0
        
        return analysis
        
    except Exception as e:
        return {"error": f"Analysis failed: {str(e)}"}

def is_select_query(query: str) -> bool:
    """Check if the query is a SELECT statement (read-only) using sqlglot"""
    try:
        # Parse the SQL query using sqlglot
        parsed = parse_one(query, dialect="sqlite")
        
        # Check if the parsed query is a SELECT statement
        # sqlglot returns Select for SELECT queries, including CTEs (WITH clauses)
        return isinstance(parsed, Select)
        
    except ParseError as e:
        # If parsing fails, reject the query for security
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid SQL syntax: {str(e)}"
        )
    except Exception as e:
        # Any other error, reject for security
        raise HTTPException(
            status_code=400, 
            detail=f"Query validation error: {str(e)}"
        )

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Survey Data API Gateway",
        "version": "1.0.0",
        "description": "REST API Gateway with sqlglot-powered SQL parsing and RESTful data access",
        "sql_endpoints": {
            "/query": "POST - Execute SQL queries with analysis",
            "/analyze": "POST - Analyze SQL queries without execution",
            "/databases": "GET - List available databases",
            "/tables/{database}": "GET - List tables in a database",
            "/sample/{database}/{table}": "GET - Get sample data from a table",
            "/schema/{database}/{table}": "GET - Get table schema information"
        },
        "restful_api_endpoints": {
            "/api/surveys": "GET - Get surveys with filtering (status, date range)",
            "/api/responses": "GET - Get responses with demographic filtering",
            "/api/demographics": "GET - Get demographic data with filtering", 
            "/api/analytics/summary": "GET - Get analytics summary with filtering",
            "/api/filters/options": "GET - Get available filter options"
        },
        "example_usage": [
            "/api/responses?location=Maharashtra&gender=female&age_group=25-34",
            "/api/surveys?status=active&created_after=2024-01-01",
            "/api/demographics?gender=female&education_level=Bachelor's",
            "/api/analytics/summary?survey_id=1&location=New York"
        ],
        "features": [
            "SQL query parsing and validation with sqlglot",
            "RESTful API with query parameter filtering",
            "Demographic filtering (age, gender, location, education, income)",
            "Pagination support for large datasets",
            "Analytics and summary endpoints",
            "Query analysis (tables, columns, joins, aggregations)",
            "Query formatting and prettification",
            "Security: Only SELECT queries allowed",
            "JSON response format for all results"
        ]
    }

@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute SQL query and return results in JSON format"""
    try:
        # Analyze the query first
        query_analysis = analyze_query(request.query)
        
        # Security check - only allow SELECT queries
        if not is_select_query(request.query):
            return QueryResponse(
                success=False,
                error="Only SELECT queries are allowed for security reasons",
                query=request.query,
                analysis=query_analysis
            )
        
        with get_db_connection(request.database) as conn:
            # Execute query using pandas for better JSON serialization
            df = pd.read_sql_query(request.query, conn)
            
            # Convert DataFrame to JSON-serializable format with type safety
            data = safe_dataframe_to_dict(df)
            columns = df.columns.tolist()
            row_count = len(df)
            
            return QueryResponse(
                success=True,
                data=data, # type: ignore
                columns=columns,
                row_count=row_count,
                query=request.query,
                analysis=query_analysis
            )
            
    except sqlite3.Error as e:
        return QueryResponse(
            success=False,
            error=f"Database error: {str(e)}",
            query=request.query,
            analysis=analyze_query(request.query) if 'query_analysis' not in locals() else query_analysis
        )
    except Exception as e:
        return QueryResponse(
            success=False,
            error=f"Unexpected error: {str(e)}",
            query=request.query,
            analysis=analyze_query(request.query) if 'query_analysis' not in locals() else query_analysis
        )

@app.post("/analyze")
async def analyze_sql_query(request: QueryRequest):
    """Analyze SQL query without executing it"""
    try:
        analysis = analyze_query(request.query)
        is_valid_select = is_select_query(request.query)
        
        return {
            "query": request.query,
            "is_valid_select": is_valid_select,
            "analysis": analysis,
            "formatted_query": sqlglot.transpile(request.query, write="sqlite", pretty=True)[0] if analysis.get("error") is None else None
        }
        
    except Exception as e:
        return {
            "query": request.query,
            "is_valid_select": False,
            "error": str(e),
            "analysis": None,
            "formatted_query": None
        }

@app.get("/databases")
async def list_databases():
    """List all available databases"""
    data_dir = "data"
    if not os.path.exists(data_dir):
        return {"databases": []}
    
    databases = []
    for file in os.listdir(data_dir):
        if file.endswith('.db'):
            databases.append(file)
    
    return {"databases": databases}

@app.get("/tables/{database}")
async def list_tables(database: str):
    """List all tables in a specific database"""
    try:
        with get_db_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [{"name": row[0]} for row in cursor.fetchall()]
            
            # Get additional info for each table
            table_info = []
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table['name']});")
                columns = cursor.fetchall()
                cursor.execute(f"SELECT COUNT(*) FROM {table['name']};")
                row_count = cursor.fetchone()[0]
                
                table_info.append({
                    "name": table['name'],
                    "columns": [{"name": col[1], "type": col[2]} for col in columns],
                    "row_count": row_count
                })
            
            return DatabaseInfo(database=database, tables=table_info)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing database: {str(e)}")

@app.get("/sample/{database}/{table}")
async def get_sample_data(database: str, table: str, limit: int = 10):
    """Get sample data from a specific table"""
    try:
        query = f"SELECT * FROM {table} LIMIT {limit};"
        request = QueryRequest(query=query, database=database)
        return await execute_query(request)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching sample data: {str(e)}")

@app.get("/schema/{database}/{table}")
async def get_table_schema(database: str, table: str):
    """Get detailed schema information for a specific table"""
    try:
        with get_db_connection(database) as conn:
            cursor = conn.cursor()
            
            # Get table info
            cursor.execute(f"PRAGMA table_info({table});")
            columns = cursor.fetchall()
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table});")
            foreign_keys = cursor.fetchall()
            
            # Get indexes
            cursor.execute(f"PRAGMA index_list({table});")
            indexes = cursor.fetchall()
            
            schema_info = {
                "table": table,
                "columns": [
                    {
                        "cid": col[0],
                        "name": col[1],
                        "type": col[2],
                        "not_null": bool(col[3]),
                        "default_value": col[4],
                        "primary_key": bool(col[5])
                    } for col in columns
                ],
                "foreign_keys": [
                    {
                        "id": fk[0],
                        "seq": fk[1],
                        "table": fk[2],
                        "from": fk[3],
                        "to": fk[4]
                    } for fk in foreign_keys
                ],
                "indexes": [
                    {
                        "seq": idx[0],
                        "name": idx[1],
                        "unique": bool(idx[2])
                    } for idx in indexes
                ]
            }
            
            return schema_info
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching schema: {str(e)}")

# RESTful API endpoints for filtered data access
@app.get("/api/surveys", response_model=SurveyDataResponse)
async def get_surveys(
    status: Optional[str] = Query(None, description="Filter by survey status (active, completed)"),
    created_after: Optional[str] = Query(None, description="Filter surveys created after date (YYYY-MM-DD)"),
    created_before: Optional[str] = Query(None, description="Filter surveys created before date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    database: str = Query("survey.db", description="Database to query")
):
    """Get surveys with optional filtering"""
    try:
        with get_db_connection(database) as conn:
            # Build dynamic query
            where_conditions = []
            params = []
            
            if status:
                where_conditions.append("status = ?")
                params.append(status)
            
            if created_after:
                where_conditions.append("created_date >= ?")
                params.append(created_after)
                
            if created_before:
                where_conditions.append("created_date <= ?")
                params.append(created_before)
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM surveys{where_clause}"
            count_result = pd.read_sql_query(count_query, conn, params=params)
            total_count = int(count_result.iloc[0, 0])
            
            # Get filtered data
            data_query = f"SELECT * FROM surveys{where_clause} LIMIT {limit} OFFSET {offset}"
            df = pd.read_sql_query(data_query, conn, params=params)
            
            return SurveyDataResponse(
                success=True,
                data=safe_dataframe_to_dict(df),
                total_count=total_count,
                filtered_count=len(df),
                filters_applied={
                    "status": status,
                    "created_after": created_after,
                    "created_before": created_before
                },
                pagination={
                    "limit": limit,
                    "offset": offset,
                    "has_more": (offset + len(df)) < total_count
                }
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching surveys: {str(e)}")

@app.get("/api/responses", response_model=SurveyDataResponse)
async def get_responses(
    survey_id: Optional[int] = Query(None, description="Filter by survey ID"),
    survey_name: Optional[str] = Query(None, description="Filter by survey name"),
    age_group: Optional[str] = Query(None, description="Filter by age group (e.g., '25-34', '35-44')"),
    gender: Optional[str] = Query(None, description="Filter by gender"),
    location: Optional[str] = Query(None, description="Filter by location/state"),
    education_level: Optional[str] = Query(None, description="Filter by education level"),
    income_range: Optional[str] = Query(None, description="Filter by income range"),
    response_after: Optional[str] = Query(None, description="Filter responses after date (YYYY-MM-DD)"),
    response_before: Optional[str] = Query(None, description="Filter responses before date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    database: str = Query("survey.db", description="Database to query")
):
    """Get survey responses with comprehensive filtering including demographics"""
    try:
        with get_db_connection(database) as conn:
            # Build comprehensive join query
            base_query = """
            SELECT 
                r.response_id,
                r.survey_id,
                s.survey_name,
                r.question_id,
                q.question_text,
                q.question_type,
                r.respondent_id,
                r.answer_text,
                r.answer_numeric,
                r.response_date,
                d.age_group,
                d.gender,
                d.education_level,
                d.income_range,
                d.location
            FROM responses r
            JOIN surveys s ON r.survey_id = s.survey_id
            JOIN questions q ON r.question_id = q.question_id
            LEFT JOIN demographics d ON r.respondent_id = d.respondent_id
            """
            
            where_conditions = []
            params = []
            
            if survey_id:
                where_conditions.append("r.survey_id = ?")
                params.append(survey_id)
                
            if survey_name:
                where_conditions.append("s.survey_name LIKE ?")
                params.append(f"%{survey_name}%")
                
            if age_group:
                where_conditions.append("d.age_group = ?")
                params.append(age_group)
                
            if gender:
                where_conditions.append("d.gender = ?")
                params.append(gender)
                
            if location:
                where_conditions.append("d.location LIKE ?")
                params.append(f"%{location}%")
                
            if education_level:
                where_conditions.append("d.education_level = ?")
                params.append(education_level)
                
            if income_range:
                where_conditions.append("d.income_range = ?")
                params.append(income_range)
                
            if response_after:
                where_conditions.append("DATE(r.response_date) >= ?")
                params.append(response_after)
                
            if response_before:
                where_conditions.append("DATE(r.response_date) <= ?")
                params.append(response_before)
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM ({base_query}{where_clause}) as filtered_data"
            total_count = pd.read_sql_query(count_query, conn, params=params).iloc[0, 0]
            
            # Get filtered data with pagination
            full_query = f"{base_query}{where_clause} ORDER BY r.response_date DESC LIMIT {limit} OFFSET {offset}"
            df = pd.read_sql_query(full_query, conn, params=params)
            
            return SurveyDataResponse(
                success=True,
                data=df.to_dict('records'),
                total_count=total_count,
                filtered_count=len(df),
                filters_applied={
                    "survey_id": survey_id,
                    "survey_name": survey_name,
                    "age_group": age_group,
                    "gender": gender,
                    "location": location,
                    "education_level": education_level,
                    "income_range": income_range,
                    "response_after": response_after,
                    "response_before": response_before
                },
                pagination={
                    "limit": limit,
                    "offset": offset,
                    "has_more": (offset + len(df)) < total_count
                }
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching responses: {str(e)}")

@app.get("/api/demographics", response_model=SurveyDataResponse)
async def get_demographics(
    age_group: Optional[str] = Query(None, description="Filter by age group"),
    gender: Optional[str] = Query(None, description="Filter by gender"),
    education_level: Optional[str] = Query(None, description="Filter by education level"),
    income_range: Optional[str] = Query(None, description="Filter by income range"),
    location: Optional[str] = Query(None, description="Filter by location"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    database: str = Query("survey.db", description="Database to query")
):
    """Get demographic data with filtering"""
    try:
        with get_db_connection(database) as conn:
            where_conditions = []
            params = []
            
            if age_group:
                where_conditions.append("age_group = ?")
                params.append(age_group)
                
            if gender:
                where_conditions.append("gender = ?")
                params.append(gender)
                
            if education_level:
                where_conditions.append("education_level = ?")
                params.append(education_level)
                
            if income_range:
                where_conditions.append("income_range = ?")
                params.append(income_range)
                
            if location:
                where_conditions.append("location LIKE ?")
                params.append(f"%{location}%")
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM demographics{where_clause}"
            total_count = pd.read_sql_query(count_query, conn, params=params).iloc[0, 0]
            
            # Get filtered data
            data_query = f"SELECT * FROM demographics{where_clause} LIMIT {limit} OFFSET {offset}"
            df = pd.read_sql_query(data_query, conn, params=params)
            
            return SurveyDataResponse(
                success=True,
                data=df.to_dict('records'),
                total_count=total_count,
                filtered_count=len(df),
                filters_applied={
                    "age_group": age_group,
                    "gender": gender,
                    "education_level": education_level,
                    "income_range": income_range,
                    "location": location
                },
                pagination={
                    "limit": limit,
                    "offset": offset,
                    "has_more": (offset + len(df)) < total_count
                }
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching demographics: {str(e)}")

@app.get("/api/analytics/summary")
async def get_analytics_summary(
    survey_id: Optional[int] = Query(None, description="Filter by survey ID"),
    age_group: Optional[str] = Query(None, description="Filter by age group"),
    gender: Optional[str] = Query(None, description="Filter by gender"),
    location: Optional[str] = Query(None, description="Filter by location"),
    database: str = Query("survey.db", description="Database to query")
):
    """Get analytics summary with optional demographic filtering"""
    try:
        with get_db_connection(database) as conn:
            # Build base analytics query
            base_query = """
            SELECT 
                s.survey_name,
                s.survey_id,
                COUNT(DISTINCT r.respondent_id) as unique_respondents,
                COUNT(r.response_id) as total_responses,
                AVG(CASE WHEN r.answer_numeric IS NOT NULL THEN r.answer_numeric END) as avg_numeric_rating,
                COUNT(CASE WHEN r.answer_numeric IS NOT NULL THEN 1 END) as numeric_responses,
                d.age_group,
                d.gender,
                d.location
            FROM surveys s
            LEFT JOIN responses r ON s.survey_id = r.survey_id
            LEFT JOIN demographics d ON r.respondent_id = d.respondent_id
            """
            
            where_conditions = []
            params = []
            
            if survey_id:
                where_conditions.append("s.survey_id = ?")
                params.append(survey_id)
                
            if age_group:
                where_conditions.append("d.age_group = ?")
                params.append(age_group)
                
            if gender:
                where_conditions.append("d.gender = ?")
                params.append(gender)
                
            if location:
                where_conditions.append("d.location LIKE ?")
                params.append(f"%{location}%")
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            group_clause = " GROUP BY s.survey_id, s.survey_name, d.age_group, d.gender, d.location"
            
            full_query = f"{base_query}{where_clause}{group_clause}"
            df = pd.read_sql_query(full_query, conn, params=params)
            
            return {
                "success": True,
                "summary": df.to_dict('records'),
                "filters_applied": {
                    "survey_id": survey_id,
                    "age_group": age_group,
                    "gender": gender,
                    "location": location
                },
                "total_records": len(df)
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating analytics: {str(e)}")

@app.get("/api/filters/options")
async def get_filter_options(database: str = Query("survey.db", description="Database to query")):
    """Get available filter options for all filterable fields"""
    try:
        with get_db_connection(database) as conn:
            # Get unique values for each filterable field
            options = {}
            
            # Survey statuses
            survey_status_df = pd.read_sql_query("SELECT DISTINCT status FROM surveys WHERE status IS NOT NULL", conn)
            options["survey_status"] = survey_status_df['status'].tolist()
            
            # Age groups
            age_groups_df = pd.read_sql_query("SELECT DISTINCT age_group FROM demographics WHERE age_group IS NOT NULL", conn)
            options["age_groups"] = age_groups_df['age_group'].tolist()
            
            # Genders
            genders_df = pd.read_sql_query("SELECT DISTINCT gender FROM demographics WHERE gender IS NOT NULL", conn)
            options["genders"] = genders_df['gender'].tolist()
            
            # Education levels
            education_df = pd.read_sql_query("SELECT DISTINCT education_level FROM demographics WHERE education_level IS NOT NULL", conn)
            options["education_levels"] = education_df['education_level'].tolist()
            
            # Income ranges
            income_df = pd.read_sql_query("SELECT DISTINCT income_range FROM demographics WHERE income_range IS NOT NULL", conn)
            options["income_ranges"] = income_df['income_range'].tolist()
            
            # Locations
            locations_df = pd.read_sql_query("SELECT DISTINCT location FROM demographics WHERE location IS NOT NULL", conn)
            options["locations"] = locations_df['location'].tolist()
            
            # Survey names
            surveys_df = pd.read_sql_query("SELECT DISTINCT survey_name FROM surveys", conn)
            options["survey_names"] = surveys_df['survey_name'].tolist()
            
            return {
                "success": True,
                "filter_options": options,
                "description": "Available filter values for RESTful API endpoints"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching filter options: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
