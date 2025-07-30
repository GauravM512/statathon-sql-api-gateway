from fastapi import FastAPI, HTTPException, Depends
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
        "description": "REST API Gateway with sqlglot-powered SQL parsing and analysis",
        "endpoints": {
            "/query": "POST - Execute SQL queries with analysis",
            "/analyze": "POST - Analyze SQL queries without execution",
            "/databases": "GET - List available databases",
            "/tables/{database}": "GET - List tables in a database",
            "/sample/{database}/{table}": "GET - Get sample data from a table",
            "/schema/{database}/{table}": "GET - Get table schema information"
        },
        "features": [
            "SQL query parsing and validation with sqlglot",
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
            
            # Convert DataFrame to JSON-serializable format
            data = df.to_dict('records')
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
