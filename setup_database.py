import sqlite3
import pandas as pd
import os
from typing import Dict, Any

def create_sample_survey_database():
    """Create a sample survey database with mock data"""
    
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Connect to SQLite database
    conn = sqlite3.connect("data/survey.db")
    cursor = conn.cursor()
    
    # Create surveys table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS surveys (
            survey_id INTEGER PRIMARY KEY AUTOINCREMENT,
            survey_name TEXT NOT NULL,
            description TEXT,
            created_date DATE,
            status TEXT DEFAULT 'active'
        )
    """)
    
    # Create questions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            question_id INTEGER PRIMARY KEY AUTOINCREMENT,
            survey_id INTEGER,
            question_text TEXT NOT NULL,
            question_type TEXT NOT NULL,
            required BOOLEAN DEFAULT 0,
            FOREIGN KEY (survey_id) REFERENCES surveys (survey_id)
        )
    """)
    
    # Create responses table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS responses (
            response_id INTEGER PRIMARY KEY AUTOINCREMENT,
            survey_id INTEGER,
            question_id INTEGER,
            respondent_id TEXT,
            answer_text TEXT,
            answer_numeric REAL,
            response_date DATETIME,
            FOREIGN KEY (survey_id) REFERENCES surveys (survey_id),
            FOREIGN KEY (question_id) REFERENCES questions (question_id)
        )
    """)
    
    # Create demographics table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demographics (
            respondent_id TEXT PRIMARY KEY,
            age_group TEXT,
            gender TEXT,
            education_level TEXT,
            income_range TEXT,
            location TEXT
        )
    """)
    
    # Insert sample survey data
    sample_surveys = [
        (1, "Customer Satisfaction Survey", "Annual customer satisfaction survey", "2024-01-15", "active"),
        (2, "Employee Engagement Survey", "Quarterly employee engagement survey", "2024-03-01", "active"),
        (3, "Product Feedback Survey", "Product feedback and improvement survey", "2024-02-10", "completed")
    ]
    
    cursor.executemany("""
        INSERT OR REPLACE INTO surveys (survey_id, survey_name, description, created_date, status)
        VALUES (?, ?, ?, ?, ?)
    """, sample_surveys)
    
    # Insert sample questions
    sample_questions = [
        (1, 1, "How satisfied are you with our service?", "rating", 1),
        (2, 1, "What can we improve?", "text", 0),
        (3, 1, "Would you recommend us to others?", "yes_no", 1),
        (4, 2, "How engaged do you feel at work?", "rating", 1),
        (5, 2, "What motivates you most?", "multiple_choice", 1),
        (6, 3, "Rate the product quality", "rating", 1),
        (7, 3, "Suggest improvements", "text", 0)
    ]
    
    cursor.executemany("""
        INSERT OR REPLACE INTO questions (question_id, survey_id, question_text, question_type, required)
        VALUES (?, ?, ?, ?, ?)
    """, sample_questions)
    
    # Insert sample responses
    sample_responses = [
        (1, 1, 1, "RESP001", "Very Satisfied", 5, "2024-01-20 10:30:00"),
        (2, 1, 2, "RESP001", "Faster response time", None, "2024-01-20 10:31:00"),
        (3, 1, 3, "RESP001", "Yes", 1, "2024-01-20 10:32:00"),
        (4, 1, 1, "RESP002", "Satisfied", 4, "2024-01-21 14:15:00"),
        (5, 1, 3, "RESP002", "Yes", 1, "2024-01-21 14:16:00"),
        (6, 2, 4, "EMP001", "Highly Engaged", 5, "2024-03-05 09:00:00"),
        (7, 2, 5, "EMP001", "Recognition", None, "2024-03-05 09:01:00"),
        (8, 3, 6, "CUST001", "Excellent", 5, "2024-02-15 16:20:00"),
        (9, 3, 7, "CUST001", "Better packaging", None, "2024-02-15 16:21:00")
    ]
    
    cursor.executemany("""
        INSERT OR REPLACE INTO responses (response_id, survey_id, question_id, respondent_id, answer_text, answer_numeric, response_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, sample_responses)
    
    # Insert sample demographics
    sample_demographics = [
        ("RESP001", "25-34", "Female", "Bachelor's", "$50,000-$75,000", "New York"),
        ("RESP002", "35-44", "Male", "Master's", "$75,000-$100,000", "California"),
        ("EMP001", "28-35", "Non-binary", "Bachelor's", "$60,000-$80,000", "Texas"),
        ("CUST001", "45-54", "Female", "High School", "$40,000-$60,000", "Florida")
    ]
    
    cursor.executemany("""
        INSERT OR REPLACE INTO demographics (respondent_id, age_group, gender, education_level, income_range, location)
        VALUES (?, ?, ?, ?, ?, ?)
    """, sample_demographics)
    
    conn.commit()
    conn.close()
    
    print("Sample survey database created successfully!")
    print("Database location: data/survey.db")

if __name__ == "__main__":
    create_sample_survey_database()
