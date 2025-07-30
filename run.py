#!/usr/bin/env python3
"""
Survey Data API Gateway Startup Script
"""

import os
import sys
import subprocess
from setup_database import create_sample_survey_database

def setup_environment():
    """Set up the development environment"""
    print("Setting up Survey Data API Gateway...")
    
    # Create sample database
    print("1. Creating sample survey database...")
    create_sample_survey_database()
    
    # Install dependencies (if needed)
    print("2. Environment setup complete!")
    print("\nTo start the API server, run:")
    print("uvicorn main:app --reload --host 0.0.0.0 --port 8000")

def start_server():
    """Start the FastAPI server"""
    print("Starting Survey Data API Gateway server...")
    os.system("uvicorn main:app --reload --host 0.0.0.0 --port 8000")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        start_server()
    else:
        setup_environment()
