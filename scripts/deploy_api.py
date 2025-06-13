#!/usr/bin/env python3
"""
Deployment script for AggieRMP API
Prepares the application for VM deployment
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, shell=True):
    """Run a command and return success status"""
    try:
        result = subprocess.run(cmd, shell=shell, check=True, capture_output=True, text=True)
        print(f"✅ {cmd}")
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {cmd}")
        print(f"   Error: {e.stderr.strip()}")
        return False

def check_environment():
    """Check if environment is properly configured"""
    print("🔍 Checking environment...")
    
    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .env file not found. Copy from env.example and configure.")
        return False
    
    # Check required environment variables
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_PORT", 
        "POSTGRES_DATABASE",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD"
    ]
    
    from dotenv import load_dotenv
    load_dotenv()
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("✅ Environment configuration looks good")
    return True

def test_database_connection():
    """Test database connectivity"""
    print("🔍 Testing database connection...")
    
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from aggiermp.database.base import get_session
        
        session = get_session()
        result = session.execute("SELECT 1")
        session.close()
        
        print("✅ Database connection successful")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_api_import():
    """Test if API can be imported"""
    print("🔍 Testing API import...")
    
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from aggiermp.api.main import app
        print("✅ API import successful")
        return True
    except Exception as e:
        print(f"❌ API import failed: {e}")
        return False

def create_systemd_service():
    """Create systemd service file for Linux deployment"""
    print("🔧 Creating systemd service file...")
    
    service_content = """[Unit]
Description=AggieRMP FastAPI Application
After=network.target

[Service]
Type=exec
User=ubuntu
WorkingDirectory=/home/ubuntu/aggiermp
Environment=PATH=/home/ubuntu/aggiermp/.venv/bin
ExecStart=/home/ubuntu/aggiermp/.venv/bin/uvicorn src.aggiermp.api.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
"""
    
    service_file = Path("scripts/aggiermp.service")
    with open(service_file, 'w') as f:
        f.write(service_content)
    
    print(f"✅ Systemd service file created: {service_file}")
    return True

def create_startup_script():
    """Create startup script for manual deployment"""
    print("🔧 Creating startup script...")
    
    startup_content = """#!/bin/bash
# AggieRMP API Startup Script

set -e

echo "🚀 Starting AggieRMP API..."

# Navigate to project directory
cd "$(dirname "$0")/.."

# Load environment variables
if [ -f .env ]; then
    echo "📋 Loading environment variables..."
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "❌ .env file not found!"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment not found! Run 'uv sync' first."
    exit 1
fi

# Activate virtual environment and start server
echo "🔥 Starting FastAPI server..."
.venv/bin/uvicorn src.aggiermp.api.main:app --host 0.0.0.0 --port 8000 --workers 4
"""
    
    startup_file = Path("scripts/start_api.sh")
    with open(startup_file, 'w') as f:
        f.write(startup_content)
    
    # Make executable
    startup_file.chmod(0o755)
    
    print(f"✅ Startup script created: {startup_file}")
    return True

def create_docker_files():
    """Create Docker configuration for containerized deployment"""
    print("🔧 Creating Docker configuration...")
    
    dockerfile_content = """FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    postgresql-client \\
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Install uv and dependencies
RUN pip install uv
RUN uv sync --frozen

# Copy environment file
COPY .env ./

# Expose port
EXPOSE 8000

# Start command
CMD ["uv", "run", "uvicorn", "src.aggiermp.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
"""
    
    dockerfile = Path("Dockerfile")
    with open(dockerfile, 'w') as f:
        f.write(dockerfile_content)
    
    # Create docker-compose.yml
    compose_content = """version: '3.8'

services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - POSTGRES_HOST=${POSTGRES_HOST}
      - POSTGRES_PORT=${POSTGRES_PORT}
      - POSTGRES_DATABASE=${POSTGRES_DATABASE}
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    restart: unless-stopped
"""
    
    compose_file = Path("docker-compose.yml")
    with open(compose_file, 'w') as f:
        f.write(compose_content)
    
    print(f"✅ Docker files created: {dockerfile}, {compose_file}")
    return True

def main():
    """Main deployment preparation function"""
    print("🚀 AggieRMP API Deployment Preparation")
    print("=" * 50)
    
    success = True
    
    # Environment checks
    if not check_environment():
        success = False
    
    if not test_database_connection():
        success = False
    
    if not test_api_import():
        success = False
    
    # Create deployment files
    create_systemd_service()
    create_startup_script()
    create_docker_files()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ Deployment preparation completed successfully!")
        print("\nNext steps for VM deployment:")
        print("1. Copy project files to VM")
        print("2. Run 'uv sync' to install dependencies")
        print("3. Configure .env file with VM database settings")
        print("4. Run './scripts/start_api.sh' or use systemd service")
        print("\nAPI will be available at: http://your-vm-ip:8000")
    else:
        print("❌ Deployment preparation failed!")
        print("Please fix the issues above before deploying.")
        sys.exit(1)

if __name__ == "__main__":
    main() 