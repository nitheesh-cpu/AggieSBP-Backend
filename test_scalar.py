#!/usr/bin/env python3
"""
Test script to verify Scalar documentation is working
"""

import requests
import json
import sys
import time

def test_api_endpoints():
    """Test key API endpoints and documentation availability"""
    base_url = "http://localhost:8000"
    
    endpoints_to_test = [
        ("/", "Root endpoint"),
        ("/health", "Health check"),
        ("/openapi.json", "OpenAPI schema"),
        ("/docs", "Scalar documentation"),
    ]
    
    print("🧪 Testing AggieRMP API with Scalar documentation...")
    print(f"📍 Base URL: {base_url}")
    print("=" * 60)
    
    for endpoint, description in endpoints_to_test:
        url = f"{base_url}{endpoint}"
        try:
            print(f"Testing {description}...")
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                print(f"✅ {endpoint} - {description}: OK")
                
                # Special handling for different content types
                if endpoint == "/openapi.json":
                    try:
                        schema = response.json()
                        print(f"   📋 OpenAPI version: {schema.get('openapi', 'Unknown')}")
                        print(f"   📋 API title: {schema.get('info', {}).get('title', 'Unknown')}")
                        print(f"   📋 API version: {schema.get('info', {}).get('version', 'Unknown')}")
                        print(f"   📋 Endpoints found: {len(schema.get('paths', {}))}")
                    except:
                        print("   ⚠️  Could not parse OpenAPI schema")
                
                elif endpoint == "/docs":
                    content_type = response.headers.get('content-type', '')
                    if 'text/html' in content_type:
                        print("   📄 Scalar documentation HTML loaded successfully")
                        if 'scalar' in response.text.lower():
                            print("   🎯 Scalar components detected in HTML")
                        else:
                            print("   ⚠️  Scalar components not found in HTML")
                    else:
                        print(f"   ⚠️  Unexpected content type: {content_type}")
                
                elif endpoint == "/health":
                    try:
                        health_data = response.json()
                        print(f"   💚 Status: {health_data.get('status', 'Unknown')}")
                        print(f"   💾 Database: {health_data.get('database', {}).get('status', 'Unknown')}")
                    except:
                        print("   ⚠️  Could not parse health response")
                        
            else:
                print(f"❌ {endpoint} - {description}: HTTP {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            print(f"❌ {endpoint} - {description}: Connection failed (server not running?)")
        except requests.exceptions.Timeout:
            print(f"❌ {endpoint} - {description}: Request timeout")
        except Exception as e:
            print(f"❌ {endpoint} - {description}: Error - {str(e)}")
        
        print()
    
    print("=" * 60)
    print("📚 Documentation Access:")
    print(f"   🎨 Scalar UI: {base_url}/docs")
    print(f"   📖 Swagger UI: {base_url}/redoc") 
    print(f"   📄 OpenAPI JSON: {base_url}/openapi.json")
    print()
    print("💡 If the server is not running, start it with:")
    print("   python run_api.py")

if __name__ == "__main__":
    # Give the server a moment to start if it was just launched
    print("⏳ Waiting for server to start...")
    time.sleep(3)
    test_api_endpoints() 