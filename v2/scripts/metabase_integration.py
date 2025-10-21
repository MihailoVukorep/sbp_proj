import requests
import json
from typing import Dict, List, Optional
import time
import sys
import io

from queries.query_1 import QUERY_1_V1, QUERY_1_V2, QUERY_NAME as Q1_NAME
from queries.query_2 import QUERY_2_V1, QUERY_2_V2, QUERY_NAME as Q2_NAME
from queries.query_3 import QUERY_3_V1, QUERY_3_V2, QUERY_NAME as Q3_NAME
from queries.query_4 import QUERY_4_V1, QUERY_4_V2, QUERY_NAME as Q4_NAME
from queries.query_5 import QUERY_5_V1, QUERY_5_V2, QUERY_NAME as Q5_NAME


class MetabaseIntegration:
    
    def __init__(self, metabase_url: str = "http://localhost:3000", 
                 username: str = "test@gmail.com", 
                 password: str = "1Qwertz*"):
        self.base_url = metabase_url.rstrip('/')
        self.username = username
        self.password = password
        self.session_token = None
        self.db_id_v1 = None
        self.db_id_v2 = None
    
    def authenticate(self) -> bool:
        """Authenticate with Metabase"""
        try:
            print(f"Connecting to Metabase: {self.base_url}")
            print(f"Username: {self.username}")
            
            response = requests.post(
                f"{self.base_url}/api/session",
                json={"username": self.username, "password": self.password},
                timeout=5
            )
            
            if response.status_code == 200:
                self.session_token = response.json().get('id')
                print(f"Authentication successful\n")
                return True
            elif response.status_code == 401:
                print(f"Authentication failed: 401 Unauthorized")
                print(f"  Check your credentials!")
                return False
            else:
                print(f"Authentication failed: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"Cannot connect to Metabase at {self.base_url}")
            return False
        except Exception as e:
            print(f"Error during authentication: {str(e)}")
            return False
    
    def setup_mongodb_connection(self, db_name: str, connection_string: str) -> Optional[int]:
        """Setup connection to local MongoDB"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            print(f"Verifying MongoDB connection: {db_name}")
            try:
                from pymongo import MongoClient
                test_client = MongoClient(connection_string, serverSelectionTimeoutMS=3000)
                test_client.admin.command('ping')
                print("MongoDB is available")
            except Exception as mongo_error:
                print(f"MongoDB is not available: {str(mongo_error)}")
                return None
            
            print(f"Configuring MongoDB in Metabase...\n")
            
            try:
                db_list_response = requests.get(
                    f"{self.base_url}/api/database",
                    headers=headers,
                    timeout=10
                )
                
                existing_db = None
                if db_list_response.status_code == 200:
                    db_list = db_list_response.json()
                    if isinstance(db_list, list):
                        for db in db_list:
                            if isinstance(db, dict) and db.get('name') == db_name:
                                existing_db = db
                                break
                
                if existing_db:
                    print(f"Found existing connection: {db_name} (ID: {existing_db['id']})\n")
                    return existing_db['id']
            except Exception as check_error:
                print(f"  Could not verify existing connections: {str(check_error)}")
                print(f"  Continuing with new connection...\n")
            
            hosts_to_try = [
                ("host.docker.internal", "Docker Desktop (Windows/Mac)"),
                ("172.17.0.1", "Docker bridge gateway"),
                ("localhost", "Direct connection"),
            ]
            
            for host_name, description in hosts_to_try:
                print(f"  Trying: {description} ({host_name})")
                
                payload = {
                    "name": db_name,
                    "engine": "mongo",
                    "details": {
                        "host": host_name,
                        "port": 27017,
                        "dbname": "SBP_DB"
                    },
                    "is_sample": False
                }
                
                max_retries = 2
                for attempt in range(max_retries):
                    try:
                        print(f"    Attempt {attempt + 1}/{max_retries}...")
                        
                        response = requests.post(
                            f"{self.base_url}/api/database",
                            json=payload,
                            headers=headers,
                            timeout=30
                        )
                        
                        if response.status_code == 200:
                            db_id = response.json().get('id')
                            print(f"  MongoDB connection created with {host_name} (ID: {db_id})\n")
                            return db_id
                        elif response.status_code == 400:
                            error_msg = response.json().get('message', response.text)
                            print(f"    Bad Request: {error_msg[:100]}")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                        else:
                            print(f"    Error: {response.status_code}")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                    
                    except requests.exceptions.Timeout:
                        print(f"    Timeout")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                    except Exception as e:
                        print(f"    Error: {str(e)[:100]}")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                
                print()
            
            print(f"Could not connect to MongoDB with any option")
            return None
            
        except Exception as e:
            print(f"Error in setup_mongodb_connection: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_query(self, query_name: str, query_pipeline: str, db_id: int, collection_name: str) -> Optional[int]:
        """Create a new query in Metabase"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            payload = {
                "name": query_name,
                "description": f"Performance Query: {query_name}",
                "type": "question",
                "display": "table",
                "visualization_settings": {},
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": query_pipeline,
                        "collection": collection_name
                    },
                    "database": db_id
                }
            }
            
            response = requests.post(
                f"{self.base_url}/api/card",
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                query_id = response.json().get('id')
                print(f"  Query created: {query_name} (ID: {query_id})")
                return query_id
            else:
                error_msg = response.text[:200]
                print(f"  Error creating query: {response.status_code}")
                print(f"    {error_msg}")
                return None
        except Exception as e:
            print(f"  Error: {str(e)}")
            return None
    
    def create_dashboard(self, dashboard_name: str) -> Optional[int]:
        """Create a new dashboard"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            payload = {
                "name": dashboard_name,
                "description": f"Performance Analysis: {dashboard_name}"
            }
            
            response = requests.post(
                f"{self.base_url}/api/dashboard",
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                dashboard_id = response.json().get('id')
                print(f"Dashboard created: {dashboard_name} (ID: {dashboard_id})\n")
                return dashboard_id
            else:
                print(f"Error creating dashboard: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None
    
    def setup_performance_comparison_dashboard(self):
        """Setup dashboard with performance comparison V1 vs V2"""
        print("\n" + "="*80)
        print("SETTING UP METABASE PERFORMANCE DASHBOARD (V1 vs V2)")
        print("="*80 + "\n")
        
        if not self.authenticate():
            return False
        
        time.sleep(2)
        
        print("1. SETTING UP MONGODB CONNECTIONS\n")
        
        self.db_id_v1 = self.setup_mongodb_connection("SBP_V1 (Original)", 
                                                       "mongodb://localhost:27017/SBP_DB")
        if not self.db_id_v1:
            print("Cannot connect to V1 database")
            return False
        
        self.db_id_v2 = self.setup_mongodb_connection("SBP_V2 (Optimized)", 
                                                       "mongodb://localhost:27017/SBP_DB")
        if not self.db_id_v2:
            print("Cannot connect to V2 database")
            return False
        
        time.sleep(2)
        
        print("2. CREATING DASHBOARD\n")
        dashboard_id = self.create_dashboard("Query Dashboard")
        if not dashboard_id:
            return False
        
        time.sleep(1)
        
        print("3. CREATING QUERIES\n")
        
        queries = [
            {
                "name": Q1_NAME,
                "v1": QUERY_1_V1,
                "v2": QUERY_1_V2
            },
            {
                "name": Q2_NAME,
                "v1": QUERY_2_V1,
                "v2": QUERY_2_V2
            },
            {
                "name": Q3_NAME,
                "v1": QUERY_3_V1,
                "v2": QUERY_3_V2
            },
            {
                "name": Q4_NAME,
                "v1": QUERY_4_V1,
                "v2": QUERY_4_V2
            },
            {
                "name": Q5_NAME,
                "v1": QUERY_5_V1,
                "v2": QUERY_5_V2
            }
        ]
        
        print("\nV1 (Original version - 'movies' collection):")
        for query_config in queries:
            time.sleep(1)
            query_name_v1 = f"[V1] {query_config['name']}"
            self.create_query(query_name_v1, json.dumps(query_config['v1']), self.db_id_v1, "movies")
        
        print("\nV2 (Optimized version - 'movies_optimized' collection):")
        for query_config in queries:
            time.sleep(1)
            query_name_v2 = f"[V2] {query_config['name']}"
            self.create_query(query_name_v2, json.dumps(query_config['v2']), self.db_id_v2, "movies_optimized")
        
        print("\n" + "="*80)
        print("METABASE DASHBOARD CREATED SUCCESSFULLY!")
        print("="*80)
        print(f"\nDashboard: {self.base_url}/dashboard/{dashboard_id}")
        print(f"Dashboard ID: {dashboard_id}")
        print(f"\nCreated queries:")
        print(f"  V1 (Original - 'movies'):")
        for query_config in queries:
            print(f"    [V1] {query_config['name']}")
        print(f"\n  V2 (Optimized - 'movies_optimized'):")
        for query_config in queries:
            print(f"    [V2] {query_config['name']}")
        
        print(f"\nUSAGE INSTRUCTIONS:")
        print(f"1. Open Metabase at: {self.base_url}")
        print(f"2. Click on 'Dashboards' in left menu")
        print(f"3. Find 'Query Dashboard'")
        print(f"4. Click 'Edit' in top right corner")
        print(f"5. Add cards with queries to create visualization")
        print(f"6. Compare results between V1 and V2\n")
        
        return True


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    print("\n" + "="*80)
    print("METABASE PERFORMANCE DASHBOARD SETUP - V1 vs V2 COMPARISON")
    print("="*80)
    
    print("\nCHECKING PREREQUISITES:\n")
    
    print("Checking MongoDB...")
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("MongoDB is available\n")
    except Exception as e:
        print(f"MongoDB is not available: {str(e)}")
        print("Start MongoDB with: mongod\n")
        sys.exit(1)
    
    metabase = MetabaseIntegration()
    success = metabase.setup_performance_comparison_dashboard()
    
    if success:
        print("\n" + "="*80)
        print("SETUP COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\nNext steps:")
        print("1. Open: http://localhost:3000")
        print("2. Navigate to: Dashboards > Query Dashboard")
        print("3. Click 'Edit' and add cards with queries")
        print("4. Compare results between V1 and V2\n")
    else:
        print("\n" + "="*80)
        print("SETUP FAILED")
        print("="*80)
        print("\nCheck:")
        print("1. Is Metabase running: docker ps | grep metabase")
        print("2. Is MongoDB available: mongosh")
        print("3. Are 'movies' and 'movies_optimized' collections available\n")
        sys.exit(1)
