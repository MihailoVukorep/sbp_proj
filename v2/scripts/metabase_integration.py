import requests
import json
from typing import Dict, List, Optional
import time
import sys
import io

# Importuj sve queries
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
        """Autentifikuj se na Metabase"""
        try:
            print(f"Konektovanje na Metabase: {self.base_url}")
            print(f"Korisnik: {self.username}")
            
            response = requests.post(
                f"{self.base_url}/api/session",
                json={"username": self.username, "password": self.password},
                timeout=5
            )
            
            if response.status_code == 200:
                self.session_token = response.json().get('id')
                print(f"✓ Autentifikacija uspešna\n")
                return True
            elif response.status_code == 401:
                print(f"✗ Autentifikacija neuspešna: 401 Unauthorized")
                print(f"  Proverite kredencijale!")
                return False
            else:
                print(f"✗ Autentifikacija neuspešna: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"✗ Nije moguće konekcija sa Metabase na {self.base_url}")
            return False
        except Exception as e:
            print(f"✗ Greška pri autentifikaciji: {str(e)}")
            return False
    
    def setup_mongodb_connection(self, db_name: str, connection_string: str) -> Optional[int]:
        """Postavi konekciju na lokalnu MongoDB"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            # Prvo verifikuj MongoDB konekciju
            print(f"Verifikujem MongoDB konekciju: {db_name}")
            try:
                from pymongo import MongoClient
                test_client = MongoClient(connection_string, serverSelectionTimeoutMS=3000)
                test_client.admin.command('ping')
                print("✓ MongoDB je dostupna")
            except Exception as mongo_error:
                print(f"✗ MongoDB nije dostupna: {str(mongo_error)}")
                return None
            
            print(f"Konfigurišem MongoDB u Metabase-u...\n")
            
            # Proveravamo da li konekcija već postoji
            try:
                db_list_response = requests.get(
                    f"{self.base_url}/api/database",
                    headers=headers,
                    timeout=10
                )
                
                existing_db = None
                if db_list_response.status_code == 200:
                    db_list = db_list_response.json()
                    # Proveri da li je response zaista lista
                    if isinstance(db_list, list):
                        for db in db_list:
                            if isinstance(db, dict) and db.get('name') == db_name:
                                existing_db = db
                                break
                
                if existing_db:
                    print(f"✓ Pronašao sam postojeću konekciju: {db_name} (ID: {existing_db['id']})\n")
                    return existing_db['id']
            except Exception as check_error:
                print(f"  ⚠️  Nije moguće proveriti postojeće konekcije: {str(check_error)}")
                print(f"  Nastavljam sa kreiranjem nove konekcije...\n")
            
            # Kreiramo novu konekciju sa host.docker.internal (za Windows/Mac Docker Desktop)
            # Pokušaj sa više host opcija
            hosts_to_try = [
                ("host.docker.internal", "Docker Desktop (Windows/Mac)"),
                ("172.17.0.1", "Docker bridge gateway"),
                ("localhost", "Direktna konekcija"),
            ]
            
            for host_name, description in hosts_to_try:
                print(f"  Pokušavam sa: {description} ({host_name})")
                
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
                        print(f"    Pokušaj {attempt + 1}/{max_retries}...")
                        
                        response = requests.post(
                            f"{self.base_url}/api/database",
                            json=payload,
                            headers=headers,
                            timeout=30
                        )
                        
                        if response.status_code == 200:
                            db_id = response.json().get('id')
                            print(f"  ✓ MongoDB konekcija uspešno kreirana sa {host_name} (ID: {db_id})\n")
                            return db_id
                        elif response.status_code == 400:
                            error_msg = response.json().get('message', response.text)
                            print(f"    ✗ Bad Request: {error_msg[:100]}")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                        else:
                            print(f"    ✗ Greška: {response.status_code}")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                    
                    except requests.exceptions.Timeout:
                        print(f"    ✗ Timeout")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                    except Exception as e:
                        print(f"    ✗ Greška: {str(e)[:100]}")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                
                print()  # Novi red između pokušaja
            
            print(f"✗ Nije bilo moguće konekcija sa MongoDB sa nijednom opcijom")
            return None
            
        except Exception as e:
            print(f"✗ Greška u setup_mongodb_connection: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_query(self, query_name: str, query_pipeline: str, db_id: int, collection_name: str) -> Optional[int]:
        """Kreiraj novi upit u Metabase"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            # Konvertuj aggregation pipeline u MongoDB native query format
            # Metabase očekuje "collection" field sa imenom kolekcije
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
                print(f"  ✓ Upit kreiran: {query_name} (ID: {query_id})")
                return query_id
            else:
                error_msg = response.text[:200]
                print(f"  ✗ Greška pri kreiranju upita: {response.status_code}")
                print(f"    {error_msg}")
                return None
        except Exception as e:
            print(f"  ✗ Greška: {str(e)}")
            return None
    
    def create_dashboard(self, dashboard_name: str) -> Optional[int]:
        """Kreiraj novi dashboard"""
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
                print(f"✓ Dashboard kreiran: {dashboard_name} (ID: {dashboard_id})\n")
                return dashboard_id
            else:
                print(f"✗ Greška pri kreiranju dashboarda: {response.status_code}")
                return None
        except Exception as e:
            print(f"✗ Greška: {str(e)}")
            return None
    
    def setup_performance_comparison_dashboard(self):
        """Postavi dashboard sa poređenjem performansi V1 vs V2"""
        print("\n" + "="*80)
        print("POSTAVLJANJE METABASE PERFORMANCE DASHBOARD-A (V1 vs V2)")
        print("="*80 + "\n")
        
        if not self.authenticate():
            return False
        
        time.sleep(2)
        
        # Postavi konekcije na obe verzije
        print("1. POSTAVLJANJE MONGODB KONEKCIJA\n")
        
        self.db_id_v1 = self.setup_mongodb_connection("SBP_V1 (Original)", 
                                                       "mongodb://localhost:27017/SBP_DB")
        if not self.db_id_v1:
            print("✗ Nije moguće konekcija na V1 bazu")
            return False
        
        self.db_id_v2 = self.setup_mongodb_connection("SBP_V2 (Optimized)", 
                                                       "mongodb://localhost:27017/SBP_DB")
        if not self.db_id_v2:
            print("✗ Nije moguće konekcija na V2 bazu")
            return False
        
        time.sleep(2)
        
        # Kreiraj dashboard
        print("2. KREIRANJE DASHBOARD-A\n")
        dashboard_id = self.create_dashboard("Upiti dashboard")
        if not dashboard_id:
            return False
        
        time.sleep(1)
        
        # Definiši upite za obe verzije
        print("3. KREIRANJE UPITA\n")
        
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
        
        # Kreiraj upite za obe verzije
        print("\nV1 (Originalna verzija - kolekcija 'movies'):")
        for query_config in queries:
            time.sleep(1)
            query_name_v1 = f"[V1] {query_config['name']}"
            self.create_query(query_name_v1, json.dumps(query_config['v1']), self.db_id_v1, "movies")
        
        print("\nV2 (Optimizovana verzija - kolekcija 'movies_optimized'):")
        for query_config in queries:
            time.sleep(1)
            query_name_v2 = f"[V2] {query_config['name']}"
            self.create_query(query_name_v2, json.dumps(query_config['v2']), self.db_id_v2, "movies_optimized")
        
        # Prikazi rezultate
        print("\n" + "="*80)
        print("✓ METABASE DASHBOARD JE USPEŠNO KREIRAN!")
        print("="*80)
        print(f"\nDashboard: {self.base_url}/dashboard/{dashboard_id}")
        print(f"Dashboard ID: {dashboard_id}")
        print(f"\nKreirani upiti:")
        print(f"  V1 (Originalna - 'movies'):")
        for query_config in queries:
            print(f"    • [V1] {query_config['name']}")
        print(f"\n  V2 (Optimizovana - 'movies_optimized'):")
        for query_config in queries:
            print(f"    • [V2] {query_config['name']}")
        
        print(f"\nUPUTSTVO ZA KORIŠĆENJE:")
        print(f"1. Pristupite Metabase na: {self.base_url}")
        print(f"2. U levi meni kliknite na 'Dashboards'")
        print(f"3. Pronađite 'Performance Analiza V1 vs V2'")
        print(f"4. Kliknite na 'Edit' u gornjem desnom uglu")
        print(f"5. Dodajte kartice sa upitima da kreirate vizuelizaciju")
        print(f"6. Poredite rezultate V1 i V2 upita\n")
        
        return True


if __name__ == "__main__":
    # Postavi UTF-8 encoding za Windows konzolu
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    print("\n" + "="*80)
    print("METABASE PERFORMANCE DASHBOARD SETUP - V1 vs V2 POREĐENJE")
    print("="*80)
    
    # Provera MongoDB
    print("\nPREVERAVANJE PREDUSLOVA:\n")
    
    print("Proveravajući MongoDB...")
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("✓ MongoDB je dostupna\n")
    except Exception as e:
        print(f"✗ MongoDB nije dostupna: {str(e)}")
        print("Pokrenite MongoDB komandom: mongod\n")
        sys.exit(1)
    
    # Pokretanje setupa
    metabase = MetabaseIntegration()
    success = metabase.setup_performance_comparison_dashboard()
    
    if success:
        print("\n" + "="*80)
        print("✓ SETUP JE USPEŠNO ZAVRŠEN!")
        print("="*80)
        print("\nSledećI koraci:")
        print("1. Otvori: http://localhost:3000")
        print("2. Navigiraj do: Dashboards > Performance Analiza V1 vs V2")
        print("3. Klikni 'Edit' i dodaj kartice sa upitima")
        print("4. Poredi rezultate između V1 i V2\n")
    else:
        print("\n" + "="*80)
        print("✗ SETUP JE NEUSPEŠAN")
        print("="*80)
        print("\nPreverite:")
        print("1. Da li je Metabase pokrenuta: docker ps | grep metabase")
        print("2. Da li je MongoDB dostupna: mongosh")
        print("3. Da li su kolekcije 'movies' i 'movies_optimized' dostupne\n")
        sys.exit(1)
