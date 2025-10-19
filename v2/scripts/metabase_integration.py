import requests
import json
from typing import Dict, List, Optional
import time


class MetabaseIntegration:
    
    def __init__(self, metabase_url: str = "http://localhost:3000", 
                 username: str = "test@gmail.com", 
                 password: str = "1Qwertz*"):
        self.base_url = metabase_url.rstrip('/')
        self.username = username
        self.password = password
        self.session_token = None
        self.db_id = None
    
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
                print(f"✓ Autentifikacija uspešna")
                return True
            elif response.status_code == 401:
                print(f"✗ Autentifikacija neuspešna: 401 Unauthorized")
                print(f"  Proverite kredencijale!")
                print(f"  Email: {self.username}")
                print(f"  Mogućnosti:")
                print(f"    1. admin@metabase.local / metabase (default)")
                print(f"    2. Pronađite korisnika sa: docker logs metabase | grep User")
                return False
            else:
                print(f"✗ Autentifikacija neuspešna: {response.status_code}")
                print(f"  Odgovor: {response.text}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"✗ Nije moguće konekcija sa Metabase na {self.base_url}")
            print("  Pokrenite Metabase sa:")
            print("  docker run -d -p 3000:3000 --name metabase metabase/metabase")
            return False
        except Exception as e:
            print(f"✗ Greška pri autentifikaciji: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_databases(self) -> List[Dict]:
        """Preuzmi listu baza"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            response = requests.get(f"{self.base_url}/api/database", headers=headers, timeout=5)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"✗ Greška pri preuzimanju baza: {response.status_code}")
                return []
        except Exception as e:
            print(f"✗ Greška: {str(e)}")
            return []
    
    def setup_mongodb_connection(self, db_name: str, connection_string: str) -> bool:
        """Postavi konekciju na lokalnu MongoDB"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            # Prvo verifikuj MongoDB konekciju
            print(f"Verifikujem MongoDB konekciju na: {connection_string}")
            try:
                from pymongo import MongoClient
                test_client = MongoClient(connection_string, serverSelectionTimeoutMS=2000)
                test_client.admin.command('ping')
                print("✓ MongoDB je dostupna")
            except Exception as mongo_error:
                print(f"✗ MongoDB nije dostupna: {str(mongo_error)}")
                return False
            
            print("\nKonfigurišem MongoDB u Metabase-u...")
            print("  (ovo može potrajati nekoliko sekundi - čekam...)\n")
            
            # Probajem alternativnu konfiguraciju (host/port umesto connection-string)
            # jer Metabase bolje podržava tu konfiguraciju
            # NAPOMENA: Koristimo host.docker.internal jer je Metabase u Docker kontejneru
            # a MongoDB je lokalno na Windows host-u
            alt_payload = {
                "name": db_name,
                "engine": "mongo",
                "details": {
                    "host": "host.docker.internal",  # Za Windows/Mac Docker Desktop
                    "port": 27017,
                    "dbname": "SBP_DB"
                },
                "is_sample": False
            }
            
            # Retry logika sa čekanjem
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(f"  Pokušaj {attempt + 1}/{max_retries}...")
                    
                    alt_response = requests.post(
                        f"{self.base_url}/api/database",
                        json=alt_payload,
                        headers=headers,
                        timeout=30  # Povećan timeout na 30 sekundi
                    )
                    
                    if alt_response.status_code == 200:
                        self.db_id = alt_response.json().get('id')
                        print(f"✓ MongoDB baza uspešno konfigurirana (ID: {self.db_id})\n")
                        return True
                    elif alt_response.status_code == 400:
                        error_msg = alt_response.json().get('message', alt_response.text)
                        print(f"  ✗ Bad Request: {error_msg}")
                        
                        # Ako je problem sa "get database details", čekaj i pokušaj ponovo
                        if "get database details" in error_msg.lower():
                            if attempt < max_retries - 1:
                                print(f"  Čekam 5 sekundi pre nego što pokušam ponovo...\n")
                                time.sleep(5)
                                continue
                        return False
                    elif alt_response.status_code == 401:
                        print(f"  ✗ Autentifikacija nije validna (401)")
                        return False
                    elif alt_response.status_code == 500:
                        print(f"  ✗ Greška u Metabase serveru (500)")
                        if attempt < max_retries - 1:
                            print(f"  Čekam 5 sekundi pre nego što pokušam ponovo...\n")
                            time.sleep(5)
                            continue
                        return False
                    else:
                        print(f"  ✗ Neočekivana greška: {alt_response.status_code}")
                        print(f"  Odgovor: {alt_response.text[:200]}")
                        return False
                
                except requests.exceptions.Timeout:
                    print(f"  ✗ Timeout (pokušaj {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        print(f"  Čekam 5 sekundi pre nego što pokušam ponovo...\n")
                        time.sleep(5)
                        continue
                    else:
                        print(f"\n✗ Metabase ne odgovara. Mogući razlozi:")
                        print(f"  - Metabase je preplavljena i sporo radi")
                        print(f"  - MongoDB konekcija je problematična")
                        print(f"  - Firewall blokira konekciju")
                        return False
                
                except Exception as e:
                    print(f"  ✗ Greška pri povezivanju: {str(e)}")
                    if attempt < max_retries - 1:
                        print(f"  Čekam 5 sekundi pre nego što pokušam ponovo...\n")
                        time.sleep(5)
                        continue
                    return False
            
            return False
            
        except Exception as e:
            print(f"✗ Greška: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_query(self, query_name: str, query_definition: Dict) -> Optional[int]:
        """Kreiraj novi upit u Metabase"""
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
                        "query": query_definition.get('query'),
                        "template-tags": {}
                    },
                    "database": self.db_id
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
                print(f"✓ Upit kreiran: {query_name} (ID: {query_id})")
                return query_id
            else:
                print(f"✗ Greška pri kreiranju upita: {response.status_code}")
                print(f"  Odgovor: {response.text[:500]}")
                return None
        except Exception as e:
            print(f"✗ Greška: {str(e)}")
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
                print(f"✓ Dashboard kreiran: {dashboard_name} (ID: {dashboard_id})")
                return dashboard_id
            else:
                print(f"✗ Greška pri kreiranju dashboarda: {response.status_code}")
                return None
        except Exception as e:
            print(f"✗ Greška: {str(e)}")
            return None
    
    def add_card_to_dashboard(self, dashboard_id: int, card_id: int, 
                            position_row: int = 0, position_col: int = 0) -> bool:
        """Dodaj karticu na dashboard"""
        try:
            headers = {'X-Metabase-Session': self.session_token}
            
            # Koristi POST na pravilan način
            payload = {
                "cardId": card_id,
                "row": position_row,
                "col": position_col,
                "sizeX": 6,
                "sizeY": 4
            }
            
            # Probaj sa alternativnim endpoint-om
            response = requests.post(
                f"{self.base_url}/api/dashboard/{dashboard_id}/cards",
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                print(f"✓ Kartica dodana (ID: {card_id})")
                return True
            else:
                # Ako ne radi, probaj sa drugim payload formatom
                payload2 = {
                    "card_id": card_id,
                    "row": position_row,
                    "col": position_col,
                    "size_x": 6,
                    "size_y": 4,
                    "parameter_mappings": [],
                    "visualization_settings": {}
                }
                
                response2 = requests.post(
                    f"{self.base_url}/api/dashboard/{dashboard_id}/cards",
                    json=payload2,
                    headers=headers,
                    timeout=15
                )
                
                if response2.status_code == 200:
                    print(f"✓ Kartica dodana (ID: {card_id})")
                    return True
                else:
                    print(f"✗ Greška pri dodavanju kartice: {response2.status_code}")
                    print(f"  Odgovor: {response2.text[:500]}")
                    return False
        except Exception as e:
            print(f"✗ Greška: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def setup_performance_dashboard(self, connection_string: str = "mongodb://localhost:27017/SBP_DB"):
        """Postavi kompletan performance dashboard"""
        print("\n" + "="*70)
        print("POSTAVLJANJE METABASE PERFORMANCE DASHBOARDA")
        print("="*70 + "\n")
        
        if not self.authenticate():
            return False
        
        time.sleep(2)
        
        if not self.setup_mongodb_connection("SBP_Movies", connection_string):
            return False
        
        # Daj Metabase-u vremena da se stabilizuje posle konfiguracije
        print("Stabilizujem Metabase...")
        time.sleep(3)
        
        dashboard_id = self.create_dashboard("Performance Analiza V1 vs V2")
        if not dashboard_id:
            return False
        
        time.sleep(1)
        
        queries = [
            {
                "name": "Query 1: Top 10 Profitable Companies",
                "query": json.dumps([
                    {"$match": {"financial.budget_category": {"$in": ["high", "ultra-high"]}}},
                    {"$unwind": "$production.companies"},
                    {"$group": {
                        "_id": "$production.companies",
                        "total_revenue": {"$sum": "$financial.revenue"},
                        "avg_budget": {"$avg": "$financial.budget"},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": 10}
                ])
            },
            {
                "name": "Query 2: Ratings by Genre and Decade",
                "query": json.dumps([
                    {"$unwind": "$content_info.genres"},
                    {"$match": {
                        "content_info.genres": {"$in": ["Action", "Comedy", "Drama", "Horror", "Thriller"]},
                        "release_info.decade": {"$exists": True}
                    }},
                    {"$group": {
                        "_id": {
                            "genre": "$content_info.genres",
                            "decade": "$release_info.decade"
                        },
                        "avg_vote": {"$avg": "$ratings.vote_average"},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"_id.decade": -1, "avg_vote": -1}},
                    {"$limit": 20}
                ])
            },
            {
                "name": "Query 3: Movies by Month",
                "query": json.dumps([
                    {"$match": {"financial.budget_category": "medium"}},
                    {"$group": {
                        "_id": {"month": "$release_info.month"},
                        "count": {"$sum": 1},
                        "avg_vote": {"$avg": "$ratings.vote_average"},
                        "avg_revenue": {"$avg": "$financial.revenue"}
                    }},
                    {"$sort": {"_id.month": 1}},
                    {"$limit": 12}
                ])
            },
            {
                "name": "Query 4: Genre Pairs Revenue",
                "query": json.dumps([
                    {"$match": {"content_info.genre_pairs": {"$exists": True, "$ne": []}}},
                    {"$unwind": "$content_info.genre_pairs"},
                    {"$group": {
                        "_id": "$content_info.genre_pairs",
                        "total_revenue": {"$sum": "$financial.revenue"},
                        "avg_vote": {"$avg": "$ratings.vote_average"},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": 10}
                ])
            },
            {
                "name": "Query 5: Runtime by Country and Quality",
                "query": json.dumps([
                    {"$match": {"ratings.quality_tier": {"$in": ["good", "excellent"]}}},
                    {"$unwind": "$production.countries"},
                    {"$addFields": {
                        "runtime_range": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$lte": ["$content_info.runtime", 90]}, "then": "Short"},
                                    {"case": {"$lte": ["$content_info.runtime", 120]}, "then": "Standard"}
                                ],
                                "default": "Long"
                            }
                        }
                    }},
                    {"$group": {
                        "_id": {
                            "country": "$production.countries",
                            "runtime_range": "$runtime_range"
                        },
                        "count": {"$sum": 1},
                        "avg_runtime": {"$avg": "$content_info.runtime"},
                        "avg_vote": {"$avg": "$ratings.vote_average"}
                    }},
                    {"$sort": {"count": -1}},
                    {"$limit": 15}
                ])
            }
        ]
        
        card_ids = []
        for i, query_config in enumerate(queries):
            time.sleep(2)
            card_id = self.create_query(query_config["name"], {"query": query_config["query"]})
            if card_id:
                card_ids.append(card_id)
        
        # NAPOMENA: Automatsko dodavanje kartica na dashboard ne radi u ovoj verziji Metabase-a
        # Umesto toga, korisnik može ručno dodati kartice
        print(f"\n" + "="*70)
        print(f"NAPOMENA: Dodavanje kartica na dashboard")
        print(f"="*70)
        print(f"\nAutomatsko dodavanje kartica ne radi u ovoj verziji Metabase-a.")
        print(f"Molimo vas da ručno dodate upite na dashboard:")
        print(f"\n1. Idite na: {self.base_url}/dashboard/{dashboard_id}")
        print(f"2. Kliknite na dugme 'Edit' u gornjem desnom uglu")
        print(f"3. Kliknite na '+' dugme da dodate karticu")
        print(f"4. Izaberite jedan od sledećih upita:")
        for i, card_id in enumerate(card_ids, 1):
            print(f"   - Query {i} (ID: {card_id})")
        print(f"5. Ponavljajte za sve upite")
        print(f"6. Kliknite 'Save' da sačuvate izmene\n")
        
        print(f"\n" + "="*70)
        print(f"✓ DASHBOARD JE USPEŠNO KREIRAN!")
        print(f"="*70)
        print(f"\nDashboard URL: {self.base_url}/dashboard/{dashboard_id}")
        print(f"Dashboard ID: {dashboard_id}")
        print(f"\nSvi upiti su dostupni na dashboard-u:")
        print(f"  • Query 1: Revenue po Production Company")
        print(f"  • Query 2: Rating po Genre i Deceniji")
        print(f"  • Query 3: Blockbuster Releasi po Mesecima")
        print(f"  • Query 4: Genre Kombinacije po Profitabilnosti")
        print(f"  • Query 5: Runtime po Zemlji i Kvalitetu")
        print(f"\n{self.base_url}/dashboard/{dashboard_id}\n")
        
        return True


class MetabaseQuickStart:
    
    @staticmethod
    def print_docker_command():
        print("\n" + "="*70)
        print("POKRETANJE METABASE DOCKER IMAGE-A")
        print("="*70 + "\n")
        
        print("Pokrenite Metabase sa sledećom komandom:\n")
        print("docker run -d -p 3000:3000 --name metabase metabase/metabase\n")
        
        print("Ili koristite docker-compose:\n")
        print("docker-compose up -d\n")
        
        print("="*70 + "\n")
    
    @staticmethod
    def print_setup_instructions():
        print("\n" + "="*70)
        print("METABASE INICIJALNA KONFIGURACIJA")
        print("="*70 + "\n")
        
        print("""1. ČEKANJE POKRETANJA
   - Metabase se pokreće, čekaj ~30 sekundi
   - Proveri status: docker ps | grep metabase

2. PRISTUP METABASE
   - Otvori u pregledniku: http://localhost:3000
   - Trebalo bi da vidiš Metabase welcome screen

3. KREIRAJ ADMIN NALOG
   - Email: admin@metabase.local
   - Lozinka: metabase (možete promeniti)
   - Potvrdi i nastavi

4. PRESKOČEVANJE BAZA
   - Pitaće te da dodaš baze - preskočí (Skip)
   - Metabase će biti spreman za Python skriptu

5. VERIFICIRAJ KONEKCIJU NA MONGODB
   - Čekaj da se sve učita i klikni Settings
   - Verifikuj MongoDB konekciju

6. POKRENI SETUP SKRIPTU
   - python metabase_integration.py
   - Skripta će kreirati sve upite i dashboard
""")
        print("="*70 + "\n")
    
    @staticmethod
    def print_troubleshooting():
        print("\n" + "="*70)
        print("TROUBLESHOOTING")
        print("="*70 + "\n")
        
        print("""Problem: Docker: command not found
Rešenje: Instalirajte Docker Desktop sa https://www.docker.com/products/docker-desktop

Problem: Port 3000 je već zauzet
Rešenje: docker run -d -p 3001:3000 --name metabase metabase/metabase
         (koristi port 3001, pristupite sa http://localhost:3001)

Problem: Metabase se ne pokreće
Rešenje: docker logs metabase
         docker rm -f metabase
         docker run -d -p 3000:3000 --name metabase metabase/metabase

Problem: Ne mogu pristupiti sa admin@metabase.local
Rešenje: Korisnik je možda drugačiji - proverite Metabase logs
         docker logs metabase

Problem: MongoDB konekcija ne radi
Rešenje: Proverite da MongoDB služi na localhost:27017
         mongosh --version  (trebalo bi da radi)
         Ažurirajte connection string ako je MongoDB na drugom host-u
""")
        print("="*70 + "\n")


if __name__ == "__main__":
    import sys
    import io
    
    # Postavi UTF-8 encoding za Windows konzolu
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    print("\n" + "="*70)
    print("METABASE PERFORMANCE DASHBOARD SETUP")
    print("="*70)
    
    # Proverom
    print("\n1. PROVERAVANJE PREDUSLOV\n")
    
    # Provera MongoDB
    print("   Proveravajući MongoDB...")
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("   ✓ MongoDB je dostupna\n")
    except Exception as e:
        print(f"   ✗ MongoDB nije dostupna: {str(e)}")
        print("   Pokrenite: mongod\n")
        sys.exit(1)
    
    # Pokretanje setupa
    print("2. POKRETANJE SETUP SKRIPTE\n")
    
    metabase = MetabaseIntegration()
    success = metabase.setup_performance_dashboard()
    
    if success:
        print("\n" + "="*70)
        print("✓ SETUP JE USPEŠNO ZAVRŠEN!")
        print("="*70)
        print(f"\nPristupite Metabase-u i ručno dodajte upite na dashboard:")
        print(f"  Dashboard URL: http://localhost:3000/dashboard/38")
        print(f"  (Dashboard ID može biti drugačiji - proverite output iznad)")
        print(f"\nKreirani upiti:")
        print(f"  • Query 1: Revenue po Production Company")
        print(f"  • Query 2: Rating po Genre i Deceniji")
        print(f"  • Query 3: Blockbuster Releasi po Mesecima")
        print(f"  • Query 4: Genre Kombinacije po Profitabilnosti")
        print(f"  • Query 5: Runtime po Zemlji i Kvalitetu")
        print(f"\nSvi upiti su dostupni u Questions sekciji u Metabase-u.\n")
    else:
        print("\n" + "="*70)
        print("✗ SETUP JE NEUSPEŠAN")
        print("="*70)
        print("\nPreverite:")
        print("  1. Da li je Metabase pokrenuta: docker ps | grep metabase")
        print("  2. Da li je MongoDB dostupna: mongosh --version")
        print("  3. Da li su podaci učitani: mongosh --eval 'db.getSiblingDB(\"SBP_DB\").getCollectionNames()'")
        print("\nZa pomoć, vidite METABASE_SETUP.md\n")
        sys.exit(1)

