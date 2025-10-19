"""
Test skripta za verifikaciju MongoDB i Metabase konekcije
"""
from pymongo import MongoClient
import requests

def test_mongodb():
    """Testiraj lokalnu MongoDB konekciju"""
    print("\n" + "="*70)
    print("TEST 1: MONGODB LOKALNO")
    print("="*70)
    
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        
        # Proveri SBP_DB bazu
        db = client['SBP_DB']
        collections = db.list_collection_names()
        
        print("✓ MongoDB je dostupna na localhost:27017")
        print(f"✓ Baza 'SBP_DB' postoji")
        print(f"✓ Kolekcije: {', '.join(collections)}")
        
        # Proveri broj dokumenata
        if 'movies_optimized' in collections:
            count = db['movies_optimized'].count_documents({})
            print(f"✓ Kolekcija 'movies_optimized' ima {count} dokumenata")
        
        return True
    except Exception as e:
        print(f"✗ MongoDB NIJE dostupna: {str(e)}")
        return False

def test_metabase():
    """Testiraj Metabase konekciju"""
    print("\n" + "="*70)
    print("TEST 2: METABASE DOCKER KONTEJNER")
    print("="*70)
    
    try:
        response = requests.get('http://localhost:3000/api/health', timeout=5)
        if response.status_code == 200:
            print("✓ Metabase je dostupna na http://localhost:3000")
            return True
        else:
            print(f"✗ Metabase odgovara sa statusom: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("✗ Metabase NIJE dostupna na http://localhost:3000")
        print("  Pokrenite: docker compose up -d")
        return False
    except Exception as e:
        print(f"✗ Greška: {str(e)}")
        return False

def print_next_steps(mongodb_ok, metabase_ok):
    """Prikaži sledeće korake"""
    print("\n" + "="*70)
    print("SLEDEĆI KORACI")
    print("="*70 + "\n")
    
    if not mongodb_ok:
        print("1. POKRENI MONGODB:")
        print("   - Windows: Otvori Services i startuj 'MongoDB Server'")
        print("   - Ili iz Command Prompt: net start MongoDB")
        print("   - Ili direktno: mongod --dbpath C:\\path\\to\\data\\db\n")
    
    if not metabase_ok:
        print("2. POKRENI METABASE:")
        print("   cd C:\\Users\\saleb\\Documents\\projekti\\sbp_proj")
        print("   docker compose up -d\n")
        print("   Čekaj ~30 sekundi da se Metabase pokrene")
        print("   Proveri: docker ps\n")
    
    if mongodb_ok and metabase_ok:
        print("✓ SVE JE SPREMNO!")
        print("\n3. POKRENI METABASE SETUP:")
        print("   cd v2\\scripts")
        print("   python metabase_integration.py")
        print("\n   Skripta će:")
        print("   - Povezati Metabase sa MongoDB koristeći 'host.docker.internal'")
        print("   - Kreirati dashboard sa upitima")
        print("   - Otvoriti http://localhost:3000/dashboard/1\n")

def main():
    print("\n" + "="*70)
    print("METABASE + MONGODB CONNECTION TEST")
    print("="*70)
    
    mongodb_ok = test_mongodb()
    metabase_ok = test_metabase()
    
    print_next_steps(mongodb_ok, metabase_ok)
    
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
