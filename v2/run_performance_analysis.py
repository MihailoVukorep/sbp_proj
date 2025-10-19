#!/usr/bin/env python3
"""
Kompletan workflow za performantnu analizu V1 vs V2
Koristi: performance_comparison.py -> performance_visualizer.py -> PERFORMANCE_ANALYSIS.md
"""

import sys
import json
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


def check_mongodb_connection():
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("✓ MongoDB je dostupan")
        return True
    except ServerSelectionTimeoutError:
        print("✗ MongoDB nije dostupan na localhost:27017")
        print("  Proverite da li je MongoDB sluzeban pokrenut sa: mongod")
        return False


def check_python_dependencies():
    dependencies = {
        'pymongo': 'PyMongo',
        'pandas': 'pandas',
        'matplotlib': 'matplotlib',
        'seaborn': 'seaborn',
        'requests': 'requests'
    }
    
    missing = []
    for module, name in dependencies.items():
        try:
            __import__(module)
            print(f"✓ {name} je instaliran")
        except ImportError:
            print(f"✗ {name} NIJE instaliran")
            missing.append(module)
    
    if missing:
        print(f"\nInstalacija nedostajućih paketa:")
        print(f"pip install {' '.join(missing)}")
        return False
    return True


def check_data_availability():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['SBP_DB']
    
    v1_count = db['movies'].count_documents({})
    v2_count = db['movies_optimized'].count_documents({})
    
    print(f"\nKolekcije u bazi:")
    print(f"✓ movies (V1): {v1_count} dokumenata")
    print(f"✓ movies_optimized (V2): {v2_count} dokumenata")
    
    if v1_count == 0 or v2_count == 0:
        print("\n✗ Nedostaju podaci!")
        print("  Trebate prvo izvršiti:")
        print("  1. python v1/scripts/init_db.py (za V1 kolekciju)")
        print("  2. python v2/scripts/init_db.py (za V2 kolekciju)")
        return False
    
    return True


def run_performance_comparison():
    print("\n" + "="*70)
    print("KORAK 1: MERENJE PERFORMANSI")
    print("="*70)
    
    try:
        from performance_comparison import PerformanceComparator
        
        client = MongoClient('mongodb://localhost:27017/')
        db = client['SBP_DB']
        
        v1_collection = db['movies']
        v2_collection = db['movies_optimized']
        
        print("\nPokrećem merenje performansi (5 iteracija po upitu)...")
        print("(Ovo može potrajati nekoliko minuta)\n")
        
        comparator = PerformanceComparator(v1_collection, v2_collection, iterations=5)
        comparator.run_comparison()
        
        summary = comparator.get_summary()
        comparator.export_results('performance_results.json')
        
        return True
    except Exception as e:
        print(f"✗ Greška pri merenju performansi: {str(e)}")
        return False


def run_visualization():
    print("\n" + "="*70)
    print("KORAK 2: GENERISANJE DIJAGRAMA")
    print("="*70)
    
    try:
        from performance_visualizer import PerformanceVisualizer
        
        if not Path('performance_results.json').exists():
            print("✗ performance_results.json nije pronađen")
            return False
        
        print("\nGenerisem dijagrame...\n")
        
        visualizer = PerformanceVisualizer('performance_results.json')
        visualizer.create_all_charts()
        
        return True
    except Exception as e:
        print(f"✗ Greška pri generisanju dijagrama: {str(e)}")
        return False


def display_summary():
    print("\n" + "="*70)
    print("KORAK 3: PRIKAZ REZULTATA")
    print("="*70)
    
    try:
        with open('performance_results.json', 'r') as f:
            results = json.load(f)
        
        print("\nDETALJNI REZULTATI:\n")
        
        total_v1 = 0
        total_v2 = 0
        
        for query, metrics in results.items():
            print(f"{query}:")
            print(f"  V1: {metrics['v1_avg_ms']}ms")
            print(f"  V2: {metrics['v2_avg_ms']}ms")
            print(f"  Poboljšanje: {metrics['improvement_percent']}%")
            print(f"  Ubrzanje: {metrics['speedup_factor']}x\n")
            
            total_v1 += metrics['v1_avg_ms']
            total_v2 += metrics['v2_avg_ms']
        
        avg_improvement = ((total_v1 - total_v2) / total_v1) * 100
        
        print(f"AGREGIRANA POBOLJŠANJA:")
        print(f"  Ukupno V1: {total_v1:.2f}ms")
        print(f"  Ukupno V2: {total_v2:.2f}ms")
        print(f"  Prosečno poboljšanje: {avg_improvement:.1f}%")
        print(f"  Prosečan speedup: {total_v1 / total_v2:.2f}x\n")
        
        print("DOSTUPNI DIJAGRAMI:")
        charts = [
            "performance_charts/execution_time_comparison.png",
            "performance_charts/improvement_percentage.png",
            "performance_charts/speedup_factor.png",
            "performance_charts/min_max_range.png",
            "performance_charts/summary_table.png"
        ]
        
        for chart in charts:
            if Path(chart).exists():
                print(f"  ✓ {chart}")
            else:
                print(f"  ✗ {chart}")
        
        print("\nDETALJAN IZVEŠTAJ: PERFORMANCE_ANALYSIS.md\n")
        
    except Exception as e:
        print(f"✗ Greška pri čitanju rezultata: {str(e)}")


def setup_metabase():
    print("\n" + "="*70)
    print("METABASE VIZUELIZACIJA (OPCIONO)")
    print("="*70)
    
    response = input("\nDa li želite da postavite Metabase dashboard? (y/n): ").strip().lower()
    
    if response == 'y':
        try:
            from metabase_integration import MetabaseIntegration, MetabaseQuickStart
            
            print("\nMetabase Setup:")
            print("1. Proverim konekciju...")
            
            metabase = MetabaseIntegration()
            if metabase.authenticate():
                print("2. Postavljam dashboard...")
                metabase.setup_performance_dashboard()
            else:
                print("✗ Ne mogu da se povežem na Metabase")
                print("\nZa pokrećanje Metabase:")
                
                docker_compose = MetabaseQuickStart.generate_docker_compose()
                with open('docker-compose.yml', 'w') as f:
                    f.write(docker_compose)
                print("✓ docker-compose.yml kreiran")
                
                MetabaseQuickStart.print_setup_instructions()
        
        except Exception as e:
            print(f"✗ Greška pri Metabase setupu: {str(e)}")


def main():
    print("\n" + "="*80)
    print(" "*15 + "ANALIZA PERFORMANSI V1 vs V2")
    print("="*80)
    
    print("\n" + "─"*80)
    print("PREDUSLOV PROVERA")
    print("─"*80)
    
    if not check_python_dependencies():
        print("\n✗ Nedostaju Python zavisnosti")
        sys.exit(1)
    
    if not check_mongodb_connection():
        print("\n✗ MongoDB nije dostupan")
        sys.exit(1)
    
    if not check_data_availability():
        print("\n✗ Podaci nisu dostupni")
        sys.exit(1)
    
    print("\n✓ Sve provjere su prošle uspešno\n")
    
    if not run_performance_comparison():
        sys.exit(1)
    
    if not run_visualization():
        sys.exit(1)
    
    display_summary()
    
    setup_metabase()
    
    print("\n" + "="*80)
    print("ANALIZA JE ZAVRŠENA!")
    print("="*80)
    print("\nSledećí koraci:")
    print("1. Pogledajte generirane dijagrame u performance_charts/")
    print("2. Pročitajte detaljnu analizu: PERFORMANCE_ANALYSIS.md")
    print("3. (Opciono) Postavite Metabase dashboard za real-time monitoring")
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
