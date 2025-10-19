#!/usr/bin/env python3
"""
Brz start skripta za kompletan workflow:
1. Merenje performansi (V1 vs V2)
2. Generisanje dijagrama
3. Metabase dashboard setup
"""

import sys
import os
import time
import subprocess
import platform
from pathlib import Path


def print_banner():
    print("\n" + "="*80)
    print(" "*20 + "BRZA ANALIZA PERFORMANSI V1 VS V2")
    print("="*80 + "\n")


def print_menu():
    print("\nODABIRE OPERACIJE:\n")
    print("1. Mere performansi (V1 vs V2)")
    print("2. Generiši dijagrame iz rezultata")
    print("3. Postavi Metabase dashboard")
    print("4. Sve (1+2+3) - PREPORUČENO")
    print("5. Samo prikazi rezultate (bez merenja)")
    print("0. Izlaz\n")


def check_prerequisites():
    print("─"*80)
    print("PROVERAVANJE PREDUSLOV")
    print("─"*80 + "\n")
    
    errors = []
    
    # Provera MongoDB
    print("Proverim MongoDB...")
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("✓ MongoDB je dostupan")
        
        db = client['SBP_DB']
        v1_count = db['movies'].count_documents({})
        v2_count = db['movies_optimized'].count_documents({})
        print(f"✓ V1 kolekcija: {v1_count} dokumenata")
        print(f"✓ V2 kolekcija: {v2_count} dokumenata")
        
        if v1_count == 0 or v2_count == 0:
            errors.append("Nedostaju podaci u kolekcijama!")
    except Exception as e:
        errors.append(f"MongoDB greška: {str(e)}")
    
    # Provera zavisnosti
    print("\nProveriim Python zavisnosti...")
    dependencies = ['pymongo', 'pandas', 'matplotlib', 'seaborn', 'requests']
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✓ {dep}")
        except ImportError:
            errors.append(f"Nedostaje: {dep}")
    
    print()
    
    if errors:
        print("✗ GREŠKE:\n")
        for error in errors:
            print(f"  • {error}")
        print("\n  Rešenja:")
        print("  - Za MongoDB: mongod")
        print("  - Za podatke: python v1/scripts/init_db.py i python v2/scripts/init_db.py")
        print("  - Za zavisnosti: pip install -r v2/requirements.txt\n")
        return False
    
    print("✓ Sve provjere su prošle!\n")
    return True


def run_performance_measurement():
    print("\n" + "─"*80)
    print("MERENJE PERFORMANSI (5 iteracija)")
    print("─"*80 + "\n")
    
    try:
        from pymongo import MongoClient
        from performance_comparison import PerformanceComparator
        
        client = MongoClient('mongodb://localhost:27017/')
        db = client['SBP_DB']
        
        print("Pokrenavam merenja (ovo može potrajati 2-3 minuta)...\n")
        
        comparator = PerformanceComparator(
            db['movies'],
            db['movies_optimized'],
            iterations=5
        )
        
        comparator.run_comparison()
        comparator.export_results('performance_results.json')
        
        print("\n✓ Performanse su izmerene i sačuvane u performance_results.json\n")
        return True
    except Exception as e:
        print(f"\n✗ Greška pri merenju: {str(e)}\n")
        return False


def run_visualization():
    print("\n" + "─"*80)
    print("GENERISANJE DIJAGRAMA")
    print("─"*80 + "\n")
    
    try:
        if not Path('performance_results.json').exists():
            print("✗ performance_results.json nije pronađen")
            print("  Prvo pokrenite merenje performansi (opcija 1)\n")
            return False
        
        from performance_visualizer import PerformanceVisualizer
        
        print("Generisem dijagrame...\n")
        
        visualizer = PerformanceVisualizer('performance_results.json')
        visualizer.create_all_charts()
        
        print("✓ Dijagrami su generisani u performance_charts/\n")
        return True
    except Exception as e:
        print(f"✗ Greška pri generisanju: {str(e)}\n")
        return False


def check_metabase_running():
    print("Proverim da li je Metabase pokrenuta...")
    
    try:
        import requests
        response = requests.get('http://localhost:3000/api/health', timeout=2)
        if response.status_code == 200:
            print("✓ Metabase je pokrenuta na http://localhost:3000\n")
            return True
    except:
        pass
    
    print("✗ Metabase nije dostupna")
    print("\nZa pokretanje Metabase:\n")
    print("  docker run -d -p 3000:3000 --name metabase metabase/metabase\n")
    print("  Čekajte ~30 sekundi da se pokrene.\n")
    
    response = input("Da li je Metabase već pokrenuta? (y/n): ").strip().lower()
    return response == 'y'


def setup_metabase_dashboard():
    print("\n" + "─"*80)
    print("METABASE DASHBOARD SETUP")
    print("─"*80 + "\n")
    
    if not check_metabase_running():
        return False
    
    try:
        from metabase_integration import MetabaseIntegration
        
        print("Povezujem se na Metabase...\n")
        
        metabase = MetabaseIntegration()
        metabase.setup_performance_dashboard()
        
        print("\n✓ Dashboard je uspešno kreiran!\n")
        print("Pristupite sa: http://localhost:3000/dashboard/1\n")
        return True
    except Exception as e:
        print(f"\n✗ Greška pri Metabase setupu: {str(e)}\n")
        return False


def display_results_summary():
    print("\n" + "─"*80)
    print("PRIKAZ REZULTATA")
    print("─"*80 + "\n")
    
    try:
        import json
        
        if not Path('performance_results.json').exists():
            print("✗ performance_results.json nije pronađen\n")
            return
        
        with open('performance_results.json', 'r') as f:
            results = json.load(f)
        
        print("PERFORMANSE PO UPITIMA:\n")
        
        total_v1 = 0
        total_v2 = 0
        
        for query, metrics in sorted(results.items()):
            print(f"{query}:")
            print(f"  V1:         {metrics['v1_avg_ms']:>8.2f}ms")
            print(f"  V2:         {metrics['v2_avg_ms']:>8.2f}ms")
            print(f"  Poboljšanje: {metrics['improvement_percent']:>7.1f}%")
            print(f"  Ubrzanje:    {metrics['speedup_factor']:>7.2f}x\n")
            
            total_v1 += metrics['v1_avg_ms']
            total_v2 += metrics['v2_avg_ms']
        
        avg_improvement = ((total_v1 - total_v2) / total_v1) * 100
        
        print("─" * 40)
        print(f"\nAGREGIRANA POBOLJŠANJA:\n")
        print(f"  Ukupno V1:              {total_v1:>8.2f}ms")
        print(f"  Ukupno V2:              {total_v2:>8.2f}ms")
        print(f"  Prosečno poboljšanje:   {avg_improvement:>7.1f}%")
        print(f"  Prosečan speedup:       {total_v1 / total_v2:>7.2f}x\n")
        
        print("DOSTUPNI DIJAGRAMI:\n")
        charts = [
            'performance_charts/execution_time_comparison.png',
            'performance_charts/improvement_percentage.png',
            'performance_charts/speedup_factor.png',
            'performance_charts/min_max_range.png',
            'performance_charts/summary_table.png'
        ]
        
        for chart in charts:
            if Path(chart).exists():
                print(f"  ✓ {chart}")
            else:
                print(f"  ✗ {chart}")
        
        print("\nDETALJAN IZVEŠTAJ: PERFORMANCE_ANALYSIS.md\n")
        
    except Exception as e:
        print(f"✗ Greška pri prikazu rezultata: {str(e)}\n")


def open_dashboard():
    print("\nDa li želite otvoriti Metabase dashboard? (y/n): ", end='')
    if input().strip().lower() == 'y':
        url = 'http://localhost:3000/dashboard/1'
        system = platform.system()
        
        try:
            if system == 'Windows':
                os.startfile(url)
            elif system == 'Darwin':
                subprocess.Popen(['open', url])
            elif system == 'Linux':
                subprocess.Popen(['xdg-open', url])
        except Exception as e:
            print(f"Nije moguće otvoriti browser: {str(e)}")
            print(f"Ručno otvorite: {url}")


def main():
    os.chdir('v2/scripts')
    
    print_banner()
    
    if not check_prerequisites():
        print("✗ Provjera preduslov je neuspešna")
        sys.exit(1)
    
    while True:
        print_menu()
        choice = input("Izaberite opciju: ").strip()
        
        if choice == '0':
            print("\nDo videnja!\n")
            break
        
        elif choice == '1':
            run_performance_measurement()
            input("Pritisni ENTER za nastavak...")
        
        elif choice == '2':
            run_visualization()
            input("Pritisni ENTER za nastavak...")
        
        elif choice == '3':
            setup_metabase_dashboard()
            open_dashboard()
            input("Pritisni ENTER za nastavak...")
        
        elif choice == '4':
            print("\n" + "="*80)
            print("KOMPLETAN WORKFLOW")
            print("="*80)
            
            if not run_performance_measurement():
                continue
            
            if not run_visualization():
                continue
            
            if not setup_metabase_dashboard():
                continue
            
            open_dashboard()
            
            print("\n" + "="*80)
            print("✓ SVI KORACI SU ZAVRŠENI!")
            print("="*80)
            print("\nRezultati:")
            print(f"  • Performanse: performance_results.json")
            print(f"  • Dijagrami: performance_charts/")
            print(f"  • Dashboard: http://localhost:3000/dashboard/1")
            print(f"  • Analiza: PERFORMANCE_ANALYSIS.md\n")
            
            input("Pritisni ENTER za nastavak...")
        
        elif choice == '5':
            display_results_summary()
            input("Pritisni ENTER za nastavak...")
        
        else:
            print("\n✗ Nepoznata opcija\n")
    
    print("="*80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✗ Prekinuto od strane korisnika\n")
        sys.exit(0)
