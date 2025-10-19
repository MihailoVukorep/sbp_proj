#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Performance Comparison Script - V1 vs V2 (Poboljšana verzija)
Izvršava sve upite na obe verzije i prikazuje detaljan poređenje performansi
"""

import time
import json
import os
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
from typing import Dict, List, Tuple
import statistics
import sys


class PerformanceComparator:
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/SBP_DB", output_folder: str = "performance_reports"):
        self.connection_string = connection_string
        self.output_folder = output_folder
        self.client = None
        self.db = None
        self.results = []
        
        # Kreiraj folder ako ne postoji
        self._create_output_folder()
    
    def _create_output_folder(self):
        """Kreira output folder ako ne postoji"""
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        print(f"✓ Output folder: {os.path.abspath(self.output_folder)}\n")
    
    def connect(self) -> bool:
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=3000)
            self.db = self.client["SBP_DB"]
            self.client.admin.command('ping')
            print("✓ Konekcija na MongoDB uspešna\n")
            return True
        except Exception as e:
            print(f"✗ Greška pri konekciji: {str(e)}\n")
            return False
    
    def run_query(self, query_pipeline: List[Dict], collection_name: str, num_runs: int = 3) -> Dict:
        """Izvršava query više puta i meri vreme izvršavanja"""
        times = []
        result_count = 0
        
        try:
            collection = self.db[collection_name]
            
            for run_num in range(num_runs):
                start_time = time.time()
                results = list(collection.aggregate(query_pipeline, allowDiskUse=True))
                elapsed_time = (time.time() - start_time) * 1000  # Konvertuj u ms
                times.append(elapsed_time)
                result_count = len(results)
            
            return {
                "times": times,
                "avg_time": statistics.mean(times) if times else 0,
                "min_time": min(times) if times else 0,
                "max_time": max(times) if times else 0,
                "result_count": result_count
            }
        except Exception as e:
            print(f"  ✗ Greška: {str(e)}")
            return {
                "times": [],
                "avg_time": -1,
                "min_time": -1,
                "max_time": -1,
                "result_count": 0
            }
    
    def execute_query_1(self) -> Dict:
        """Query 1: Top 10 Profitable Companies (Budget > 50M)"""
        print("\n" + "="*80)
        print("QUERY 1: Top 10 Profitable Companies (Budget > 50M)")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - direktni budget filter):")
        v1_pipeline = [
            {"$match": {"financial.budget": {"$gt": 50000000}, "financial.revenue": {"$gt": 0}}},
            {"$unwind": "$production.companies"},
            {"$group": {
                "_id": "$production.companies",
                "avg_revenue": {"$avg": "$financial.revenue"},
                "total_movies": {"$sum": 1},
                "total_revenue": {"$sum": "$financial.revenue"}
            }},
            {"$sort": {"avg_revenue": -1}},
            {"$limit": 10}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Min vreme: {v1_result['min_time']:.2f}ms")
        print(f"  Max vreme: {v1_result['max_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija
        print("\nV2 (Optimizovana - budget_category index):")
        v2_pipeline = [
            {"$match": {"financial.budget_category": {"$in": ["high", "blockbuster"]}}},
            {"$unwind": "$production.companies"},
            {"$group": {
                "_id": "$production.companies",
                "avg_revenue": {"$avg": "$financial.revenue"},
                "total_movies": {"$sum": 1},
                "total_revenue": {"$sum": "$financial.revenue"}
            }},
            {"$sort": {"avg_revenue": -1}},
            {"$limit": 10}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Min vreme: {v2_result['min_time']:.2f}ms")
        print(f"  Max vreme: {v2_result['max_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        # Poređenje
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 1: Top 10 Profitable Companies",
            "description": "Pronalaženje 10 najrentabilnijih produkcijskih kuća sa budžetom > 50M",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_2(self) -> Dict:
        """Query 2: Average Rating by Genre and Decade"""
        print("\n" + "="*80)
        print("QUERY 2: Average Rating by Genre and Decade")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - računanje decade u pipeline):")
        v1_pipeline = [
            {"$match": {"release_info.release_date.year": {"$exists": True, "$ne": None}, "ratings.vote_average": {"$gt": 0}}},
            {"$unwind": "$content_info.genres"},
            {"$addFields": {
                "decade": {"$multiply": [{"$floor": {"$divide": ["$release_info.release_date.year", 10]}}, 10]}
            }},
            {"$group": {
                "_id": {"genre": "$content_info.genres", "decade": "$decade"},
                "avg_rating": {"$avg": "$ratings.vote_average"},
                "movie_count": {"$sum": 1}
            }},
            {"$sort": {"_id.decade": 1}},
            {"$limit": 50}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija
        print("\nV2 (Optimizovana - precomputed decade field):")
        v2_pipeline = [
            {"$match": {"release_info.decade": {"$exists": True, "$ne": None}, "ratings.vote_average": {"$gt": 0}}},
            {"$unwind": "$content_info.genres"},
            {"$group": {
                "_id": {"genre": "$content_info.genres", "decade": "$release_info.decade"},
                "avg_rating": {"$avg": "$ratings.vote_average"},
                "movie_count": {"$sum": 1}
            }},
            {"$sort": {"_id.decade": 1}},
            {"$limit": 50}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        # Poređenje
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 2: Average Rating by Genre and Decade",
            "description": "Analiza prosečne ocene po žanru i deceniji",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_3(self) -> Dict:
        """Query 3: Blockbuster Movies by Month"""
        print("\n" + "="*80)
        print("QUERY 3: Blockbuster Movies by Month (Budget > 100M)")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - direktni budget filter i month extraction):")
        v1_pipeline = [
            {"$match": {"financial.budget": {"$gt": 100000000}, "release_info.release_date.month": {"$exists": True}}},
            {"$group": {
                "_id": "$release_info.release_date.month",
                "blockbuster_count": {"$sum": 1},
                "avg_budget": {"$avg": "$financial.budget"},
                "total_revenue": {"$sum": "$financial.revenue"}
            }},
            {"$sort": {"blockbuster_count": -1}}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija
        print("\nV2 (Optimizovana - budget_category i precomputed month):")
        v2_pipeline = [
            {"$match": {"financial.budget_category": "blockbuster", "release_info.month": {"$exists": True}}},
            {"$group": {
                "_id": "$release_info.month",
                "blockbuster_count": {"$sum": 1},
                "avg_budget": {"$avg": "$financial.budget"},
                "total_revenue": {"$sum": "$financial.revenue"}
            }},
            {"$sort": {"blockbuster_count": -1}}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        # Poređenje
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 3: Blockbuster Movies by Month",
            "description": "Analiza blockbuster premijera po mesecima",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_4(self) -> Dict:
        """Query 4: Most Profitable Genre Combinations"""
        print("\n" + "="*80)
        print("QUERY 4: Most Profitable Genre Combinations")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - kalkulacija profit/roi u pipeline):")
        v1_pipeline = [
            {"$match": {"financial.revenue": {"$gt": 0}, "financial.budget": {"$gt": 0}, "content_info.genres": {"$exists": True, "$ne": []}}},
            {"$addFields": {
                "profit": {"$subtract": ["$financial.revenue", "$financial.budget"]},
                "roi": {"$multiply": [{"$divide": [{"$subtract": ["$financial.revenue", "$financial.budget"]}, "$financial.budget"]}, 100]}
            }},
            {"$group": {
                "_id": "$content_info.genres",
                "avg_profit": {"$avg": "$profit"},
                "avg_roi": {"$avg": "$roi"},
                "movie_count": {"$sum": 1}
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$sort": {"avg_profit": -1}},
            {"$limit": 20}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija
        print("\nV2 (Optimizovana - precomputed profit/roi i genre_pairs):")
        v2_pipeline = [
            {"$match": {"financial.is_profitable": True, "content_info.genre_pairs": {"$exists": True, "$ne": []}}},
            {"$unwind": "$content_info.genre_pairs"},
            {"$group": {
                "_id": "$content_info.genre_pairs",
                "avg_profit": {"$avg": "$financial.profit"},
                "avg_roi": {"$avg": "$financial.roi"},
                "movie_count": {"$sum": 1}
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$sort": {"avg_roi": -1}},
            {"$limit": 20}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        # Poređenje
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 4: Most Profitable Genre Combinations",
            "description": "Pronalaženje najprofitabilnijih kombinacija žanrova",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_5(self) -> Dict:
        """Query 5: Average Runtime by Country (Rating > 7)"""
        print("\n" + "="*80)
        print("QUERY 5: Average Runtime by Country (Rating > 7)")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - runtime i quality calculation u pipeline):")
        v1_pipeline = [
            {"$match": {"ratings.vote_average": {"$gt": 7.0}}},
            {"$unwind": "$production.countries"},
            {"$group": {
                "_id": "$production.countries",
                "avg_runtime": {"$avg": "$content_info.runtime"},
                "movie_count": {"$sum": 1},
                "avg_rating": {"$avg": "$ratings.vote_average"}
            }},
            {"$match": {"movie_count": {"$gte": 100}}},
            {"$sort": {"avg_runtime": -1}},
            {"$limit": 20}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija
        print("\nV2 (Optimizovana - precomputed quality_tier index):")
        v2_pipeline = [
            {"$match": {"ratings.quality_tier": {"$in": ["good", "excellent"]}, "content_info.runtime": {"$gt": 0}}},
            {"$unwind": "$production.countries"},
            {"$group": {
                "_id": "$production.countries",
                "avg_runtime": {"$avg": "$content_info.runtime"},
                "movie_count": {"$sum": 1},
                "avg_rating": {"$avg": "$ratings.vote_average"}
            }},
            {"$match": {"movie_count": {"$gte": 100}}},
            {"$sort": {"avg_runtime": -1}},
            {"$limit": 20}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Broj izvršavanja: 3")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        # Poređenje
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 5: Average Runtime by Country (Rating > 7)",
            "description": "Analiza prosečnog trajanja filmova po zemlji sa ocenom > 7",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def generate_summary(self):
        """Generiše sumarizovani izveštaj"""
        print("\n" + "="*80)
        print("SUMARNI IZVEŠTAJ - PERFORMANCE POREĐENJE V1 vs V2")
        print("="*80 + "\n")
        
        print(f"{'Query':<5} {'V1 (ms)':<15} {'V2 (ms)':<15} {'Poboljšanje':<15} {'Ubrzanje':<10}")
        print("-" * 80)
        
        total_v1_time = 0
        total_v2_time = 0
        improvements = []
        speedups = []
        
        for result in self.results:
            v1_avg = result['v1']['avg_time']
            v2_avg = result['v2']['avg_time']
            improvement = result['improvement']
            speedup = result['speedup']
            
            total_v1_time += v1_avg
            total_v2_time += v2_avg
            improvements.append(improvement)
            speedups.append(speedup)
            
            query_num = self.results.index(result) + 1
            print(f"{query_num:<5} {v1_avg:<15.2f} {v2_avg:<15.2f} {improvement:<14.1f}% {speedup:<10.2f}x")
        
        print("-" * 80)
        
        total_improvement = ((total_v1_time - total_v2_time) / total_v1_time * 100) if total_v1_time > 0 else 0
        total_speedup = total_v1_time / total_v2_time if total_v2_time > 0 else 0
        avg_improvement = statistics.mean(improvements)
        avg_speedup = statistics.mean(speedups)
        
        print(f"UKUPNO {total_v1_time:<14.2f} {total_v2_time:<14.2f} {total_improvement:<14.1f}% {total_speedup:<10.2f}x")
        
        print(f"\n📊 STATISTIKA:")
        print(f"  Prosečno poboljšanje: {avg_improvement:.1f}%")
        print(f"  Minimalno poboljšanje: {min(improvements):.1f}%")
        print(f"  Maksimalno poboljšanje: {max(improvements):.1f}%")
        print(f"  Prosečno ubrzanje: {avg_speedup:.2f}x")
        print(f"  UKUPNO ubrzanje: {total_speedup:.2f}x")
        
        print(f"\n🎯 KLJUČNA POBOLJŠANJA V2 VERZIJE:")
        print(f"  ✓ Precomputed fields (decade, budget_category, quality_tier, profit, roi)")
        print(f"  ✓ Indexiranja na često korišćenim poljima")
        print(f"  ✓ Genre pairs precomputtion za brže agregacije")
        print(f"  ✓ Denormalizovani podaci (year, month sa top nivoa)")
        print(f"  ✓ Kategorisani nivoi kvaliteta")
        
        # Pripremi podatke za čuvanje
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        summary_data = {
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": timestamp,
            "total_queries": len(self.results),
            "total_v1_time_ms": round(total_v1_time, 2),
            "total_v2_time_ms": round(total_v2_time, 2),
            "total_improvement_percent": round(total_improvement, 1),
            "total_speedup": round(total_speedup, 2),
            "average_improvement": round(avg_improvement, 1),
            "average_speedup": round(avg_speedup, 2),
            "queries": [
                {
                    "name": r['query_name'],
                    "description": r['description'],
                    "v1_avg_ms": round(r['v1']['avg_time'], 2),
                    "v1_min_ms": round(r['v1']['min_time'], 2),
                    "v1_max_ms": round(r['v1']['max_time'], 2),
                    "v1_results": r['v1']['result_count'],
                    "v2_avg_ms": round(r['v2']['avg_time'], 2),
                    "v2_min_ms": round(r['v2']['min_time'], 2),
                    "v2_max_ms": round(r['v2']['max_time'], 2),
                    "v2_results": r['v2']['result_count'],
                    "improvement_percent": round(r['improvement'], 1),
                    "speedup": round(r['speedup'], 2)
                }
                for r in self.results
            ]
        }
        
        # Sačuvaj detailed report sa timestamp
        detailed_filename = f"performance_comparison_detailed_{timestamp}.json"
        detailed_path = os.path.join(self.output_folder, detailed_filename)
        
        with open(detailed_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        # Sačuvaj i "latest" verziju
        latest_path = os.path.join(self.output_folder, "performance_comparison_latest.json")
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Detaljan izveštaj sačuvan u:")
        print(f"  - {detailed_path}")
        print(f"  - {latest_path}")
        print(f"✓ Svi rezultati su dostupni u Metabase-u\n")
        
        return detailed_path, latest_path
    
    def run_all_comparisons(self):
        """Pokreni sve upite i generiši izveštaj"""
        print("\n" + "="*80)
        print("POKRETANJE PERFORMANCE KOMPARACIJE - V1 vs V2")
        print("="*80)
        print("\nSvi upiti će biti izvršeni 3 puta na obe verzije...")
        print("(Ovo može potrajati nekoliko minuta)\n")
        
        self.results = [
            self.execute_query_1(),
            self.execute_query_2(),
            self.execute_query_3(),
            self.execute_query_4(),
            self.execute_query_5()
        ]
        
        return self.generate_summary()


def main():
    # Možeš promeniti naziv foldera ako želiš
    output_folder = "output"
    
    comparator = PerformanceComparator(output_folder=output_folder)
    
    if not comparator.connect():
        print("Greška: Nije moguća konekcija na MongoDB")
        sys.exit(1)
    
    # Proveravamo dostupnost kolekcija
    print("Proveravajući dostupnost kolekcija...")
    collections = comparator.db.list_collection_names()
    print(f"Dostupne kolekcije: {collections}\n")
    
    if "movies" not in collections:
        print("✗ Greška: Kolekcija 'movies' (V1) nije dostupna")
        sys.exit(1)
    
    if "movies_optimized" not in collections:
        print("✗ Greška: Kolekcija 'movies_optimized' (V2) nije dostupna")
        sys.exit(1)
    
    # Prikaži info o kolekcijama
    v1_count = comparator.db['movies'].count_documents({})
    v2_count = comparator.db['movies_optimized'].count_documents({})
    
    print(f"✓ Kolekcija 'movies' (V1): {v1_count:,} dokumenta")
    print(f"✓ Kolekcija 'movies_optimized' (V2): {v2_count:,} dokumenta\n")
    
    detailed_path, latest_path = comparator.run_all_comparisons()


if __name__ == "__main__":
    main()