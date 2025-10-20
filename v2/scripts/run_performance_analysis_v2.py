#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Performance Comparison Script - V1 vs V2 (CORRECTED VERSION)
Koristi tačne queryje iz optimizacionog dokumenta
"""

import time
import json
import os
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
from typing import Dict, List
import statistics
import sys


class PerformanceComparator:
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/SBP_DB", output_folder: str = "performance_reports"):
        self.connection_string = connection_string
        self.output_folder = output_folder
        self.client = None
        self.db = None
        self.results = []
        
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
                elapsed_time = (time.time() - start_time) * 1000
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
        """Query 1: Average revenue per company (budget > 50M)"""
        print("\n" + "="*80)
        print("QUERY 1: Average revenue per company (budget > 50M)")
        print("="*80)
        
        # V1 verzija - direktni budget filter
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
            {"$limit": 20}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija - CORRECTED: budget_category + avg_roi
        print("\nV2 (Optimizovana - budget_category index):")
        v2_pipeline = [
            {"$match": {"financial.budget_category": {"$in": ["high", "blockbuster"]}}},
            {"$unwind": "$production.companies"},
            {"$group": {
                "_id": "$production.companies",
                "avg_revenue": {"$avg": "$financial.revenue"},
                "total_movies": {"$sum": 1},
                "total_revenue": {"$sum": "$financial.revenue"},
                "avg_roi": {"$avg": "$financial.roi"}
            }},
            {"$sort": {"avg_revenue": -1}},
            {"$limit": 20}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 1: Average revenue per company (budget > 50M)",
            "description": "Prosečan prihod po kompaniji sa budžetom > 50M",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_2(self) -> Dict:
        """Query 2: Average rating per genre by decade"""
        print("\n" + "="*80)
        print("QUERY 2: Average rating per genre by decade")
        print("="*80)
        
        # V1 verzija - računanje decade u pipeline
        print("\nV1 (Originalna - decade calculation in pipeline):")
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
            {"$sort": {"_id.decade": 1, "_id.genre": 1}}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija - CORRECTED: precomputed decade field
        print("\nV2 (Optimizovana - precomputed decade):")
        v2_pipeline = [
            {"$match": {"release_info.decade": {"$exists": True, "$ne": None}, "ratings.vote_average": {"$gt": 0}}},
            {"$unwind": "$content_info.genres"},
            {"$group": {
                "_id": {"genre": "$content_info.genres", "decade": "$release_info.decade"},
                "avg_rating": {"$avg": "$ratings.vote_average"},
                "movie_count": {"$sum": 1}
            }},
            {"$sort": {"_id.decade": 1, "_id.genre": 1}}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 2: Average rating per genre by decade",
            "description": "Prosečna ocena po žanru i deceniji",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_3(self) -> Dict:
        """Query 3: Months with most blockbuster movies (budget > 100M)"""
        print("\n" + "="*80)
        print("QUERY 3: Months with most blockbuster movies (budget > 100M)")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - direct budget filter):")
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
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija - CORRECTED: budget_category + denormalized month
        print("\nV2 (Optimizovana - budget_category + denormalized month):")
        v2_pipeline = [
            {"$match": {"financial.budget_category": "blockbuster", "release_info.month": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$release_info.month",
                "blockbuster_count": {"$sum": 1},
                "avg_budget": {"$avg": "$financial.budget"},
                "total_revenue": {"$sum": "$financial.revenue"}
            }},
            {"$sort": {"blockbuster_count": -1}}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 3: Months with most blockbuster movies",
            "description": "Meseci sa najviše blockbuster filmova",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_4(self) -> Dict:
        """Query 4: Most profitable genre combinations"""
        print("\n" + "="*80)
        print("QUERY 4: Most profitable genre combinations")
        print("="*80)
        
        # V1 verzija - računanje profit/roi u pipeline
        print("\nV1 (Originalna - profit/roi calculation in pipeline):")
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
                "total_profit": {"$sum": "$profit"},
                "movie_count": {"$sum": 1}
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$sort": {"avg_roi": -1}},
            {"$limit": 20}
        ]
        
        v1_result = self.run_query(v1_pipeline, "movies")
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija - CORRECTED: precomputed genre_pairs + profit/roi
        print("\nV2 (Optimizovana - precomputed genre_pairs + profit/roi):")
        v2_pipeline = [
            {"$match": {"financial.is_profitable": True, "content_info.genre_pairs": {"$exists": True, "$ne": []}}},
            {"$unwind": "$content_info.genre_pairs"},
            {"$group": {
                "_id": "$content_info.genre_pairs",
                "avg_profit": {"$avg": "$financial.profit"},
                "avg_roi": {"$avg": "$financial.roi"},
                "total_profit": {"$sum": "$financial.profit"},
                "movie_count": {"$sum": 1}
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$sort": {"avg_roi": -1}},
            {"$limit": 20}
        ]
        
        v2_result = self.run_query(v2_pipeline, "movies_optimized")
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 4: Most profitable genre combinations",
            "description": "Najprofitabilnije kombinacije žanrova",
            "v1": v1_result,
            "v2": v2_result,
            "improvement": improvement,
            "speedup": speedup
        }
    
    def execute_query_5(self) -> Dict:
        """Query 5: Average runtime per country (rating > 7, 100+ movies)"""
        print("\n" + "="*80)
        print("QUERY 5: Average runtime per country (rating > 7, 100+ movies)")
        print("="*80)
        
        # V1 verzija
        print("\nV1 (Originalna - direct rating filter):")
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
        print(f"  Prosečno vreme: {v1_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v1_result['result_count']}")
        
        # V2 verzija - CORRECTED: quality_tier = "excellent" (rating > 8)
        print("\nV2 (Optimizovana - quality_tier index):")
        v2_pipeline = [
            {"$match": {"ratings.quality_tier": "excellent", "content_info.runtime": {"$gt": 0}}},
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
        print(f"  Prosečno vreme: {v2_result['avg_time']:.2f}ms")
        print(f"  Rezultata: {v2_result['result_count']}")
        
        improvement = ((v1_result['avg_time'] - v2_result['avg_time']) / v1_result['avg_time'] * 100) if v1_result['avg_time'] > 0 else 0
        speedup = v1_result['avg_time'] / v2_result['avg_time'] if v2_result['avg_time'] > 0 else 0
        
        print(f"\n  📊 POBOLJŠANJE: {improvement:.1f}%")
        print(f"  ⏱️  UBRZANJE: {speedup:.2f}x brže")
        
        return {
            "query_name": "Query 5: Average runtime per country (rating > 7)",
            "description": "Prosečno trajanje po zemlji sa ocenom > 7",
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
        
        # Sačuvaj izveštaj
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        summary_data = {
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": timestamp,
            "total_queries": len(self.results),
            "total_v1_time_ms": round(total_v1_time, 2),
            "total_v2_time_ms": round(total_v2_time, 2),
            "total_improvement_percent": round(total_improvement, 1),
            "total_speedup": round(total_speedup, 2),
            "queries": [
                {
                    "name": r['query_name'],
                    "description": r['description'],
                    "v1_avg_ms": round(r['v1']['avg_time'], 2),
                    "v2_avg_ms": round(r['v2']['avg_time'], 2),
                    "improvement_percent": round(r['improvement'], 1),
                    "speedup": round(r['speedup'], 2)
                }
                for r in self.results
            ]
        }
        
        output_filename = f"performance_comparison_detailed.json"
        output_path = os.path.join(self.output_folder, output_filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        

        
        print(f"\n✓ Izveštaj sačuvan u:")
        print(f"  - {output_path}")
        
        return output_path
    
    def run_all_comparisons(self):
        """Pokreni sve upite i generiši izveštaj"""
        print("\n" + "="*80)
        print("POKRETANJE PERFORMANCE KOMPARACIJE - V1 vs V2")
        print("="*80)
        print("\nSvi upiti će biti izvršeni 3 puta na obe verzije...\n")
        
        self.results = [
            self.execute_query_1(),
            self.execute_query_2(),
            self.execute_query_3(),
            self.execute_query_4(),
            self.execute_query_5()
        ]
        
        return self.generate_summary()


def main():
    output_folder = "output"
    
    comparator = PerformanceComparator(output_folder=output_folder)
    
    if not comparator.connect():
        print("Greška: Nije moguća konekcija na MongoDB")
        sys.exit(1)
    
    print("Proveravajući dostupnost kolekcija...")
    collections = comparator.db.list_collection_names()
    print(f"Dostupne kolekcije: {collections}\n")
    
    if "movies" not in collections:
        print("✗ Greška: Kolekcija 'movies' (V1) nije dostupna")
        sys.exit(1)
    
    if "movies_optimized" not in collections:
        print("✗ Greška: Kolekcija 'movies_optimized' (V2) nije dostupna")
        sys.exit(1)
    
    v1_count = comparator.db['movies'].count_documents({})
    v2_count = comparator.db['movies_optimized'].count_documents({})
    
    print(f"✓ Kolekcija 'movies' (V1): {v1_count:,} dokumenta")
    print(f"✓ Kolekcija 'movies_optimized' (V2): {v2_count:,} dokumenta\n")
    
    comparator.run_all_comparisons()


if __name__ == "__main__":
    main()