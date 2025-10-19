#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Performance Comparison Script - V1 vs V2 (Ispravljena verzija)
Izvršava sve upite na obe verzije i prikazuje poređenje performansi
"""

import time
import json
import os
from pymongo import MongoClient
from typing import Dict, List, Tuple
import statistics


class PerformanceComparator:
    
    def __init__(self, v1_collection, v2_collection, iterations=3):
        self.v1_collection = v1_collection
        self.v2_collection = v2_collection
        self.iterations = iterations
        self.results = {
            'query_1': {'v1': [], 'v2': []},
            'query_2': {'v1': [], 'v2': []},
            'query_3': {'v1': [], 'v2': []},
            'query_4': {'v1': [], 'v2': []},
            'query_5': {'v1': [], 'v2': []}
        }
    
    def measure_query(self, collection, query_func, query_name: str, version: str):
        """Meri vreme izvršavanja query-ja"""
        times = []
        for i in range(self.iterations):
            try:
                start = time.time()
                result = list(query_func(collection))
                end = time.time()
                times.append((end - start) * 1000)
            except Exception as e:
                print(f"Error in {query_name} ({version}, iteration {i+1}): {str(e)}")
                times.append(None)
        
        return times
    
    # QUERY 1: Top 10 Profitable Companies
    def query_1_v1(self, collection):
        """V1 - Direktni budget filter"""
        return collection.aggregate([
            {'$match': {
                'financial.budget': {'$gt': 50000000}, 
                'financial.revenue': {'$gt': 0}
            }},
            {'$unwind': '$production.companies'},
            {'$group': {
                '_id': '$production.companies',
                'avg_revenue': {'$avg': '$financial.revenue'},
                'total_movies': {'$sum': 1},
                'total_revenue': {'$sum': '$financial.revenue'}
            }},
            {'$sort': {'avg_revenue': -1}},
            {'$limit': 10}
        ], allowDiskUse=True)
    
    def query_1_v2(self, collection):
        """V2 - Budget category index"""
        return collection.aggregate([
            {'$match': {
                'financial.budget_category': {'$in': ['high', 'blockbuster']}
            }},
            {'$unwind': '$production.companies'},
            {'$group': {
                '_id': '$production.companies',
                'avg_revenue': {'$avg': '$financial.revenue'},
                'total_movies': {'$sum': 1},
                'total_revenue': {'$sum': '$financial.revenue'}
            }},
            {'$sort': {'avg_revenue': -1}},
            {'$limit': 10}
        ], allowDiskUse=True)
    
    # QUERY 2: Average Rating by Genre and Decade
    def query_2_v1(self, collection):
        """V1 - Računanje decade u pipeline"""
        return collection.aggregate([
            {'$match': {
                'release_info.release_date.year': {'$exists': True, '$ne': None}, 
                'ratings.vote_average': {'$gt': 0}
            }},
            {'$unwind': '$content_info.genres'},
            {'$addFields': {
                'decade': {
                    '$multiply': [
                        {'$floor': {'$divide': ['$release_info.release_date.year', 10]}}, 
                        10
                    ]
                }
            }},
            {'$group': {
                '_id': {
                    'genre': '$content_info.genres', 
                    'decade': '$decade'
                },
                'avg_rating': {'$avg': '$ratings.vote_average'},
                'movie_count': {'$sum': 1}
            }},
            {'$sort': {'_id.decade': 1}},
            {'$limit': 50}
        ], allowDiskUse=True)
    
    def query_2_v2(self, collection):
        """V2 - Precomputed decade field"""
        return collection.aggregate([
            {'$match': {
                'release_info.decade': {'$exists': True, '$ne': None}, 
                'ratings.vote_average': {'$gt': 0}
            }},
            {'$unwind': '$content_info.genres'},
            {'$group': {
                '_id': {
                    'genre': '$content_info.genres', 
                    'decade': '$release_info.decade'
                },
                'avg_rating': {'$avg': '$ratings.vote_average'},
                'movie_count': {'$sum': 1}
            }},
            {'$sort': {'_id.decade': 1}},
            {'$limit': 50}
        ], allowDiskUse=True)
    
    # QUERY 3: Blockbuster Movies by Month
    def query_3_v1(self, collection):
        """V1 - Direktni budget filter i month extraction"""
        return collection.aggregate([
            {'$match': {
                'financial.budget': {'$gt': 100000000}, 
                'release_info.release_date.month': {'$exists': True}
            }},
            {'$group': {
                '_id': '$release_info.release_date.month',
                'blockbuster_count': {'$sum': 1},
                'avg_budget': {'$avg': '$financial.budget'},
                'total_revenue': {'$sum': '$financial.revenue'}
            }},
            {'$sort': {'blockbuster_count': -1}}
        ], allowDiskUse=True)
    
    def query_3_v2(self, collection):
        """V2 - Budget category i precomputed month"""
        return collection.aggregate([
            {'$match': {
                'financial.budget_category': 'blockbuster', 
                'release_info.month': {'$exists': True}
            }},
            {'$group': {
                '_id': '$release_info.month',
                'blockbuster_count': {'$sum': 1},
                'avg_budget': {'$avg': '$financial.budget'},
                'total_revenue': {'$sum': '$financial.revenue'}
            }},
            {'$sort': {'blockbuster_count': -1}}
        ], allowDiskUse=True)
    
    # QUERY 4: Most Profitable Genre Combinations
    def query_4_v1(self, collection):
        """V1 - Kalkulacija profit/roi u pipeline"""
        return collection.aggregate([
            {'$match': {
                'financial.revenue': {'$gt': 0}, 
                'financial.budget': {'$gt': 0}, 
                'content_info.genres': {'$exists': True, '$ne': []}
            }},
            {'$addFields': {
                'profit': {'$subtract': ['$financial.revenue', '$financial.budget']},
                'roi': {
                    '$multiply': [
                        {'$divide': [
                            {'$subtract': ['$financial.revenue', '$financial.budget']}, 
                            '$financial.budget'
                        ]}, 
                        100
                    ]
                }
            }},
            {'$group': {
                '_id': '$content_info.genres',
                'avg_profit': {'$avg': '$profit'},
                'avg_roi': {'$avg': '$roi'},
                'movie_count': {'$sum': 1}
            }},
            {'$match': {'movie_count': {'$gte': 10}}},
            {'$sort': {'avg_profit': -1}},
            {'$limit': 20}
        ], allowDiskUse=True)
    
    def query_4_v2(self, collection):
        """V2 - Precomputed profit/roi i genre_pairs"""
        return collection.aggregate([
            {'$match': {
                'financial.is_profitable': True, 
                'content_info.genre_pairs': {'$exists': True, '$ne': []}
            }},
            {'$unwind': '$content_info.genre_pairs'},
            {'$group': {
                '_id': '$content_info.genre_pairs',
                'avg_profit': {'$avg': '$financial.profit'},
                'avg_roi': {'$avg': '$financial.roi'},
                'movie_count': {'$sum': 1}
            }},
            {'$match': {'movie_count': {'$gte': 10}}},
            {'$sort': {'avg_roi': -1}},
            {'$limit': 20}
        ], allowDiskUse=True)
    
    # QUERY 5: Average Runtime by Country
    def query_5_v1(self, collection):
        """V1 - Runtime i quality calculation u pipeline"""
        return collection.aggregate([
            {'$match': {'ratings.vote_average': {'$gt': 7.0}}},
            {'$unwind': '$production.countries'},
            {'$group': {
                '_id': '$production.countries',
                'avg_runtime': {'$avg': '$content_info.runtime'},
                'movie_count': {'$sum': 1},
                'avg_rating': {'$avg': '$ratings.vote_average'}
            }},
            {'$match': {'movie_count': {'$gte': 100}}},
            {'$sort': {'avg_runtime': -1}},
            {'$limit': 20}
        ], allowDiskUse=True)
    
    def query_5_v2(self, collection):
        """V2 - Precomputed quality_tier index"""
        return collection.aggregate([
            {'$match': {
                'ratings.quality_tier': {'$in': ['good', 'excellent']}, 
                'content_info.runtime': {'$gt': 0}
            }},
            {'$unwind': '$production.countries'},
            {'$group': {
                '_id': '$production.countries',
                'avg_runtime': {'$avg': '$content_info.runtime'},
                'movie_count': {'$sum': 1},
                'avg_rating': {'$avg': '$ratings.vote_average'}
            }},
            {'$match': {'movie_count': {'$gte': 100}}},
            {'$sort': {'avg_runtime': -1}},
            {'$limit': 20}
        ], allowDiskUse=True)
    
    def run_comparison(self):
        """Izvršava sve upite i poredi performanse"""
        print("\n" + "="*70)
        print("PERFORMANCE COMPARISON: V1 vs V2")
        print("="*70)
        
        queries = [
            ('query_1', self.query_1_v1, self.query_1_v2, 'Top 10 Profitable Companies'),
            ('query_2', self.query_2_v1, self.query_2_v2, 'Average Rating by Genre/Decade'),
            ('query_3', self.query_3_v1, self.query_3_v2, 'Blockbuster Movies by Month'),
            ('query_4', self.query_4_v1, self.query_4_v2, 'Most Profitable Genre Combinations'),
            ('query_5', self.query_5_v1, self.query_5_v2, 'Average Runtime by Country'),
        ]
        
        for query_name, query_v1_func, query_v2_func, description in queries:
            print(f"\n{query_name.upper()}: {description}")
            print("-" * 70)
            
            # Meri V1
            print(f"V1 (Originalna verzija):")
            v1_times = self.measure_query(self.v1_collection, query_v1_func, query_name, "V1")
            
            # Meri V2
            print(f"V2 (Optimizovana verzija):")
            v2_times = self.measure_query(self.v2_collection, query_v2_func, query_name, "V2")
            
            # Sačuvaj rezultate
            self.results[query_name]['v1'] = [t for t in v1_times if t is not None]
            self.results[query_name]['v2'] = [t for t in v2_times if t is not None]
            
            # Prikaži statistiku
            if self.results[query_name]['v1'] and self.results[query_name]['v2']:
                v1_avg = statistics.mean(self.results[query_name]['v1'])
                v2_avg = statistics.mean(self.results[query_name]['v2'])
                v1_min = min(self.results[query_name]['v1'])
                v1_max = max(self.results[query_name]['v1'])
                v2_min = min(self.results[query_name]['v2'])
                v2_max = max(self.results[query_name]['v2'])
                
                improvement = ((v1_avg - v2_avg) / v1_avg) * 100
                speedup = v1_avg / v2_avg
                
                print(f"\n  V1: avg={v1_avg:.2f}ms, min={v1_min:.2f}ms, max={v1_max:.2f}ms")
                print(f"  V2: avg={v2_avg:.2f}ms, min={v2_min:.2f}ms, max={v2_max:.2f}ms")
                print(f"  📊 Poboljšanje: {improvement:.1f}%")
                print(f"  ⏱️  Ubrzanje: {speedup:.2f}x brže")
            else:
                print("  ✗ Error: Could not measure times")
        
        return self.results
    
    def get_summary(self) -> Dict:
        """Generiše sumarnu statistiku"""
        summary = {}
        
        for query_name, times in self.results.items():
            v1_times = times['v1']
            v2_times = times['v2']
            
            if v1_times and v2_times:
                v1_avg = statistics.mean(v1_times)
                v2_avg = statistics.mean(v2_times)
                
                summary[query_name] = {
                    'v1_avg_ms': round(v1_avg, 2),
                    'v2_avg_ms': round(v2_avg, 2),
                    'improvement_percent': round(((v1_avg - v2_avg) / v1_avg) * 100, 1),
                    'speedup_factor': round(v1_avg / v2_avg, 2),
                    'v1_min_ms': round(min(v1_times), 2),
                    'v1_max_ms': round(max(v1_times), 2),
                    'v2_min_ms': round(min(v2_times), 2),
                    'v2_max_ms': round(max(v2_times), 2),
                }
        
        return summary
    
    def export_results(self, filepath: str):
        """Eksportuje rezultate u JSON"""
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
        summary = self.get_summary()
        
        # Dodaj ukupnu statistiku
        total_v1 = sum(q['v1_avg_ms'] for q in summary.values())
        total_v2 = sum(q['v2_avg_ms'] for q in summary.values())
        
        output = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'iterations': self.iterations,
            'queries': summary,
            'totals': {
                'total_v1_ms': round(total_v1, 2),
                'total_v2_ms': round(total_v2, 2),
                'total_improvement_percent': round(((total_v1 - total_v2) / total_v1) * 100, 1),
                'total_speedup': round(total_v1 / total_v2, 2)
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Results exported to: {filepath}")


if __name__ == "__main__":
    # Konekcija na MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['SBP_DB']
    
    v1_collection = db['movies']
    v2_collection = db['movies_optimized']
    
    # Proveri kolekcije
    print("\nProveravajući kolekcije...")
    v1_count = v1_collection.count_documents({})
    v2_count = v2_collection.count_documents({})
    
    print(f"✓ V1 (movies): {v1_count:,} dokumenta")
    print(f"✓ V2 (movies_optimized): {v2_count:,} dokumenta")
    
    # Pokreni poređenje
    comparator = PerformanceComparator(v1_collection, v2_collection, iterations=3)
    comparator.run_comparison()
    
    # Prikaži summary
    summary = comparator.get_summary()
    print("\n" + "="*70)
    print("SUMARNI IZVEŠTAJ")
    print("="*70)
    
    total_v1 = 0
    total_v2 = 0
    
    for query, metrics in summary.items():
        print(f"\n{query}:")
        print(f"  V1: {metrics['v1_avg_ms']}ms")
        print(f"  V2: {metrics['v2_avg_ms']}ms")
        print(f"  Poboljšanje: {metrics['improvement_percent']}%")
        print(f"  Ubrzanje: {metrics['speedup_factor']}x")
        
        total_v1 += metrics['v1_avg_ms']
        total_v2 += metrics['v2_avg_ms']
    
    total_improvement = ((total_v1 - total_v2) / total_v1) * 100
    total_speedup = total_v1 / total_v2
    
    print("\n" + "="*70)
    print(f"UKUPNO:")
    print(f"  V1: {total_v1:.2f}ms")
    print(f"  V2: {total_v2:.2f}ms")
    print(f"  Ukupno poboljšanje: {total_improvement:.1f}%")
    print(f"  Ukupno ubrzanje: {total_speedup:.2f}x")
    
    # Eksportuj rezultate
    comparator.export_results('output/performance_results.json')