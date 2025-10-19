import time
import json
import statistics
from pymongo import MongoClient
from typing import Dict, List, Tuple
import traceback


class PerformanceComparator:
    
    def __init__(self, v1_collection, v2_collection, iterations=5):
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
    
    def query_1_v1(self, collection):
        return collection.aggregate([
            {'$match': {'revenue': {'$gte': 100000000}}},
            {'$unwind': '$production_companies'},
            {'$group': {
                '_id': '$production_companies.name',
                'total_revenue': {'$sum': '$revenue'},
                'avg_budget': {'$avg': '$budget'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'total_revenue': -1}},
            {'$limit': 10}
        ])
    
    def query_1_v2(self, collection):
        return collection.aggregate([
            {'$match': {'budget_category': {'$in': ['High', 'Ultra-High']}}},
            {'$unwind': '$companies'},
            {'$group': {
                '_id': '$companies.name',
                'total_revenue': {'$sum': '$revenue'},
                'avg_budget': {'$avg': '$budget'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'total_revenue': -1}},
            {'$limit': 10}
        ])
    
    def query_2_v1(self, collection):
        return collection.aggregate([
            {'$addFields': {
                'decade': {
                    '$cond': [
                        {'$eq': [{'$substr': ['$release_date', 0, 4]}, '']},
                        0,
                        {'$toInt': {'$substr': ['$release_date', 0, 3]}}
                    ]
                }
            }},
            {'$unwind': '$genres'},
            {'$match': {'genres.name': {'$in': ['Action', 'Comedy', 'Drama', 'Horror', 'Thriller']}}},
            {'$group': {
                '_id': {'genre': '$genres.name', 'decade': '$decade'},
                'avg_vote': {'$avg': '$vote_average'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id.decade': -1, 'avg_vote': -1}},
            {'$limit': 20}
        ])
    
    def query_2_v2(self, collection):
        return collection.aggregate([
            {'$unwind': '$genres'},
            {'$match': {
                'genres.name': {'$in': ['Action', 'Comedy', 'Drama', 'Horror', 'Thriller']},
                'decade': {'$exists': True}
            }},
            {'$group': {
                '_id': {'genre': '$genres.name', 'decade': '$decade'},
                'avg_vote': {'$avg': '$vote_average'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id.decade': -1, 'avg_vote': -1}},
            {'$limit': 20}
        ])
    
    def query_3_v1(self, collection):
        return collection.aggregate([
            {'$match': {'budget': {'$gte': 50000000, '$lte': 150000000}}},
            {'$addFields': {
                'release_month': {
                    '$cond': [
                        {'$eq': [{'$substr': ['$release_date', 5, 2]}, '']},
                        0,
                        {'$toInt': {'$substr': ['$release_date', 5, 2]}}
                    ]
                }
            }},
            {'$group': {
                '_id': {'month': '$release_month'},
                'count': {'$sum': 1},
                'avg_vote': {'$avg': '$vote_average'},
                'avg_revenue': {'$avg': '$revenue'}
            }},
            {'$sort': {'_id.month': 1}},
            {'$limit': 12}
        ])
    
    def query_3_v2(self, collection):
        return collection.aggregate([
            {'$match': {'budget_category': 'Medium'}},
            {'$group': {
                '_id': {'month': '$release_month'},
                'count': {'$sum': 1},
                'avg_vote': {'$avg': '$vote_average'},
                'avg_revenue': {'$avg': '$revenue'}
            }},
            {'$sort': {'_id.month': 1}},
            {'$limit': 12}
        ])
    
    def query_4_v1(self, collection):
        movies = list(collection.find(
            {'genres': {'$size': 2}},
            {'genres': 1, 'revenue': 1, 'vote_average': 1}
        ).limit(5000))
        
        genre_pairs = {}
        for movie in movies:
            if len(movie.get('genres', [])) == 2:
                pair = tuple(sorted([g['name'] for g in movie['genres']]))
                if pair not in genre_pairs:
                    genre_pairs[pair] = {'revenue': [], 'votes': []}
                genre_pairs[pair]['revenue'].append(movie.get('revenue', 0))
                genre_pairs[pair]['votes'].append(movie.get('vote_average', 0))
        
        results = []
        for pair, data in sorted(genre_pairs.items(), 
                                key=lambda x: sum(x[1]['revenue']) or 0, 
                                reverse=True)[:10]:
            results.append({
                'genres': list(pair),
                'total_revenue': sum(data['revenue']),
                'avg_vote': statistics.mean(data['votes']) if data['votes'] else 0,
                'count': len(data['votes'])
            })
        return results
    
    def query_4_v2(self, collection):
        return collection.aggregate([
            {'$match': {'genre_pairs': {'$exists': True, '$ne': []}}},
            {'$unwind': '$genre_pairs'},
            {'$group': {
                '_id': '$genre_pairs',
                'total_revenue': {'$sum': '$revenue'},
                'avg_vote': {'$avg': '$vote_average'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'total_revenue': -1}},
            {'$limit': 10}
        ])
    
    def query_5_v1(self, collection):
        return collection.aggregate([
            {'$match': {'vote_average': {'$gte': 7.0, '$lte': 9.0}}},
            {'$unwind': '$spoken_languages'},
            {'$group': {
                '_id': {
                    'country': '$spoken_languages',
                    'runtime_range': {
                        '$cond': [
                            {'$lte': ['$runtime', 90]},
                            'Short',
                            {'$cond': [
                                {'$lte': ['$runtime', 120]},
                                'Standard',
                                'Long'
                            ]}
                        ]
                    }
                },
                'count': {'$sum': 1},
                'avg_runtime': {'$avg': '$runtime'},
                'avg_vote': {'$avg': '$vote_average'}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 15}
        ])
    
    def query_5_v2(self, collection):
        return collection.aggregate([
            {'$match': {'quality_tier': {'$in': ['Good', 'Excellent']}}},
            {'$unwind': '$countries'},
            {'$group': {
                '_id': {
                    'country': '$countries',
                    'runtime_range': '$runtime_range'
                },
                'count': {'$sum': 1},
                'avg_runtime': {'$avg': '$runtime'},
                'avg_vote': {'$avg': '$vote_average'}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 15}
        ])
    
    def run_comparison(self):
        print("\n" + "="*70)
        print("PERFORMANCE COMPARISON: V1 vs V2")
        print("="*70)
        
        queries = [
            ('query_1', self.query_1_v1, self.query_1_v2),
            ('query_2', self.query_2_v1, self.query_2_v2),
            ('query_3', self.query_3_v1, self.query_3_v2),
            ('query_4', self.query_4_v1, self.query_4_v2),
            ('query_5', self.query_5_v1, self.query_5_v2),
        ]
        
        for query_name, query_v1_func, query_v2_func in queries:
            print(f"\n{query_name.upper()}:")
            print("-" * 70)
            
            v1_times = self.measure_query(self.v1_collection, query_v1_func, query_name, "V1")
            v2_times = self.measure_query(self.v2_collection, query_v2_func, query_name, "V2")
            
            self.results[query_name]['v1'] = [t for t in v1_times if t is not None]
            self.results[query_name]['v2'] = [t for t in v2_times if t is not None]
            
            if self.results[query_name]['v1'] and self.results[query_name]['v2']:
                v1_avg = statistics.mean(self.results[query_name]['v1'])
                v2_avg = statistics.mean(self.results[query_name]['v2'])
                improvement = ((v1_avg - v2_avg) / v1_avg) * 100
                
                print(f"V1 (avg): {v1_avg:.2f}ms | V2 (avg): {v2_avg:.2f}ms")
                print(f"Improvement: {improvement:.1f}%")
                print(f"Speedup: {v1_avg/v2_avg:.2f}x")
            else:
                print("Error: Could not measure times")
        
        return self.results
    
    def get_summary(self) -> Dict:
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
        summary = self.get_summary()
        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nResults exported to: {filepath}")


if __name__ == "__main__":
    client = MongoClient('mongodb://localhost:27017/')
    db = client['SBP_DB']
    
    v1_collection = db['movies']
    v2_collection = db['movies_optimized']
    
    comparator = PerformanceComparator(v1_collection, v2_collection, iterations=5)
    comparator.run_comparison()
    
    summary = comparator.get_summary()
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    for query, metrics in summary.items():
        print(f"\n{query}:")
        print(f"  V1: {metrics['v1_avg_ms']}ms")
        print(f"  V2: {metrics['v2_avg_ms']}ms")
        print(f"  Improvement: {metrics['improvement_percent']}%")
        print(f"  Speedup: {metrics['speedup_factor']}x")
    
    comparator.export_results('performance_results.json')
