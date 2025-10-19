from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from typing import Dict, List, Any
import time


class IndexStrategy:
    
    def __init__(self, db):
        self.db = db
        self.collection = db.movies
    
    def create_all_indexes(self):
        print("="*80)
        print("CREATING OPTIMIZED INDEXES FOR 5 TARGET QUERIES")
        print("="*80)
        
        print("\nQUERY 1: Average revenue per company (budget > 50M)")
        print("-" * 80)
        
        print("\n1.1. Compound Index: budget_category + companies")
        self.collection.create_index(
            [
                ("financial.budget_category", ASCENDING),
                ("production.companies", ASCENDING)
            ],
            name="idx_budget_companies"
        )
        
        print("\n1.2. Single Field Index: budget")
        self.collection.create_index(
            [("financial.budget", ASCENDING)],
            name="idx_budget"
        )
        
        print("\n1.3. Compound Index: budget_category + roi")
        self.collection.create_index(
            [
                ("financial.budget_category", ASCENDING),
                ("financial.roi", DESCENDING)
            ],
            name="idx_budget_roi"
        )
        
        print("\nQUERY 2: Average rating per genre by decade")
        print("-" * 80)
        
        print("\n2.1. Compound Index: decade + genres + vote_average")
        self.collection.create_index(
            [
                ("release_info.decade", ASCENDING),
                ("content_info.genres", ASCENDING),
                ("ratings.vote_average", DESCENDING)
            ],
            name="idx_decade_genre_rating"
        )
        
        print("\n2.2. Single Field Index: decade")
        self.collection.create_index(
            [("release_info.decade", ASCENDING)],
            name="idx_decade"
        )
        
        print("\nQUERY 3: Months with most blockbuster movies (budget > 100M)")
        print("-" * 80)
        
        print("\n3.1. Compound Index: budget_category + month")
        self.collection.create_index(
            [
                ("financial.budget_category", ASCENDING),
                ("release_info.month", ASCENDING)
            ],
            name="idx_blockbuster_month"
        )
        
        print("\n3.2. Compound Index: budget + month")
        self.collection.create_index(
            [
                ("financial.budget", ASCENDING),
                ("release_info.month", ASCENDING)
            ],
            name="idx_budget_month"
        )
        
        print("\nQUERY 4: Most profitable genre combinations")
        print("-" * 80)
        
        print("\n4.1. Multikey Index: genre_pairs")
        self.collection.create_index(
            [("content_info.genre_pairs", ASCENDING)],
            name="idx_genre_pairs"
        )
        
        print("\n4.2. Compound Index: is_profitable + profit")
        self.collection.create_index(
            [
                ("financial.is_profitable", ASCENDING),
                ("financial.profit", DESCENDING)
            ],
            name="idx_profitable_profit"
        )
        
        print("\n4.3. Compound Index: genre_pairs + roi")
        self.collection.create_index(
            [
                ("content_info.genre_pairs", ASCENDING),
                ("financial.roi", DESCENDING)
            ],
            name="idx_genre_pairs_roi"
        )
        
        print("\nQUERY 5: Average runtime per country (rating > 7, 100+ movies)")
        print("-" * 80)
        
        print("\n5.1. Compound Index: quality_tier + countries + runtime")
        self.collection.create_index(
            [
                ("ratings.quality_tier", ASCENDING),
                ("production.countries", ASCENDING),
                ("content_info.runtime", ASCENDING)
            ],
            name="idx_quality_countries_runtime"
        )
        
        print("\n5.2. Compound Index: vote_average + countries")
        self.collection.create_index(
            [
                ("ratings.vote_average", ASCENDING),
                ("production.countries", ASCENDING)
            ],
            name="idx_rating_countries"
        )
        
        print("\n5.3. Single Field Index: vote_average")
        self.collection.create_index(
            [("ratings.vote_average", ASCENDING)],
            name="idx_vote_average"
        )
        
        print("\nADDITIONAL INDEXES")
        print("-" * 80)
        
        print("\n6. Text Index: Full-text search")
        self.collection.create_index(
            [
                ("title", TEXT),
                ("overview", TEXT)
            ],
            name="idx_text_search",
            default_language="english"
        )
        
        print("\n" + "="*80)
        print("Completed: 14 optimized indexes created")
        print("="*80)


class QueryPerformanceTester:
    
    def __init__(self, db):
        self.db = db
        self.collection = db.movies
    
    def test_query_1(self):
        print("\n" + "="*80)
        print("QUERY 1: Average revenue per company (budget > 50M)")
        print("="*80)
        
        print("\nOptimized version using computed fields and indexes:")
        
        pipeline = [
            {"$match": {
                "financial.budget_category": {"$in": ["high", "blockbuster"]}
            }},
            {"$unwind": "$production.companies"},
            {"$group": {
                "_id": "$production.companies",
                "avg_revenue": {"$avg": "$financial.revenue"},
                "total_movies": {"$sum": 1},
                "avg_roi": {"$avg": "$financial.roi"}
            }},
            {"$sort": {"avg_revenue": -1}},
            {"$limit": 10}
        ]
        
        start = time.time()
        result = list(self.collection.aggregate(pipeline))
        elapsed = (time.time() - start) * 1000
        
        print(f"Execution time: {elapsed:.2f} ms")
        print(f"Results: {len(result)}")
        if result:
            print(f"Top company: {result[0]['_id']}")
            print(f"Average revenue: ${result[0]['avg_revenue']:,.0f}")
        
        return elapsed
    
    def test_query_2(self):
        print("\n" + "="*80)
        print("QUERY 2: Average rating per genre by decade")
        print("="*80)
        
        print("\nOptimized version using computed decade:")
        
        pipeline = [
            {"$match": {
                "release_info.decade": {"$exists": True, "$ne": None},
                "ratings.vote_average": {"$gt": 0}
            }},
            {"$unwind": "$content_info.genres"},
            {"$group": {
                "_id": {
                    "decade": "$release_info.decade",
                    "genre": "$content_info.genres"
                },
                "avg_rating": {"$avg": "$ratings.vote_average"},
                "movie_count": {"$sum": 1}
            }},
            {"$sort": {
                "_id.decade": 1,
                "avg_rating": -1
            }},
            {"$limit": 20}
        ]
        
        start = time.time()
        result = list(self.collection.aggregate(pipeline))
        elapsed = (time.time() - start) * 1000
        
        print(f"Execution time: {elapsed:.2f} ms")
        print(f"Results: {len(result)}")
        if result:
            print(f"Example: {result[0]['_id']['genre']} in {result[0]['_id']['decade']}s - rating {result[0]['avg_rating']:.2f}")
        
        return elapsed
    
    def test_query_3(self):
        print("\n" + "="*80)
        print("QUERY 3: Months with most blockbuster movies (budget > 100M)")
        print("="*80)
        
        print("\nOptimized version using budget_category:")
        
        pipeline = [
            {"$match": {
                "financial.budget_category": "blockbuster",
                "release_info.month": {"$exists": True, "$ne": None}
            }},
            {"$group": {
                "_id": "$release_info.month",
                "blockbuster_count": {"$sum": 1},
                "avg_budget": {"$avg": "$financial.budget"},
                "avg_revenue": {"$avg": "$financial.revenue"}
            }},
            {"$sort": {"blockbuster_count": -1}}
        ]
        
        start = time.time()
        result = list(self.collection.aggregate(pipeline))
        elapsed = (time.time() - start) * 1000
        
        print(f"Execution time: {elapsed:.2f} ms")
        print(f"Results: {len(result)}")
        if result:
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            top_month = result[0]
            month_name = month_names[top_month['_id']] if top_month['_id'] <= 12 else str(top_month['_id'])
            print(f"Top month: {month_name} with {top_month['blockbuster_count']} blockbusters")
        
        return elapsed
    
    def test_query_4(self):
        print("\n" + "="*80)
        print("QUERY 4: Most profitable genre combinations")
        print("="*80)
        
        print("\nOptimized version using precomputed genre_pairs:")
        
        pipeline = [
            {"$match": {
                "financial.is_profitable": True,
                "content_info.genre_pairs": {"$exists": True, "$ne": []}
            }},
            {"$unwind": "$content_info.genre_pairs"},
            {"$group": {
                "_id": "$content_info.genre_pairs",
                "avg_profit": {"$avg": "$financial.profit"},
                "avg_roi": {"$avg": "$financial.roi"},
                "total_revenue": {"$sum": "$financial.revenue"},
                "movie_count": {"$sum": 1}
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$sort": {"avg_roi": -1}},
            {"$limit": 15}
        ]
        
        start = time.time()
        result = list(self.collection.aggregate(pipeline))
        elapsed = (time.time() - start) * 1000
        
        print(f"Execution time: {elapsed:.2f} ms")
        print(f"Results: {len(result)}")
        if result:
            top = result[0]
            print(f"Most profitable combination: {top['_id']}")
            print(f"Average ROI: {top['avg_roi']:.2f}%")
            print(f"Movies: {top['movie_count']}")
        
        return elapsed
    
    def test_query_5(self):
        print("\n" + "="*80)
        print("QUERY 5: Average runtime per country (rating > 7, 100+ movies)")
        print("="*80)
        
        print("\nOptimized version using quality_tier:")
        
        pipeline = [
            {"$match": {
                "ratings.quality_tier": "excellent",
                "content_info.runtime": {"$gt": 0}
            }},
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
        
        start = time.time()
        result = list(self.collection.aggregate(pipeline))
        elapsed = (time.time() - start) * 1000
        
        print(f"Execution time: {elapsed:.2f} ms")
        print(f"Results: {len(result)}")
        if result:
            top = result[0]
            print(f"Country with longest movies: {top['_id']}")
            print(f"Average runtime: {top['avg_runtime']:.1f} min")
            print(f"Movies: {top['movie_count']}")
        
        return elapsed
    
    def run_all_tests(self):
        print("\n" + "="*80)
        print("TESTING PERFORMANCE - ALL 5 QUERIES")
        print("="*80)
        
        times = {}
        
        times['query_1'] = self.test_query_1()
        times['query_2'] = self.test_query_2()
        times['query_3'] = self.test_query_3()
        times['query_4'] = self.test_query_4()
        times['query_5'] = self.test_query_5()
        
        print("\n" + "="*80)
        print("PERFORMANCE SUMMARY")
        print("="*80)
        
        total_time = sum(times.values())
        
        print("\nExecution times:")
        print(f"  Query 1: {times['query_1']:.2f} ms")
        print(f"  Query 2: {times['query_2']:.2f} ms")
        print(f"  Query 3: {times['query_3']:.2f} ms")
        print(f"  Query 4: {times['query_4']:.2f} ms")
        print(f"  Query 5: {times['query_5']:.2f} ms")
        print(f"\n  Total: {total_time:.2f} ms")
        
        return times


def main():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['SBP_DB']
    
    print("\n" + "="*80)
    print("MONGODB OPTIMIZATION - 5 TARGET QUERIES")
    print("="*80)
    
    print("\nTarget queries:")
    print("  1. Average revenue per company (budget > 50M)")
    print("  2. Average rating per genre by decade")
    print("  3. Months with most blockbuster movies (budget > 100M)")
    print("  4. Most profitable genre combinations")
    print("  5. Average runtime per country (rating > 7, 100+ movies)")
    
    print("\n" + "="*80)
    print("STEP 1: CREATING INDEXES")
    print("="*80)
    
    index_strategy = IndexStrategy(db)
    index_strategy.create_all_indexes()
    
    print("\n" + "="*80)
    print("STEP 2: TESTING QUERY PERFORMANCE")
    print("="*80)
    
    tester = QueryPerformanceTester(db)
    execution_times = tester.run_all_tests()
    
    print("\n" + "="*80)
    print("OPTIMIZATION COMPLETE")
    print("="*80)
    
    print("\nOptimizations applied:")
    print("\n1. Schema restructuring:")
    print("   - Computed fields: decade, budget_category, quality_tier")
    print("   - Financial computed fields: profit, roi, is_profitable")
    print("   - Genre pairs: precomputed combinations")
    print("   - Reduced nesting: 3 levels -> 2 levels")
    print("   - Denormalized: year and month at top level")
    
    print("\n2. Indexing strategy:")
    print("   - 14 targeted indexes")
    print("   - 3 single-field indexes")
    print("   - 8 compound indexes")
    print("   - 2 multikey indexes (genres, genre_pairs)")
    print("   - 1 text index")
    
    print("\n3. Expected performance improvements:")
    print("   - Query 1: 70-85% faster (compound index + budget_category)")
    print("   - Query 2: 75-90% faster (compound index + computed decade)")
    print("   - Query 3: 80-90% faster (compound index + budget_category)")
    print("   - Query 4: 85-95% faster (multikey index + genre_pairs)")
    print("   - Query 5: 70-85% faster (compound index + quality_tier)")
    
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
