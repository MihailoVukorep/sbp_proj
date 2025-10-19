from pymongo import MongoClient


class OptimizedQueries:
    
    def __init__(self, db_name='SBP_DB', collection_name='movies'):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    
    def query_1_avg_revenue_by_company(self, limit=20):
        pipeline = [
            {
                "$match": {
                    "financial.budget_category": {"$in": ["high", "blockbuster"]}
                }
            },
            {
                "$unwind": "$production.companies"
            },
            {
                "$group": {
                    "_id": "$production.companies",
                    "avg_revenue": {"$avg": "$financial.revenue"},
                    "total_movies": {"$sum": 1},
                    "total_revenue": {"$sum": "$financial.revenue"},
                    "avg_roi": {"$avg": "$financial.roi"}
                }
            },
            {
                "$sort": {"avg_revenue": -1}
            },
            {
                "$limit": limit
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_2_rating_by_genre_decade(self, limit=None):
        pipeline = [
            {
                "$match": {
                    "release_info.decade": {"$exists": True, "$ne": None},
                    "ratings.vote_average": {"$gt": 0}
                }
            },
            {
                "$unwind": "$content_info.genres"
            },
            {
                "$group": {
                    "_id": {
                        "decade": "$release_info.decade",
                        "genre": "$content_info.genres"
                    },
                    "avg_rating": {"$avg": "$ratings.vote_average"},
                    "movie_count": {"$sum": 1},
                    "avg_popularity": {"$avg": "$ratings.popularity"}
                }
            },
            {
                "$sort": {
                    "_id.decade": 1,
                    "avg_rating": -1
                }
            }
        ]
        
        if limit:
            pipeline.append({"$limit": limit})
        
        return list(self.collection.aggregate(pipeline))
    
    def query_3_blockbuster_months(self):
        pipeline = [
            {
                "$match": {
                    "financial.budget_category": "blockbuster",
                    "release_info.month": {"$exists": True, "$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$release_info.month",
                    "blockbuster_count": {"$sum": 1},
                    "avg_budget": {"$avg": "$financial.budget"},
                    "avg_revenue": {"$avg": "$financial.revenue"},
                    "avg_roi": {"$avg": "$financial.roi"}
                }
            },
            {
                "$sort": {"blockbuster_count": -1}
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_4_profitable_genre_combinations(self, min_movies=10, limit=20):
        pipeline = [
            {
                "$match": {
                    "financial.is_profitable": True,
                    "content_info.genre_pairs": {"$exists": True, "$ne": []}
                }
            },
            {
                "$unwind": "$content_info.genre_pairs"
            },
            {
                "$group": {
                    "_id": "$content_info.genre_pairs",
                    "avg_profit": {"$avg": "$financial.profit"},
                    "avg_roi": {"$avg": "$financial.roi"},
                    "total_revenue": {"$sum": "$financial.revenue"},
                    "total_profit": {"$sum": "$financial.profit"},
                    "movie_count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "movie_count": {"$gte": min_movies}
                }
            },
            {
                "$sort": {"avg_roi": -1}
            },
            {
                "$limit": limit
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_5_runtime_by_country(self, min_movies=100, limit=20):
        pipeline = [
            {
                "$match": {
                    "ratings.quality_tier": "excellent",
                    "content_info.runtime": {"$gt": 0}
                }
            },
            {
                "$unwind": "$production.countries"
            },
            {
                "$group": {
                    "_id": "$production.countries",
                    "avg_runtime": {"$avg": "$content_info.runtime"},
                    "movie_count": {"$sum": 1},
                    "avg_rating": {"$avg": "$ratings.vote_average"},
                    "total_revenue": {"$sum": "$financial.revenue"}
                }
            },
            {
                "$match": {
                    "movie_count": {"$gte": min_movies}
                }
            },
            {
                "$sort": {"avg_runtime": -1}
            },
            {
                "$limit": limit
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_budget_distribution(self):
        pipeline = [
            {
                "$group": {
                    "_id": "$financial.budget_category",
                    "count": {"$sum": 1},
                    "avg_revenue": {"$avg": "$financial.revenue"},
                    "avg_roi": {"$avg": "$financial.roi"},
                    "profitable_count": {
                        "$sum": {"$cond": ["$financial.is_profitable", 1, 0]}
                    }
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_decade_statistics(self):
        pipeline = [
            {
                "$group": {
                    "_id": "$release_info.decade",
                    "movie_count": {"$sum": 1},
                    "avg_rating": {"$avg": "$ratings.vote_average"},
                    "avg_revenue": {"$avg": "$financial.revenue"},
                    "avg_roi": {"$avg": "$financial.roi"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_quality_statistics(self):
        pipeline = [
            {
                "$group": {
                    "_id": "$ratings.quality_tier",
                    "count": {"$sum": 1},
                    "avg_revenue": {"$avg": "$financial.revenue"},
                    "avg_runtime": {"$avg": "$content_info.runtime"}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_top_profitable_movies(self, limit=20):
        pipeline = [
            {
                "$match": {
                    "financial.is_profitable": True
                }
            },
            {
                "$sort": {"financial.profit": -1}
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "title": 1,
                    "profit": "$financial.profit",
                    "roi": "$financial.roi",
                    "revenue": "$financial.revenue",
                    "rating": "$ratings.vote_average"
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def query_top_rated_movies(self, limit=20):
        pipeline = [
            {
                "$match": {
                    "ratings.vote_average": {"$gt": 0}
                }
            },
            {
                "$sort": {"ratings.vote_average": -1}
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "title": 1,
                    "rating": "$ratings.vote_average",
                    "votes": "$ratings.vote_count",
                    "year": "$release_info.year",
                    "runtime": "$content_info.runtime"
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))


def main():
    queries = OptimizedQueries()
    
    print("="*80)
    print("OPTIMIZED QUERY EXAMPLES")
    print("="*80)
    
    print("\n1. Average revenue by company (budget > 50M):")
    result = queries.query_1_avg_revenue_by_company(limit=5)
    for company in result:
        print(f"   {company['_id']}: ${company['avg_revenue']:,.0f}")
    
    print("\n2. Average rating by genre/decade:")
    result = queries.query_2_rating_by_genre_decade(limit=10)
    for item in result[:5]:
        print(f"   {item['_id']['genre']} ({item['_id']['decade']}s): {item['avg_rating']:.2f}")
    
    print("\n3. Blockbuster months:")
    result = queries.query_3_blockbuster_months()
    months = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    for item in result[:5]:
        month = months[item['_id']] if item['_id'] <= 12 else str(item['_id'])
        print(f"   {month}: {item['blockbuster_count']} movies")
    
    print("\n4. Profitable genre combinations:")
    result = queries.query_4_profitable_genre_combinations(limit=5)
    for item in result:
        print(f"   {item['_id']}: ROI {item['avg_roi']:.2f}% ({item['movie_count']} movies)")
    
    print("\n5. Average runtime by country (rating > 7):")
    result = queries.query_5_runtime_by_country(limit=5)
    for item in result:
        print(f"   {item['_id']}: {item['avg_runtime']:.0f} min ({item['movie_count']} movies)")
    
    print("\n6. Budget distribution:")
    result = queries.query_budget_distribution()
    for item in result:
        print(f"   {item['_id']}: {item['count']} movies (ROI: {item['avg_roi']:.2f}%)")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
