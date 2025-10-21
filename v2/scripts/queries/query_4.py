"""
Query 4: Most Profitable Genre Combinations
Najprofitabilnije kombinacije žanrova
"""

# V1: Kalkulacija profit/roi u pipeline
QUERY_4_V1 = [
    {
        '$match': {
            'financial.revenue': {'$gt': 0},
            'financial.budget': {'$gt': 0},
            'content_info.genres': {'$exists': True, '$ne': []}
        }
    },
    {
        '$addFields': {
            'profit': {'$subtract': ['$financial.revenue', '$financial.budget']},
            'roi': {
                '$multiply': [
                    {
                        '$divide': [
                            {'$subtract': ['$financial.revenue', '$financial.budget']},
                            '$financial.budget'
                        ]
                    },
                    100
                ]
            },
            'sorted_genres': {'$sortArray': {'input': '$content_info.genres', 'sortBy': 1}}
        }
    },
    {
        '$group': {
            '_id': '$sorted_genres',
            'avg_profit': {'$avg': '$profit'},
            'avg_roi': {'$avg': '$roi'},
            'total_profit': {'$sum': '$profit'},
            'movie_count': {'$sum': 1}
        }
    },
    {
        '$match': {
            'movie_count': {'$gte': 10}
        }
    },
    {
        '$sort': {'avg_profit': -1}
    },
    {
        '$limit': 20
    }
]

# V2: OPTIMIZED - Precomputed sorted_genres + profit/roi
QUERY_4_V2 = [
    {
        '$match': {
            'financial.revenue': {'$gt': 0},
            'financial.budget': {'$gt': 0},
            'content_info.sorted_genres': {'$exists': True, '$ne': []}
        }
    },
    {
        '$group': {
            '_id': '$content_info.sorted_genres',
            'avg_profit': {'$avg': '$financial.profit'},
            'avg_roi': {'$avg': '$financial.roi'},
            'total_profit': {'$sum': '$financial.profit'},
            'movie_count': {'$sum': 1}
        }
    },
    {
        '$match': {
            'movie_count': {'$gte': 10}
        }
    },
    {
        '$sort': {'avg_profit': -1}
    },
    {
        '$limit': 20
    }
]

QUERY_NAME = "Query 4: Most Profitable Genre Combinations"
QUERY_DESCRIPTION = "Najprofitabilnije kombinacije žanrova"
