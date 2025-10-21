"""
Query 2: Average Rating by Genre and Decade
Prosečna ocena filmova po žanrovima kroz decenije
"""

# V1: Računanje decade u pipeline
QUERY_2_V1 = [
    {
        '$match': {
            'release_info.release_date.year': {'$exists': True, '$ne': None},
            'ratings.vote_average': {'$gt': 0}
        }
    },
    {
        '$unwind': '$content_info.genres'
    },
    {
        '$addFields': {
            'decade': {
                '$multiply': [
                    {'$floor': {'$divide': ['$release_info.release_date.year', 10]}},
                    10
                ]
            }
        }
    },
    {
        '$group': {
            '_id': {
                'genre': '$content_info.genres',
                'decade': '$decade'
            },
            'avg_rating': {'$avg': '$ratings.vote_average'},
            'movie_count': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.genre': 1,
            '_id.decade': 1
        }
    }
]

# V2: OPTIMIZED - Precomputed decade field
QUERY_2_V2 = [
    {
        '$match': {
            'release_info.decade': {'$exists': True, '$ne': None},
            'ratings.vote_average': {'$gt': 0}
        }
    },
    {
        '$unwind': '$content_info.genres'
    },
    {
        '$group': {
            '_id': {
                'genre': '$content_info.genres',
                'decade': '$release_info.decade'
            },
            'avg_rating': {'$avg': '$ratings.vote_average'},
            'movie_count': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.genre': 1,
            '_id.decade': 1
        }
    }
]

QUERY_NAME = "Query 2: Average Rating by Genre and Decade"
QUERY_DESCRIPTION = "Prosečna ocena filmova po žanrovima kroz decenije"
