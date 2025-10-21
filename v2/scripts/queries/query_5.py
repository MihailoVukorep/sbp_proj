"""
Query 5: Average Runtime by Country
Prosečno trajanje filma po zemlji produkcije (ocena > 7.0, > 100 filmova ukupno)
"""

# V1: Direktno iz queries.sql - unwind pa match
QUERY_5_V1 = [
    {
        '$unwind': '$production.countries'
    },
    {
        '$match': {
            'production.countries': {'$ne': ''},
            'content_info.runtime': {'$gt': 0},
            'ratings.vote_average': {'$gt': 7.0}
        }
    },
    {
        '$group': {
            '_id': '$production.countries',
            'avg_runtime': {'$avg': '$content_info.runtime'},
            'movie_count': {'$sum': 1},
            'avg_rating': {'$avg': '$ratings.vote_average'}
        }
    },
    {
        '$match': {'movie_count': {'$gte': 100}}
    },
    {
        '$sort': {'avg_runtime': -1}
    },
    {
        '$limit': 20
    }
]

# V2: OPTIMIZED - Quality tier index + compound index
QUERY_5_V2 = [
    {
        '$match': {
            'ratings.quality_tier': 'excellent',
            'content_info.runtime': {'$gt': 0},
            'production.countries': {'$exists': True, '$ne': []}
        }
    },
    {
        '$unwind': '$production.countries'
    },
    {
        '$group': {
            '_id': '$production.countries',
            'avg_runtime': {'$avg': '$content_info.runtime'},
            'movie_count': {'$sum': 1},
            'avg_rating': {'$avg': '$ratings.vote_average'}
        }
    },
    {
        '$match': {'movie_count': {'$gte': 100}}
    },
    {
        '$sort': {'avg_runtime': -1}
    },
    {
        '$limit': 20
    }
]

QUERY_NAME = "Query 5: Average Runtime by Country (rating > 7)"
QUERY_DESCRIPTION = "Prosečno trajanje filma po zemlji produkcije (ocena > 7.0, > 100 filmova)"
