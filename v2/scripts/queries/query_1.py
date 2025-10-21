"""
Query 1: Top Profitable Companies (budget > 50M)
Prosečan prihod po filmu produkcijskih kuća sa budžetom > 50M
"""

# V1: Direktni budget filter
QUERY_1_V1 = [
    {
        '$match': {
            'financial.budget': {'$gt': 50000000},
            'financial.revenue': {'$gt': 0}
        }
    },
    {
        '$unwind': '$production.companies'
    },
    {
        '$group': {
            '_id': '$production.companies',
            'avg_revenue': {'$avg': '$financial.revenue'},
            'total_movies': {'$sum': 1},
            'total_revenue': {'$sum': '$financial.revenue'}
        }
    },
    {
        '$sort': {'avg_revenue': -1}
    },
    {
        '$limit': 20
    }
]

# V2: OPTIMIZED - Budget category + compound index
QUERY_1_V2 = [
    {
        '$match': {
            'financial.budget_category': {'$in': ['high', 'blockbuster']},
            'financial.revenue': {'$gt': 0}
        }
    },
    {
        '$unwind': '$production.companies'
    },
    {
        '$group': {
            '_id': '$production.companies',
            'avg_revenue': {'$avg': '$financial.revenue'},
            'total_movies': {'$sum': 1},
            'total_revenue': {'$sum': '$financial.revenue'}
        }
    },
    {
        '$sort': {'avg_revenue': -1}
    },
    {
        '$limit': 20
    }
]

QUERY_NAME = "Query 1: Top Profitable Companies (budget > 50M)"
QUERY_DESCRIPTION = "Prosečan prihod po filmu produkcijskih kuća sa budžetom > 50M"
