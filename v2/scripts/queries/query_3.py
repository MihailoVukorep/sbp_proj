"""
Query 3: Blockbuster Movies by Month
Meseci sa najviše blockbuster premijera (budžet > 100M)
"""

# V1: Direktni budget filter i month extraction
QUERY_3_V1 = [
    {
        '$match': {
            'financial.budget': {'$gt': 100000000},
            'release_info.release_date.month': {'$exists': True}
        }
    },
    {
        '$group': {
            '_id': '$release_info.release_date.month',
            'blockbuster_count': {'$sum': 1},
            'avg_budget': {'$avg': '$financial.budget'},
            'total_revenue': {'$sum': '$financial.revenue'}
        }
    },
    {
        '$sort': {'blockbuster_count': -1}
    }
]

# V2: OPTIMIZED - Budget category + denormalized month
QUERY_3_V2 = [
    {
        '$match': {
            'financial.budget_category': 'blockbuster',
            'release_info.month': {'$exists': True, '$ne': None}
        }
    },
    {
        '$group': {
            '_id': '$release_info.month',
            'blockbuster_count': {'$sum': 1},
            'avg_budget': {'$avg': '$financial.budget'},
            'total_revenue': {'$sum': '$financial.revenue'}
        }
    },
    {
        '$sort': {'blockbuster_count': -1}
    }
]

QUERY_NAME = "Query 3: Blockbuster Movies by Month"
QUERY_DESCRIPTION = "Meseci sa najviše blockbuster premijera (budžet > 100M)"
