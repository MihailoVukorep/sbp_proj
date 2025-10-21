"""
Query 4: Koje su najprofitabilnije kombinacije žanrova?

# BOTTLENECK:
U prvoj verziji profit i ROI se izračunavaju unutar pipeline-a pomoću `$addFields`, 
a žanrovi se dodatno sortiraju u svakoj obradi dokumenta.  
To povećava trošak procesiranja i usporava agregaciju na velikim datasetima.

# REŠENJE:
Druga verzija koristi unapred izračunata polja `financial.profit`, `financial.roi` 
i denormalizovano `content_info.sorted_genres`.  
Kombinacijom sa kompozitnim indeksom nad finansijskim i žanrovskim poljima postiže se 
značajno brže filtriranje i grupisanje žanrovskih kombinacija.

"""

# V1: Neoptimizovan - računanje profita, ROI i sortiranje žanrova u pipeline-u
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

# V2: Optimizovan - koristi precomputed profit, ROI i sortirane žanrove + kompozitni indeks
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
