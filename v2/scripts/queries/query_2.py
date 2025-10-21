"""
Query 2: Kako se prosečna ocena filmova u različitim žanrovima menjala tokom decenija?

# BOTTLENECK:
U prvoj verziji polje `decade` se računa direktno unutar pipeline-a pomoću `$addFields`. 
To povećava trošak obrade jer se izračunavanje vrši za svaki dokument tokom izvršavanja upita.

# REŠENJE:
Druga verzija koristi unapred izračunato polje `release_info.decade` i kompozitni indeks 
koji uključuje deceniju, prosečnu ocenu i žanr. 
Na taj način filtriranje i grupisanje po deceniji i žanru postaje znatno brže.

"""

# V1: Neoptimizovan - izračunavanje decenije u pipeline-u
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

# V2: Optimizovan - koristi precomputed deceniju + kompozitni indeks
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
