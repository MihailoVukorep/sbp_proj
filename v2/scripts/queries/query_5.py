"""
Query 5: Prosečno trajanje filma po zemlji produkcije sa ocenom iznad 7.0,
         uzimajući u obzir samo zemlje koje su proizvele više od 100 filmova

# BOTTLENECK:
U prvoj verziji upit prvo vrši `$unwind` nad poljem `production.countries`, a zatim filtrira po oceni i trajanju.  
Bez indeksa, MongoDB mora da procesira sve dokumente, što može biti sporo na velikim datasetima.

# REŠENJE:
Druga verzija koristi unapred definisan kvalitet ocene (`ratings.quality_tier`) i kompozitni indeks 
koji uključuje zemlje, trajanje i kvalitet ocene.  
Time se filtriranje i agregacija po zemlji znatno ubrzava.

"""

# V1: Neoptimizovan - unwind pa match
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

# V2: Optimizovan - koristi kvalitet ocene + kompozitni indeks
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
