"""
Query 1: Koliki je prosečan prihod po filmu produkcijskih kuća čiji su filmovi imali budžet veći od 50 miliona dolara?

# BOTTLENECK:
U prvoj verziji, filtriranje se vrši direktno nad poljem `financial.budget` pomoću `$gt: 50000000`. 
To znači da MongoDB mora da prolazi kroz sve dokumente i proverava vrednost budžeta, što je skupo 
ako kolekcija sadrži mnogo filmova i nema adekvatan indeks na tom polju.

# REŠENJE:
U drugoj verziji, dodato je predefinisano polje `financial.budget_category`, koje klasifikuje filmove 
('low', 'medium', 'high', 'blockbuster') i omogućava korišćenje kompozitnog indeksa koji
uključuje budžetsku kategoriju, prihod i produkcijske kuće. 
Tako MongoDB može mnogo brže da filtrira samo relevantne dokumente (high-budget filmove) 
bez skeniranja cele kolekcije.

"""

# V1: Neoptimizovan - direktni filter po budžetu
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

# V2: Optimizovan - koristi kategoriju budžeta + kompozitni indeks
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
