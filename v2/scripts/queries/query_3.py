"""
Query 3: Koji meseci u godini imaju najveću stopu objavljivanja blockbuster (hit) filmova (budžet > 100M)?

# BOTTLENECK:
U prvoj verziji filtriranje se vrši direktno nad poljem `financial.budget` i koristi se ugnježdeno 
polje `release_info.release_date.month`. To dovodi do sporijeg izvršavanja jer nije indeksirano 
i MongoDB mora da prolazi kroz sve dokumente radi provere budžeta i ekstrakcije meseca.

# REŠENJE:
Druga verzija koristi unapred definisanu kategoriju budžeta (`financial.budget_category`) 
i denormalizovano polje `release_info.month`.  
Kombinacija sa kompozitnim indeksom koji uključuje budžetsku kategoriju i mesec značajno ubrzava filtriranje 
i agregaciju blockbuster filmova po mesecima.

"""

# V1: Neoptimizovan - direktni budget filter i izdvajanje meseca
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

# V2: Optimizovan - koristi budžetsku kategoriju + denormalizovan mesec + kompozitni indeks
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
