use SBP_DB


// ============================================================================
// GRUPA 1: INDEKSI ZA UPIT 1 (Prosečan prihod po kompaniji, budžet > 50M)
// ============================================================================

// 1.1. Compound Index: budget_category + companies
db.movies.createIndex(
    {
        "financial.budget_category": 1,
        "production.companies": 1
    },
    { 
        name: "idx_budget_companies",
    }
);


// ============================================================================
// GRUPA 2: INDEKSI ZA UPIT 2 (Prosečna ocena po žanru kroz decenije)
// ============================================================================

// 2.1. Compound Index: decade + genres + vote_average
db.movies.createIndex(
    {
        "release_info.decade": 1,
        "content_info.genres": 1,
        "ratings.vote_average": -1
    },
    { 
        name: "idx_decade_genre_rating",
    }
);


// ============================================================================
// GRUPA 3: INDEKSI ZA UPIT 3 (Meseci sa najviše blockbuster filmova)
// ============================================================================


// 3.1. Compound Index: budget_category + month
db.movies.createIndex(
    {
        "financial.budget_category": 1,
        "release_info.month": 1
    },
    { 
        name: "idx_blockbuster_month",
    }
);


// ============================================================================
// GRUPA 4: INDEKSI ZA UPIT 4 (Najprofitabilnije kombinacije žanrova)
// ============================================================================


// 4.1. Compound Index: is_profitable + genre_pairs
db.movies.createIndex(
    {
        "financial.is_profitable": 1,
        "content_info.genre_pairs": 1
    },
    { 
        name: "idx_profitable_genre_pairs"
    }
);


// ============================================================================
// GRUPA 5: INDEKSI ZA UPIT 5 (Prosečno trajanje po zemlji, ocena > 7, 100+ filmova)
// ============================================================================


// 5.1. Compound Index: quality_tier + countries + runtime
db.movies.createIndex(
    {
        "ratings.quality_tier": 1,
        "production.countries": 1,
        "content_info.runtime": 1
    },
    { 
        name: "idx_quality_countries_runtime",
    }
);



---------------------------------------------------------------------------------------------

// ============================================================================
// GRUPA 6: DODATNI KORISNI INDEKSI
// ============================================================================


// 6.1. Text Index: Za tekstualnu pretragu
db.movies.createIndex(
    {
        "title": "text",
        "overview": "text"
    },
    { 
        name: "idx_text_search",
        default_language: "english",
    }
);

// 6.2. Single Field Index: IMDB ID (za lookup)
db.movies.createIndex(
    {
        "media.imdb_id": 1
    },
    { 
        name: "idx_imdb_id",
        sparse: true,
    }
);