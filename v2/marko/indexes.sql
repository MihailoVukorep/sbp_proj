use SBP_DB


// ============================================================================
// GRUPA 1: INDEKSI ZA UPIT 1 (Prosečan prihod po kompaniji, budžet > 50M)
// ============================================================================

db.movies_optimized.createIndex(
    {
        "financial.budget_category": 1,
        "financial.revenue": 1,
        "production.companies": 1
    },
    { 
        name: "idx_budget_companies",
    }
);

// ============================================================================
// GRUPA 2: INDEKSI ZA UPIT 2 (Prosečna ocena po žanru kroz decenije)
// ============================================================================

db.movies_optimized.createIndex(
    {
        "content_info.genres": 1,
        "release_info.decade": 1,
        "ratings.vote_average": 1
    },
    { name: "idx_genre_decade_rating" }
);


// ============================================================================
// GRUPA 3: INDEKSI ZA UPIT 3 (Meseci sa najviše blockbuster filmova)
// ============================================================================

db.movies_optimized.createIndex(
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

db.movies_optimized.createIndex(
    {
        "financial.revenue": 1,
        "financial.budget": 1,
        "content_info.sorted_genres": 1
    }
);


// ============================================================================
// GRUPA 5: INDEKSI ZA UPIT 5 (Prosečno trajanje po zemlji, ocena > 7, 100+ filmova)
// ============================================================================

db.movies_optimized.createIndex(
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