// ============================================================================
// Query 1: Koliki je prosečan prihod po filmu produkcijskih kuća čiji su filmovi imali budžet veći od 50 miliona dolara?
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
// Query 2: Kako se prosečna ocena filmova u različitim žanrovima menjala tokom decenija?
// ============================================================================

db.movies_optimized.createIndex(
    {
        "release_info.decade": 1,
        "ratings.vote_average": 1,
        "content_info.genres": 1
    },
    { name: "idx_genre_decade_rating" }
);


// ============================================================================
// Query 3: Koji meseci u godini imaju najveću stopu objavljivanja blockbuster (hit) filmova (budžet > 100M)?
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
// Query 4: Koje su najprofitabilnije kombinacije žanrova?
// ============================================================================

db.movies_optimized.createIndex(
    {
        "financial.revenue": 1,
        "financial.budget": 1,
        "content_info.sorted_genres": 1,

        "financial.profit": 1,
        "financial.roi": 1
    }
);


// ============================================================================
// Query 5: Prosečno trajanje filma po zemlji produkcije sa ocenom iznad 7.0, uzimajući u obzir samo zemlje koje su proizvele više od
// 100 filmova?
// ============================================================================

db.movies_optimized.createIndex(
  {
    "production.countries": 1,
    "content_info.runtime": 1,
    "ratings.quality_tier": 1  
  },
  { name: "idx_ESR_countries_runtime_quality" }
);