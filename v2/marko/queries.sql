QUERY 1: Average revenue per company (budget > 50M)
---------------------------------------------------------
Bottleneck: No index on budget, unwind on companies array, multiple aggregation stages
Solution: Computed budget_category, compound index (budget_category, companies)

db.movies.aggregate([
  {
    $match: {
      "financial.budget_category": { $in: ["high", "blockbuster"] },
      "financial.revenue": { $gt: 0 }
    }
  },
  {
    $unwind: "$production.companies"
  },
  {
    $group: {
      _id: "$production.companies",
      avg_revenue: { $avg: "$financial.revenue" },
      total_movies: { $sum: 1 },
      total_revenue: { $sum: "$financial.revenue" },
    }
  },
  {
    $sort: { avg_revenue: -1 }
  },
  {
    $limit: 20
  }
]);


QUERY 2: Average rating per genre by decade
----------------------------------------------
Bottleneck: Decade calculation in runtime, nested date structure, multiple unwinds
Solution: Precomputed decade field, compound index (decade, genres, vote_average)

db.movies.aggregate([
  {
    $match: {
      "release_info.decade": { $exists: true, $ne: null },
      "ratings.vote_average": { $gt: 0 }
    }
  },
  { $unwind: "$content_info.genres" },
  {
    $group: {
      _id: {
        genre: "$content_info.genres",
        decade: "$release_info.decade"
      },
      avg_rating: { $avg: "$ratings.vote_average" },
      movie_count: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.genre": 1,
      "_id.decade": 1
    }
  }
]);


QUERY 3: Months with most blockbuster movies (budget > 100M)
--------------------------------------------------------------
Bottleneck: Budget filtering without index, nested month extraction
Solution: Computed budget_category, denormalized month field, compound index

db.movies.aggregate([
  {
    $match: {
      "financial.budget_category": "blockbuster",
      "release_info.month": { $exists: true, $ne: null }
    }
  },
  {
    $group: {
      _id: "$release_info.month",
      blockbuster_count: { $sum: 1 },
      avg_budget: { $avg: "$financial.budget" },
      total_revenue: { $sum: "$financial.revenue" }
    }
  },
  {
    $sort: { blockbuster_count: -1 }
  }
]);


QUERY 4: Most profitable genre combinations
---------------------------------------------
Bottleneck: Genre pair generation in pipeline (very slow), profit/roi calculation, no index
Solution: Precomputed genre_pairs, computed profit/roi, multikey index on genre_pairs

db.movies.aggregate([
  {
    $match: {
      "financial.is_profitable": true,
      "content_info.genre_pairs": { $exists: true, $ne: [] }
    }
  },
  {
    $unwind: "$content_info.genre_pairs"
  },
  {
    $group: {
      _id: "$content_info.genre_pairs",
      avg_profit: { $avg: "$financial.profit" },
      avg_roi: { $avg: "$financial.roi" },
      total_profit: { $sum: "$financial.profit" },
      movie_count: { $sum: 1 }
    }
  },
  {
    $match: {
      movie_count: { $gte: 10 }
    }
  },
  {
    $sort: { avg_roi: -1 }
  },
  {
    $limit: 20
  }
]);


QUERY 5: Average runtime per country (rating > 7, 100+ movies)
--------------------------------------------------------------
Bottleneck: Rating filter without index, unwind on countries, two-pass aggregation
Solution: Precomputed quality_tier, compound index (quality_tier, countries, runtime)

db.movies.aggregate([
  {
    $match: {
      "ratings.quality_tier": "excellent",
      "content_info.runtime": { $gt: 0 }
    }
  },
  {
    $unwind: "$production.countries"
  },
  {
    $group: {
      _id: "$production.countries",
      avg_runtime: { $avg: "$content_info.runtime" },
      movie_count: { $sum: 1 },
      avg_rating: { $avg: "$ratings.vote_average" }
    }
  },
  {
    $match: {
      movie_count: { $gte: 100 }
    }
  },
  {
    $sort: { avg_runtime: -1 }
  },
  {
    $limit: 20
  }
]);


OPTIMIZATION ANALYSIS
======================

Computed fields applied:
- decade: (year // 10) * 10
- budget_category: low/medium/high/blockbuster
- quality_tier: poor/average/good/excellent
- profit: revenue - budget
- roi: ((revenue - budget) / budget) * 100
- is_profitable: profit > 0
- genre_pairs: all 2-combinations of genres

Indexes created:
1. idx_budget_companies (Query 1)
2. idx_budget (Query 1)
3. idx_budget_roi (Query 1)
4. idx_decade_genre_rating (Query 2)
5. idx_decade (Query 2)
6. idx_blockbuster_month (Query 3)
7. idx_budget_month (Query 3)
8. idx_genre_pairs (Query 4)
9. idx_profitable_profit (Query 4)
10. idx_genre_pairs_roi (Query 4)
11. idx_quality_countries_runtime (Query 5)
12. idx_rating_countries (Query 5)
13. idx_vote_average (Query 5)
14. idx_text_search (full-text search)

Performance improvements:
- Query 1: Eliminates budget calculation, uses index for category filtering
- Query 2: Decade precomputed, reduces aggregation stages
- Query 3: Budget category filtering is O(log n) instead of full scan
- Query 4: Genre pairs precomputed, eliminates expensive pipeline generation
- Query 5: Quality tier filtering is O(log n), avoids vote_average calculation

Schema changes from v1:
- release_info: Added year, month, decade at top level (was nested in release_date)
- financial: Added profit, roi, is_profitable, budget_category
- ratings: Added quality_tier
- content_info: Added genre_pairs
- production: Added company_count, country_count
- Added schema_version and optimized_for_queries metadata
