// ============================================================================
// 1. Prosečan prihod po filmu produkcijskih kuća sa budžetom > 50M
// ============================================================================
db.movies.aggregate([
  {
    $match: {
      "financial.budget": { $gt: 50000000 },
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
      total_revenue: { $sum: "$financial.revenue" }
    }
  },
  {
    $sort: { avg_revenue: -1 }
  },
  {
    $limit: 20
  }
]);

// ============================================================================
// 2. Prosečna ocena filmova po žanrovima kroz decenije
// ============================================================================
db.movies.aggregate([
  {
    $match: {
      "release_info.release_date.year": { $exists: true, $ne: null },
      "ratings.vote_average": { $gt: 0 }
    }
  },
  {
    $unwind: "$content_info.genres"
  },
  {
    $addFields: {
      decade: {
        $multiply: [
          { $floor: { $divide: ["$release_info.release_date.year", 10] } },
          10
        ]
      }
    }
  },
  {
    $group: {
      _id: {
        genre: "$content_info.genres",
        decade: "$decade"
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

// ============================================================================
// 3. Meseci sa najviše blockbuster premijera (budžet > 100M)
// ============================================================================
db.movies.aggregate([
  {
    $match: {
      "financial.budget": { $gt: 100000000 },
      "release_info.release_date.month": { $exists: true, $ne: null }
    }
  },
  {
    $group: {
      _id: "$release_info.release_date.month",
      blockbuster_count: { $sum: 1 },
      avg_budget: { $avg: "$financial.budget" },
      total_revenue: { $sum: "$financial.revenue" }
    }
  },
  {
    $sort: { blockbuster_count: -1 }
  },
  {
    $addFields: {
      month_name: {
        $switch: {
          branches: [
            { case: { $eq: ["$_id", 1] }, then: "Januar" },
            { case: { $eq: ["$_id", 2] }, then: "Februar" },
            { case: { $eq: ["$_id", 3] }, then: "Mart" },
            { case: { $eq: ["$_id", 4] }, then: "April" },
            { case: { $eq: ["$_id", 5] }, then: "Maj" },
            { case: { $eq: ["$_id", 6] }, then: "Jun" },
            { case: { $eq: ["$_id", 7] }, then: "Jul" },
            { case: { $eq: ["$_id", 8] }, then: "Avgust" },
            { case: { $eq: ["$_id", 9] }, then: "Septembar" },
            { case: { $eq: ["$_id", 10] }, then: "Oktobar" },
            { case: { $eq: ["$_id", 11] }, then: "Novembar" },
            { case: { $eq: ["$_id", 12] }, then: "Decembar" }
          ],
          default: "Nepoznato"
        }
      }
    }
  }
]);

// ============================================================================
// 4. Najprofitabilnije kombinacije žanrova
// ============================================================================
db.movies.aggregate([
  {
    $match: {
      "financial.revenue": { $gt: 0 },
      "financial.budget": { $gt: 0 },
      "content_info.genres": { $exists: true, $ne: [] }
    }
  },
  {
    $addFields: {
      profit: { $subtract: ["$financial.revenue", "$financial.budget"] },
      roi: {
        $multiply: [
          {
            $divide: [
              { $subtract: ["$financial.revenue", "$financial.budget"] },
              "$financial.budget"
            ]
          },
          100
        ]
      },
      sorted_genres: { $sortArray: { input: "$content_info.genres", sortBy: 1 } }
    }
  },
  {
    $group: {
      _id: "$sorted_genres",
      avg_profit: { $avg: "$profit" },
      avg_roi: { $avg: "$roi" },
      total_profit: { $sum: "$profit" },
      movie_count: { $sum: 1 }
    }
  },
  {
    $match: {
      movie_count: { $gte: 10 }
    }
  },
  {
    $sort: { avg_profit: -1 }
  },
  {
    $limit: 20
  }
]);

// ============================================================================
// 5. Prosečno trajanje filma po zemlji produkcije (ocena > 7.0, > 100 filmova ukupno)
// ============================================================================
db.movies.aggregate([
  { $unwind: "$production.countries" },
  { $match: { "production.countries": { $ne: "" }, /* ... */ } },
  {
    $group: {
      _id: "$production.countries",
      totalMovieCount: { $sum: 1 },
      totalRuntimeOver7: {
        $sum: {
          $cond: { if: { $gt: ["$ratings.vote_average", 7.0] }, then: "$content_info.runtime", else: 0 }
        }
      },
      movieCountOver7: {
        $sum: {
          $cond: { if: { $gt: ["$ratings.vote_average", 7.0] }, then: 1, else: 0 }
        }
      }
    }
  },
  {
    $match: {
      totalMovieCount: { $gt: 100 }
    }
  },
  {
    $project: {
      avgRuntimeOver7: { $divide: ["$totalRuntimeOver7", "$movieCountOver7"] }
    }
  },
  { $sort: { avgRuntimeOver7: -1 } }
]);