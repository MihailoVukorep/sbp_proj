from pymongo import MongoClient
from datetime import datetime
import json


class SchemaValidator:
    
    def __init__(self, db_name='SBP_DB'):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db.movies
    
    def validate_schema(self):
        print("="*80)
        print("SCHEMA VALIDATION")
        print("="*80)
        
        sample = self.collection.find_one()
        
        if not sample:
            print("No documents found")
            return False
        
        print("\nChecking computed fields...")
        
        checks = {
            "schema_version": sample.get('schema_version') == 2,
            "decade computed": 'decade' in sample.get('release_info', {}),
            "budget_category computed": 'budget_category' in sample.get('financial', {}),
            "profit computed": 'profit' in sample.get('financial', {}),
            "roi computed": 'roi' in sample.get('financial', {}),
            "is_profitable computed": 'is_profitable' in sample.get('financial', {}),
            "quality_tier computed": 'quality_tier' in sample.get('ratings', {}),
            "genre_pairs computed": 'genre_pairs' in sample.get('content_info', {}),
            "month denormalized": 'month' in sample.get('release_info', {}),
            "year denormalized": 'year' in sample.get('release_info', {}),
        }
        
        for field, exists in checks.items():
            status = "OK" if exists else "MISSING"
            print(f"  {field}: {status}")
        
        print("\nSample document structure:")
        self._print_structure(sample, depth=2)
        
        return all(checks.values())
    
    def _print_structure(self, doc, depth=0, max_depth=3):
        if depth >= max_depth:
            return
        
        indent = "  " * depth
        
        if isinstance(doc, dict):
            for key, value in list(doc.items())[:10]:
                if isinstance(value, dict):
                    print(f"{indent}{key}:")
                    self._print_structure(value, depth + 1, max_depth)
                elif isinstance(value, list):
                    print(f"{indent}{key}: [{len(value)} items]")
                else:
                    print(f"{indent}{key}: {type(value).__name__}")
    
    def compare_with_v1(self):
        print("\n" + "="*80)
        print("SCHEMA COMPARISON: V1 vs V2")
        print("="*80)
        
        v1_structure = {
            "title": "Top level",
            "ratings.vote_average": "Top level",
            "release_info.release_date.year": "3 levels deep",
            "release_info.release_date.month": "3 levels deep",
            "financial.budget": "Top level",
            "financial.revenue": "Top level",
            "production.companies": "Top level",
            "content_info.genres": "Top level"
        }
        
        v2_structure = {
            "title": "Top level (same)",
            "ratings.vote_average": "Top level (same)",
            "release_info.year": "2 levels (OPTIMIZED)",
            "release_info.month": "2 levels (OPTIMIZED)",
            "release_info.decade": "2 levels (COMPUTED)",
            "financial.budget": "Top level (same)",
            "financial.revenue": "Top level (same)",
            "financial.budget_category": "Top level (COMPUTED)",
            "financial.profit": "Top level (COMPUTED)",
            "financial.roi": "Top level (COMPUTED)",
            "financial.is_profitable": "Top level (COMPUTED)",
            "ratings.quality_tier": "Top level (COMPUTED)",
            "production.companies": "Top level (same)",
            "content_info.genres": "Top level (same)",
            "content_info.genre_pairs": "Top level (COMPUTED)"
        }
        
        print("\nV1 Schema Fields:")
        for field, note in sorted(v1_structure.items()):
            print(f"  {field}: {note}")
        
        print("\nV2 Schema Fields:")
        for field, note in sorted(v2_structure.items()):
            print(f"  {field}: {note}")
        
        print("\nKey improvements:")
        print("  - 7 computed fields added for query optimization")
        print("  - Date structure flattened from 3 to 2 levels")
        print("  - Financial metrics precomputed")
        print("  - Genre combinations precomputed")
    
    def check_indexes(self):
        print("\n" + "="*80)
        print("INDEX STATUS")
        print("="*80)
        
        indexes = self.collection.list_indexes()
        
        expected_indexes = {
            "idx_budget_companies": "Query 1",
            "idx_budget": "Query 1",
            "idx_budget_roi": "Query 1",
            "idx_decade_genre_rating": "Query 2",
            "idx_decade": "Query 2",
            "idx_blockbuster_month": "Query 3",
            "idx_budget_month": "Query 3",
            "idx_genre_pairs": "Query 4",
            "idx_profitable_profit": "Query 4",
            "idx_genre_pairs_roi": "Query 4",
            "idx_quality_countries_runtime": "Query 5",
            "idx_rating_countries": "Query 5",
            "idx_vote_average": "Query 5",
            "idx_text_search": "Full-text search"
        }
        
        existing = {idx['name']: idx for idx in indexes}
        
        print("\nExpected indexes:")
        for idx_name, purpose in sorted(expected_indexes.items()):
            status = "EXISTS" if idx_name in existing else "MISSING"
            print(f"  {idx_name}: {status} ({purpose})")
        
        print(f"\nTotal indexes found: {len(existing)}")
        print(f"Expected: {len(expected_indexes) + 1}")  # +1 for _id index
    
    def get_stats(self):
        print("\n" + "="*80)
        print("COLLECTION STATISTICS")
        print("="*80)
        
        count = self.collection.count_documents({})
        stats = self.db.command('collstats', 'movies')
        
        print(f"\nDocuments: {count:,}")
        print(f"Avg document size: {stats.get('avgObjSize', 0):,.0f} bytes")
        print(f"Storage used: {stats.get('size', 0):,.0f} bytes ({stats.get('size', 0) / (1024*1024):.2f} MB)")
        print(f"Index size: {stats.get('totalIndexSize', 0):,.0f} bytes ({stats.get('totalIndexSize', 0) / (1024*1024):.2f} MB)")
        
        print(f"\nComputed field analysis:")
        
        with_genre_pairs = self.collection.count_documents(
            {"content_info.genre_pairs": {"$exists": True, "$ne": []}}
        )
        print(f"  Documents with genre_pairs: {with_genre_pairs:,} ({(with_genre_pairs/count)*100:.1f}%)")
        
        profitable = self.collection.count_documents(
            {"financial.is_profitable": True}
        )
        print(f"  Profitable movies: {profitable:,} ({(profitable/count)*100:.1f}%)")
        
        by_category = self.collection.aggregate([
            {"$group": {
                "_id": "$financial.budget_category",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ])
        
        print(f"  Budget categories:")
        for doc in by_category:
            category = doc['_id']
            cnt = doc['count']
            print(f"    {category}: {cnt:,} ({(cnt/count)*100:.1f}%)")
        
        by_tier = self.collection.aggregate([
            {"$group": {
                "_id": "$ratings.quality_tier",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ])
        
        print(f"  Quality tiers:")
        for doc in by_tier:
            tier = doc['_id']
            cnt = doc['count']
            print(f"    {tier}: {cnt:,} ({(cnt/count)*100:.1f}%)")


def main():
    validator = SchemaValidator()
    
    print("\n" + "="*80)
    print("V2 SCHEMA VALIDATION AND ANALYSIS")
    print("="*80)
    
    if validator.validate_schema():
        print("\n[SUCCESS] Schema validation passed")
    else:
        print("\n[ERROR] Schema validation failed")
    
    validator.compare_with_v1()
    validator.check_indexes()
    validator.get_stats()
    
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
