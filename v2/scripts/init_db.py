import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import traceback
from models import OptimizedMovieDocument
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


class DatabaseInitializer:
    
    def __init__(self, csv_path, connection_string='mongodb://localhost:27017/', 
                 database_name='SBP_DB', collection_name='movies_optimized'):
        
        self.csv_path = csv_path
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.batch_size = 10000
        self.num_workers = 8
        
        self.client = None
        self.db = None
        self.collection = None
        self.lock = Lock()
        
    def connect(self) -> bool:
        try:
            print("Connecting to MongoDB...")
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            self.client.admin.command('ping')
            print(f"Connected to database: {self.database_name}")
            return True
        except Exception as e:
            print(f"Connection failed: {type(e).__name__}: {str(e)}")
            return False
    
    def _clean_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\nCleaning duplicates...")
        
        original_count = len(df)
        
        df['_completeness_score'] = (
            df['imdb_id'].notna().astype(int) * 10 +
            df['release_date'].notna().astype(int) * 5 +
            df['overview'].notna().astype(int) * 3 +
            df['revenue'].fillna(0).astype(bool).astype(int) * 2 +
            df['vote_count'].fillna(0).astype(int) / 100
        )
        
        df = df.sort_values(['id', '_completeness_score'], ascending=[True, False])
        df_clean = df.drop_duplicates(subset=['id'], keep='first')
        df_clean = df_clean.drop(columns=['_completeness_score'])
        
        removed_count = original_count - len(df_clean)
        
        print(f"Original rows: {original_count:,}")
        print(f"After deduplication: {len(df_clean):,}")
        print(f"Removed duplicates: {removed_count:,}")
        
        return df_clean
    
    def _process_chunk(self, chunk_data):
        chunk_idx, start_idx, end_idx, df = chunk_data
        
        chunk = df.iloc[start_idx:end_idx]
        movies = []
        errors = 0
        
        for row_dict in chunk.to_dict('records'):
            try:
                movie = OptimizedMovieDocument.transform(row_dict)
                movies.append(movie)
            except Exception:
                errors += 1
                continue
        
        if movies:
            client = MongoClient(self.connection_string)
            db = client[self.database_name]
            collection = db[self.collection_name]
            
            try:
                collection.insert_many(movies, ordered=False)
                inserted = len(movies)
                duplicates = 0
                other_errors = 0
            except BulkWriteError as e:
                details = e.details
                write_errors = details.get('writeErrors', [])
                duplicates = sum(1 for err in write_errors if err.get('code') == 11000)
                other_errors = len(write_errors) - duplicates
                inserted = details.get('nInserted', 0)
            finally:
                client.close()
            
            return inserted, duplicates, other_errors + errors
        
        return 0, 0, errors
    
    def load_movies_to_db(self) -> bool:
        try:
            print(f"Reading CSV: {self.csv_path}")
            df = pd.read_csv(self.csv_path)
            print(f"Loaded {len(df):,} rows from CSV")
            
            print(f"Unique TMDB IDs: {df['id'].nunique():,}")
            print(f"TMDB ID duplicates: {df['id'].duplicated().sum():,}")
            print(f"Missing IMDB IDs: {df['imdb_id'].isna().sum():,}")
            
            df = self._clean_duplicates(df)
            
            total_rows = len(df)
            
            self.collection = self.db[self.collection_name]
            self.collection.drop()
            print(f"Collection '{self.collection_name}' prepared")
            
            processed = 0
            duplicate_errors = 0
            other_errors = 0
            
            num_chunks = (total_rows + self.batch_size - 1) // self.batch_size
            
            chunk_data_list = []
            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * self.batch_size
                end_idx = min(start_idx + self.batch_size, total_rows)
                chunk_data_list.append((chunk_idx, start_idx, end_idx, df))
            
            print(f"Processing with {self.num_workers} workers...")
            
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = {executor.submit(self._process_chunk, chunk_data): chunk_data[0] 
                          for chunk_data in chunk_data_list}
                
                completed = 0
                for future in as_completed(futures):
                    try:
                        inserted, dups, errors = future.result()
                        processed += inserted
                        duplicate_errors += dups
                        other_errors += errors
                        completed += 1
                        
                        if completed % 10 == 0 or completed == num_chunks:
                            print(f"Processed {processed:,}/{total_rows:,} documents ({(processed/total_rows)*100:.1f}%)")
                    except Exception as e:
                        print(f"Chunk processing error: {type(e).__name__}: {str(e)}")
                        other_errors += 1
            
            print(f"\nImport complete:")
            print(f"  Successfully inserted: {processed:,}")
            if duplicate_errors > 0:
                print(f"  Duplicates skipped: {duplicate_errors:,}")
            if other_errors > 0:
                print(f"  Errors: {other_errors:,}")
            
            return True
            
        except Exception as e:
            print(f"Critical error: {type(e).__name__}: {str(e)}")
            traceback.print_exc()
            return False
    
    def verify(self):
        print("\nVerifying import...")
        
        try:
            count = self.collection.count_documents({})
            print(f"Total documents: {count:,}")
            
            with_imdb = self.collection.count_documents({"media.imdb_id": {"$ne": ""}})
            print(f"Documents with IMDB ID: {with_imdb:,} ({(with_imdb/count)*100:.1f}%)")
            
            pipeline = [
                {"$match": {"release_info.year": {"$exists": True}}},
                {"$group": {
                    "_id": None,
                    "min_year": {"$min": "$release_info.year"},
                    "max_year": {"$max": "$release_info.year"}
                }}
            ]
            year_range = list(self.collection.aggregate(pipeline))
            if year_range:
                print(f"Year range: {year_range[0]['min_year']} - {year_range[0]['max_year']}")
            
            sample = self.collection.find_one()
            if sample:
                print(f"\nSample document:")
                print(f"  ID: {sample['_id']}")
                print(f"  Title: {sample.get('title')}")
                print(f"  IMDB ID: {sample.get('media', {}).get('imdb_id')}")
                print(f"  Rating: {sample.get('ratings', {}).get('vote_average')}")
                print(f"  Year: {sample.get('release_info', {}).get('year')}")
                print(f"  Decade: {sample.get('release_info', {}).get('decade')}")
                print(f"  Budget Category: {sample.get('financial', {}).get('budget_category')}")
                print(f"  Quality Tier: {sample.get('ratings', {}).get('quality_tier')}")
            else:
                print("No documents found")
                
        except Exception as e:
            print(f"Verification failed: {type(e).__name__}: {str(e)}")


def main():
    initializer = DatabaseInitializer(
        csv_path='../../dataset/TMDB_movie_dataset_v11.csv',
        connection_string='mongodb://localhost:27017/',
        database_name='SBP_DB',
        collection_name='movies'
    )
    
    if not initializer.connect():
        print("Failed to connect to MongoDB")
        exit(1)
    
    if not initializer.load_movies_to_db():
        print("Failed to load movies")
        exit(1)
    
    initializer.verify()
    print("\nDatabase initialization complete")


if __name__ == "__main__":
    main()
