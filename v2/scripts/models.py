from typing import Dict, List, Any


class OptimizedMovieDocument:
    
    @staticmethod
    def parse_date(date_value: Any) -> Dict[str, Any]:
        if not date_value:
            return {}
        
        try:
            if isinstance(date_value, str):
                parts = date_value.split('-')
                year = int(parts[0])
                month = int(parts[1])
                return {
                    "year": year,
                    "month": month,
                    "day": int(parts[2]),
                    "full_date": date_value,
                    "decade": (year // 10) * 10              #added
                }
            elif hasattr(date_value, 'year'):
                year = date_value.year
                return {
                    "year": year,
                    "month": date_value.month,
                    "day": date_value.day,
                    "full_date": date_value.strftime('%Y-%m-%d'),
                    "decade": (year // 10) * 10              #added
                }
        except (ValueError, IndexError, AttributeError):
            return {"full_date": str(date_value)}
        
        return {}
    
    @staticmethod
    def parse_array_field(field_value: Any) -> List[str]:
        if not field_value or field_value == "":
            return []
        return [item.strip() for item in str(field_value).split(', ')]
    
    @staticmethod
    def categorize_budget(budget: float) -> str:             #added
        if budget >= 100_000_000:
            return 'blockbuster'
        elif budget >= 50_000_000:
            return 'high'
        elif budget >= 10_000_000:
            return 'medium'
        else:
            return 'low'
    
    @staticmethod
    def categorize_quality(vote_average: float) -> str:      #added
        if vote_average >= 7.0:
            return 'excellent'
        elif vote_average >= 6.0:
            return 'good'
        elif vote_average >= 5.0:
            return 'average'
        else:
            return 'poor'
    
    @staticmethod
    def generate_genre_pairs(genres: List[str]) -> List[str]: #added
        if len(genres) < 2:
            return []
        
        pairs = []
        sorted_genres = sorted(genres)
        
        for i in range(len(sorted_genres)):
            for j in range(i + 1, len(sorted_genres)):
                pair = f"{sorted_genres[i]}+{sorted_genres[j]}"
                pairs.append(pair)
        
        return pairs
    
    @staticmethod
    def calculate_roi(budget: float, revenue: float) -> float:  #added
        if budget > 0:
            return round(((revenue - budget) / budget) * 100, 2)
        return 0.0
    
    @classmethod
    def transform(cls, doc: Dict[str, Any]) -> Dict[str, Any]:  #modified
        release_date = cls.parse_date(doc.get('release_date'))
        
        budget = doc.get('budget', 0)                           #?added ŠABLON PRORAČUNAVANJA (Computation Pattern)
        revenue = doc.get('revenue', 0)
        profit = revenue - budget
        roi = cls.calculate_roi(budget, revenue)
        budget_category = cls.categorize_budget(budget)
        is_profitable = profit > 0
        
        vote_avg = doc.get('vote_average', 0)
        vote_count = doc.get('vote_count', 0)
        quality_tier = cls.categorize_quality(vote_avg)
        
        genres = cls.parse_array_field(doc.get('genres', ''))
        sorted_genres = sorted(genres)
        
        companies = cls.parse_array_field(doc.get('production_companies', ''))
        countries = cls.parse_array_field(doc.get('production_countries', ''))
        
        runtime = doc.get('runtime', 0)                         #?added
        
        return {
            "_id": doc.get('id'),
            "title": doc.get('title', ''),
            "original_title": doc.get('original_title', ''),
            "overview": doc.get('overview', ''),
            "tagline": doc.get('tagline', ''),
            
            "ratings": {
                "vote_average": vote_avg,
                "vote_count": vote_count,
                "popularity": doc.get('popularity', 0),
                "quality_tier": quality_tier                    #added
            },
            
            "release_info": {
                "status": doc.get('status', ''),
                "year": release_date.get('year'),               #*modified Flattened izravnavanje release_date
                "month": release_date.get('month'),
                "day": release_date.get('day'),
                "decade": release_date.get('decade'),
                "full_date": release_date.get('full_date'),     #*modified Smanji dubinu release_date -> year, month, day, decade, full_date
                
                "original_language": doc.get('original_language', ''),
                "spoken_languages": cls.parse_array_field(doc.get('spoken_languages', ''))
            },
            
            "content_info": {
                "adult": doc.get('adult', False),
                "runtime": runtime,
                "genres": genres,
                "sorted_genres": sorted_genres                      #added
            },
            
            "financial": {
                "budget": budget,
                "revenue": revenue,
                "profit": profit,                              #added
                "roi": roi,                                    #added
                "is_profitable": is_profitable,                #added
                "budget_category": budget_category             #added
            },
            
            "production": {
                "companies": companies,
                "countries": countries,
                "company_count": len(companies),               #added 
                "country_count": len(countries)                #added
            },
            
            "media": {
                "poster_path": doc.get('poster_path', ''),
                "backdrop_path": doc.get('backdrop_path', ''),
                "homepage": doc.get('homepage', ''),
                "imdb_id": doc.get('imdb_id', '')
            },
            
            "keywords": cls.parse_array_field(doc.get('keywords', '')),        
        }
