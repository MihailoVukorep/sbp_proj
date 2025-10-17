from typing import Dict, List, Any

class MovieDocument:
    
    @staticmethod
    def parse_date(date_value: Any) -> Dict[str, Any]:
        if not date_value:
            return {}
        
        try:
            if isinstance(date_value, str):
                parts = date_value.split('-')
                return {
                    "year": int(parts[0]),
                    "month": int(parts[1]),
                    "day": int(parts[2]),
                    "full_date": date_value
                }
            elif hasattr(date_value, 'year'):
                return {
                    "year": date_value.year,
                    "month": date_value.month,
                    "day": date_value.day,
                    "full_date": date_value.strftime('%Y-%m-%d')
                }
        except (ValueError, IndexError, AttributeError):
            return {"full_date": str(date_value)}
        
        return {}
    
    @staticmethod
    def parse_array_field(field_value: Any) -> List[str]:
        if not field_value or field_value == "":
            return []
        return [item.strip() for item in str(field_value).split(', ')]
    
    @classmethod
    def transform(cls, doc: Dict[str, Any]) -> Dict[str, Any]:
        release_date = cls.parse_date(doc.get('release_date'))
        
        return {
            "_id": f"tmdb_{doc.get('id')}",
            "tmdb_id": doc.get('id'),
            "title": doc.get('title', ''),
            "original_title": doc.get('original_title', ''),
            "overview": doc.get('overview', ''),
            "tagline": doc.get('tagline', ''),
            
            "ratings": {
                "vote_average": doc.get('vote_average', 0),
                "vote_count": doc.get('vote_count', 0),
                "popularity": doc.get('popularity', 0)
            },
            
            "release_info": {
                "status": doc.get('status', ''),
                "release_date": release_date,
                "original_language": doc.get('original_language', ''),
                "spoken_languages": cls.parse_array_field(doc.get('spoken_languages', ''))
            },
            
            "content_info": {
                "adult": doc.get('adult', False),
                "runtime": doc.get('runtime', 0),
                "genres": cls.parse_array_field(doc.get('genres', ''))
            },
            
            "financial": {
                "budget": doc.get('budget', 0),
                "revenue": doc.get('revenue', 0)
            },
            
            "production": {
                "companies": cls.parse_array_field(doc.get('production_companies', '')),
                "countries": cls.parse_array_field(doc.get('production_countries', ''))
            },
            
            "media": {
                "poster_path": doc.get('poster_path', ''),
                "backdrop_path": doc.get('backdrop_path', ''),
                "homepage": doc.get('homepage', ''),
                "imdb_id": doc.get('imdb_id', '')
            },
            
            "keywords": cls.parse_array_field(doc.get('keywords', ''))
        }