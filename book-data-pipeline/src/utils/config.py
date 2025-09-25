from pydantic_settings  import BaseSettings

class Settings(BaseSettings):
    # Rate limiting
    request_delay: float = 1.0  # seconds between requests
    max_retries: int = 3
    
    # Data quality rules
    min_price: float = 0.0
    max_price: float = 1000.0
    valid_currencies: list = ["USD", "GBP", "EUR"]
    
    # Matching thresholds
    title_similarity_threshold: float = 0.8
    author_similarity_threshold: float = 0.7

    class Config:
        env_file = ".env"

settings = Settings()