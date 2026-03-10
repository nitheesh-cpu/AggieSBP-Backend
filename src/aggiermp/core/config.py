"""
Configuration management for AggieRMP application.
"""

from pathlib import Path
from typing import Optional, Any
from pydantic import Field, AnyUrl, model_validator
from pydantic_settings import BaseSettings  # type: ignore


class Settings(BaseSettings):
    """Application settings."""

    # Application
    app_name: str = "AggieRMP"
    debug: bool = False
    version: str = "1.0.0"

    # Database
    # Support both internal naming (db_*) and environmental naming (postgres_*)
    db_host: str = Field("localhost", alias="postgres_host")
    db_port: int = Field(5432, alias="postgres_port")
    db_name: str = Field("aggiermp", alias="postgres_database")
    db_user: str = Field("postgres", alias="postgres_user")
    db_password: str = Field("", alias="postgres_password")
    
    database_url: Optional[str] = None

    # Redis configuration
    redis_url: Optional[str] = Field(None, alias="redis_url")

    # Paths
    project_root: Path = Path(__file__).parent.parent.parent.parent
    data_dir: Path = project_root / "data"
    config_dir: Path = project_root / "config"

    # Scraping
    scraping_delay: float = 1.0
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # SuperTokens
    supertokens_connection_uri: str = "http://localhost:3567"
    supertokens_api_key: Optional[str] = None
    supertokens_app_name: str = "AggieRMP"
    supertokens_api_domain: str = "http://localhost:8000"
    supertokens_website_domain: str = "http://localhost:3000"

    # Novu
    novu_api_key: Optional[str] = Field(None, alias="novu_api_key")
    novu_workflow_id: str = "class-alert"

    # Web Push
    vapid_private_key: Optional[str] = Field(None, alias="vapid_private_key")
    vapid_contact_email: str = "mailto:support@AggieSBP.com"

    # Google OAuth
    google_oauth_client_id: Optional[str] = None
    google_oauth_client_secret: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Ignore extra fields like database_id, universities_collection_id etc.
        extra = "ignore"

    @model_validator(mode='after')
    def assemble_db_connection(self) -> 'Settings':
        if not self.database_url:
            self.database_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        return self


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings."""
    return settings
