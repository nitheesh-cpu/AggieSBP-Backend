"""
Configuration management for AggieRMP application.
"""

from pathlib import Path
from typing import Optional
from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    """Application settings."""

    # Application
    app_name: str = "AggieRMP"
    debug: bool = False
    version: str = "1.0.0"

    # Database
    database_url: Optional[str] = None
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "aggiermp"
    db_user: str = "postgres"
    db_password: str = ""

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

    @validator("database_url", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: dict) -> str:
        if isinstance(v, str):
            return v
        return f"postgresql://{values.get('db_user')}:{values.get('db_password')}@{values.get('db_host')}:{values.get('db_port')}/{values.get('db_name')}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings."""
    return settings
