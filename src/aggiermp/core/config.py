"""
Configuration management for AggieRMP application.
"""

from pathlib import Path
from typing import Optional, Any
from urllib.parse import quote_plus
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
    # Required in the X-Admin-Key header for admin endpoints (e.g. /cache/clear).
    # When unset, those endpoints are disabled.
    admin_api_key: Optional[str] = Field(None, alias="admin_api_key")
    # Server-only read access to Cronitor; never use a NEXT_PUBLIC_ variable.
    cronitor_api_key: Optional[str] = None
    cronitor_monitor_keys: str = "CX7Ga9,PpJGqD,YStGRp,FnepfT"
    cronitor_environment: str = "production"
    # Shared secret the website's server-side renderer sends in the
    # X-Internal-Key header. Matching requests skip the per-IP rate limits
    # (SSR traffic all comes from a few Vercel IPs). When unset, nothing is
    # exempt.
    internal_api_key: Optional[str] = Field(None, alias="internal_api_key")

    # Where the backend asks the website to refresh its cached course and
    # professor pages after data changes (POST, X-Revalidate-Secret header).
    # When either is unset, page revalidation is skipped.
    site_revalidate_url: Optional[str] = Field(None, alias="site_revalidate_url")
    revalidate_secret: Optional[str] = Field(None, alias="revalidate_secret")

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

    # Per-user limits. The watcher reads section status the scraper already
    # collects, so watches are cheap; this only stops scripted abuse.
    max_tracked_sections_per_user: int = Field(100, alias="max_tracked_sections_per_user")

    # Email (SMTP) for fallback notifications
    smtp_host: Optional[str] = Field(None, alias="smtp_host")
    smtp_port: int = Field(587, alias="smtp_port")
    smtp_user: Optional[str] = Field(None, alias="smtp_user")
    smtp_password: Optional[str] = Field(None, alias="smtp_password")
    smtp_from_address: str = Field("noreply@aggiesbp.com", alias="smtp_from_address")

    # CORS: comma-separated extra origins (e.g. preview URLs). Production website
    # should still be set via SUPERTOKENS_WEBSITE_DOMAIN; previews need to be
    # allowed explicitly or via cors_allow_vercel_previews regex below.
    cors_origins_extra: str = Field(
        default="",
        alias="cors_origins_extra",
        description="Comma-separated list of additional allowed origins",
    )
    # When True, any origin matching https://*.vercel.app is allowed (preview deploys).
    cors_allow_vercel_previews: bool = Field(
        default=True,
        alias="cors_allow_vercel_previews",
    )

    class Config:
        env_file = str(Path(__file__).parent.parent.parent.parent / ".env")
        env_file_encoding = "utf-8"
        # Ignore extra fields like database_id, universities_collection_id etc.
        extra = "ignore"

    @model_validator(mode='after')
    def assemble_db_connection(self) -> 'Settings':
        if not self.database_url:
            user = quote_plus(self.db_user)
            password = quote_plus(self.db_password)
            self.database_url = f"postgresql://{user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"
        return self


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings."""
    return settings
