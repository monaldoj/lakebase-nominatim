"""Configuration settings for the Nominatim service."""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # PostgreSQL connection settings
    pg_host: str = Field(default="localhost", env="PGHOST")
    pg_user: str = Field(default="postgres", env="PGUSER")
    pg_password: str = Field(default="", env="PGPASSWORD")
    pg_database: str = Field(default="nominatim", env="PGDATABASE")
    pg_port: int = Field(default=5432, env="PGPORT")
    pg_sslmode: str = Field(default="prefer", env="PGSSLMODE")

    # Nominatim settings
    nominatim_schema: str = Field(default="public", env="NOMINATIM_SCHEMA")

    # API settings
    api_title: str = "Nominatim Geocoding API"
    api_description: str = "OpenStreetMap-based geocoding service"
    api_version: str = "1.0.0"

    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields in .env that aren't defined in Settings

    @property
    def postgres_dsn(self) -> str:
        """Build PostgreSQL connection string."""
        password_part = f":{self.pg_password}" if self.pg_password else ""
        return (
            f"postgresql://{self.pg_user}{password_part}@{self.pg_host}:{self.pg_port}"
            f"/{self.pg_database}?sslmode={self.pg_sslmode}"
        )

    @property
    def postgres_dsn_dict(self) -> dict:
        """Return PostgreSQL connection parameters as dictionary."""
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "user": self.pg_user,
            "password": self.pg_password,
            "database": self.pg_database,
            "sslmode": self.pg_sslmode,
        }


# Global settings instance
settings = Settings()
