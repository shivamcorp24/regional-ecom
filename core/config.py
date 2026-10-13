from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_port: int
    postgres_host: str
    data_dir: str = "data"

    @property
    def database_url(self) -> URL:
        """SQLAlchemy URL for the warehouse.
        
        Uses URL.create to prevent special character corruption in passwords 
        and explicitly pins psycopg2 (since SQLAlchemy 2.x defaults to psycopg3).
        """
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )

    def masked_database_url(self) -> str:
        """URL with the password redacted, safe for logs."""
        return self.database_url.render_as_string(hide_password=True)

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """Clear cached settings (useful for tests)."""
    get_settings.cache_clear()


__all__ = ["Settings", "get_settings", "reset_settings_cache"]