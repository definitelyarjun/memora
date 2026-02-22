"""Application configuration using pydantic-settings."""

from pathlib import Path

from pydantic_settings import BaseSettings

# Always resolve .env relative to this file (backend/.env) regardless of cwd
_ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    """Global application settings loaded from environment / .env file."""

    app_name: str = "FoundationIQ"
    app_version: str = "0.1.0"
    debug: bool = False

    # File upload constraints
    max_upload_size_mb: int = 50

    # Tabular ingestion
    allowed_tabular_extensions: list[str] = [".csv", ".xlsx", ".xls"]

    # Document ingestion
    allowed_document_extensions: list[str] = [".pdf", ".docx", ".txt"]

    # Session store
    session_ttl_minutes: int = 60

    # LLM
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"

    model_config = {"env_file": str(_ENV_FILE), "extra": "ignore"}


settings = Settings()
