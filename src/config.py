"""Type-safe configuration management using Pydantic."""
import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    """Application configuration with validation."""
    
    # Database
    supabase_db_url: str = Field(
        ..., 
        description="PostgreSQL/Supabase database connection URL"
    )
    
    # Google Sheets
    google_sheets_credentials_file: Optional[str] = Field(
        None,
        description="Path to Google Sheets service account JSON file"
    )
    
    # Sources (loaded separately from JSON)
    _sources: Dict[str, Any] = {}
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )
    
    @field_validator('supabase_db_url')
    @classmethod
    def validate_db_url(cls, v: str) -> str:
        """Validate database URL format."""
        if not v.startswith(('postgres://', 'postgresql://')):
            raise ValueError('Database URL must start with postgres:// or postgresql://')
        return v
    
    @field_validator('google_sheets_credentials_file')
    @classmethod
    def validate_credentials_file(cls, v: Optional[str]) -> Optional[str]:
        """Auto-detect credentials file if not specified."""
        if v and Path(v).exists():
            return v
        
        # Auto-detect in secrets directory
        secrets_dir = Path(__file__).parent.parent / 'secrets'
        if secrets_dir.exists():
            for file in secrets_dir.iterdir():
                if file.suffix == '.json' and 'sources' not in file.name:
                    return str(file)
        
        return v
    
    def load_sources(self) -> Dict[str, Any]:
        """Load sources configuration from JSON file."""
        if self._sources:
            return self._sources
        
        # Try src/sources.json first (new location)
        sources_path = Path(__file__).parent / 'sources.json'
        
        # Fallback to secrets/sources.json (old location)
        if not sources_path.exists():
            sources_path = Path(__file__).parent.parent / 'secrets' / 'sources.json'
        
        if sources_path.exists():
            with open(sources_path, 'r', encoding='utf-8') as f:
                sources_data = json.load(f)
                # Filter out comments (keys starting with '_')
                self._sources = {
                    k: v for k, v in sources_data.items()
                    if not k.startswith('_') and isinstance(v, dict)
                }
        
        return self._sources
    
    @property
    def sources(self) -> Dict[str, Any]:
        """Get sources configuration."""
        return self.load_sources()


# Singleton instance
_config: Optional[AppConfig] = None


def load_config() -> dict:
    """
    Load and return configuration (backward compatible with old dict interface).
    
    Returns:
        dict: Configuration dictionary with keys:
            - SUPABASE_DB_URL
            - GOOGLE_SHEETS_CREDENTIALS_FILE
            - SOURCES
    """
    global _config
    
    if _config is None:
        # Try loading from secrets/.env first
        env_path = Path(__file__).parent.parent / 'secrets' / '.env'
        if env_path.exists():
            _config = AppConfig(_env_file=str(env_path))
        else:
            _config = AppConfig()
    
    # Return dict for backward compatibility
    return {
        'SUPABASE_DB_URL': _config.supabase_db_url,
        'GOOGLE_SHEETS_CREDENTIALS_FILE': _config.google_sheets_credentials_file,
        'SOURCES': _config.sources,
    }


def get_config() -> AppConfig:
    """
    Get typed configuration object.
    
    Returns:
        AppConfig: Pydantic config model
    """
    global _config
    
    if _config is None:
        env_path = Path(__file__).parent.parent / 'secrets' / '.env'
        if env_path.exists():
            _config = AppConfig(_env_file=str(env_path))
        else:
            _config = AppConfig()
    
    return _config
