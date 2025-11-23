"""Модуль конфигурации для загрузки переменных окружения и источников данных."""
import os
from dotenv import load_dotenv

def load_config():
    """
    Загружает конфигурацию приложения из переменных окружения и файлов.
    
    Returns:
        dict: Словарь с конфигурацией, включая:
            - SUPABASE_DB_URL: URL подключения к Supabase/PostgreSQL
            - GOOGLE_SHEETS_CREDENTIALS_FILE: Путь к JSON файлу с credentials
            - SOURCES: Словарь с описанием источников данных (из secrets/sources.json)
    """
    load_dotenv()
    
    config = {
        'SUPABASE_DB_URL': os.getenv('SUPABASE_DB_URL'),
        'GOOGLE_SHEETS_CREDENTIALS_FILE': os.getenv('GOOGLE_SHEETS_CREDENTIALS_FILE'),
    }

    # Auto-detect credentials in secrets if not set or not found
    if not config['GOOGLE_SHEETS_CREDENTIALS_FILE'] or not os.path.exists(config['GOOGLE_SHEETS_CREDENTIALS_FILE']):
        secrets_dir = os.path.join(os.path.dirname(__file__), '..', 'secrets')
        if os.path.exists(secrets_dir):
            for file in os.listdir(secrets_dir):
                if file.endswith('.json') and 'sources' not in file:
                    config['GOOGLE_SHEETS_CREDENTIALS_FILE'] = os.path.join(secrets_dir, file)
                    break
    
    if not config['GOOGLE_SHEETS_CREDENTIALS_FILE']:
        # Fallback to default expected path for error message clarity
        config['GOOGLE_SHEETS_CREDENTIALS_FILE'] = 'secrets/credentials.json'
    
    # Basic validation (DB URL не обязателен для тестирования подключения к Sheets)
    # if not config['SUPABASE_DB_URL']:
    #     raise ValueError("Missing SUPABASE_DB_URL environment variable")

    # Load sources
    sources_path = os.path.join(os.path.dirname(__file__), '..', 'secrets', 'sources.json')
    if os.path.exists(sources_path):
        import json
        with open(sources_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Удаляем комментарии из JSON (строки начинающиеся с "_comment" или "_note")
            sources_data = json.loads(content)
            # Фильтруем только валидные источники (не комментарии)
            config['SOURCES'] = {
                k: v for k, v in sources_data.items() 
                if not k.startswith('_') and isinstance(v, dict)
            }
    else:
        config['SOURCES'] = {}
        
    return config
