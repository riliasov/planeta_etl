import json
from typing import Dict

from src.core.etl_pipeline import ETLPipeline
from src.config import load_config
import sqlalchemy

class HistoricalSyncPipeline(ETLPipeline):
    def get_source_mapping(self) -> Dict[str, str]:
        return {
            'historical_sales': 'sales_hst',
            'historical_expenses': 'expenses_hst',
            'historical_trainings': 'trainings_hst',
            'clients_data': 'clients_hst'
        }
    
    def get_column_mappings(self) -> Dict[str, Dict[str, str]]:
        # Загружаем маппинг из файла
        mapping_path = os.path.join(os.path.dirname(__file__), '../config/column_mappings.json')
        if os.path.exists(mapping_path):
            with open(mapping_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

def run_historical_sync():
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ Ошибка: Нет подключения к БД")
        return
        
    engine = sqlalchemy.create_engine(db_url)
    
    pipeline = HistoricalSyncPipeline(config, engine)
    pipeline.run()

if __name__ == "__main__":
    run_historical_sync()
