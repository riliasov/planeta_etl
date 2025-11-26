from typing import Dict

from src.core.etl_pipeline import ETLPipeline
from src.config import load_config
import sqlalchemy

class CurrentSyncPipeline(ETLPipeline):
    def get_source_mapping(self) -> Dict[str, str]:
        return {
            'current_sales': 'sales_cur',
            'current_expenses': 'expenses_cur',
            'current_trainings': 'trainings_cur'
        }
    
    def get_column_mappings(self) -> Dict[str, Dict[str, str]]:
        return {}  # Нет специального маппинга для current

    def _process_source(self, source_config: Dict, source_name: str, target_table: str):
        if target_table == 'trainings_cur':
            # Читаем без маппинга
            df = self.sheets_processor.read_and_transform(source_config, target_table, None)
            if df is not None and not df.empty:
                # Переименовываем по позиции (col_1, col_2...), пропуская source_row_id
                rename_map = {}
                col_idx = 1
                for col in df.columns:
                    if col == 'source_row_id':
                        continue
                    rename_map[col] = f"col_{col_idx}"
                    col_idx += 1
                
                df = df.rename(columns=rename_map)
                
                # Очистка и загрузка
                from src.etl.data_cleaner import clean_dataframe
                df_cleaned = clean_dataframe(df, target_table)
                self.loader.load_staging(df_cleaned, target_table, source_name)
        else:
            super()._process_source(source_config, source_name, target_table)

def run_current_sync():
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ Ошибка: Нет подключения к БД")
        return
        
    engine = sqlalchemy.create_engine(db_url)
    
    pipeline = CurrentSyncPipeline(config, engine)
    pipeline.run()

if __name__ == "__main__":
    run_current_sync()
