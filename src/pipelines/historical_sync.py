import sys
import os
import pandas as pd
import sqlalchemy
import traceback

# Добавляем корень
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.config import load_config
from src.sheets import get_sheets_client, read_sheet_data
from src.etl.loader import DataLoader
from src.etl.data_cleaner import clean_dataframe
from src.utils.infer_schema import clean_column_name
from src.logger import get_logger

logger = get_logger(__name__)

def run_historical_sync():
    logger.info("📚 Запуск синхронизации ИСТОРИЧЕСКИХ данных (Historical Sync)...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        logger.info("❌ Ошибка: Нет подключения к БД")
        return

    # Инициализация
    gc = get_sheets_client(config)
    engine = sqlalchemy.create_engine(db_url)
    loader = DataLoader(engine)
    
    sources = config.get('SOURCES', {})
    
    # 1. Historical Sales
    if 'historical_sales' in sources:
        process_source(gc, loader, sources['historical_sales'], 'historical_sales', 'sales_hst')
        
    # 2. Historical Expenses
    if 'historical_expenses' in sources:
        process_source(gc, loader, sources['historical_expenses'], 'historical_expenses', 'expenses_hst')

    # 3. Historical Trainings
    if 'historical_trainings' in sources:
        process_source(gc, loader, sources['historical_trainings'], 'historical_trainings', 'trainings_hst')

    # 4. Clients Data (обычно это история клиентов)
    if 'clients_data' in sources:
        process_source(gc, loader, sources['clients_data'], 'clients_data', 'clients_hst')

def process_source(gc, loader, source_config, source_name, target_table):
    logger.info(f"\n📦 Обработка {source_name} -> staging.{target_table}...")
    
    spreadsheet_id = source_config.get('spreadsheet_id')
    sheet_identifiers = source_config.get('sheet_identifiers', [])
    ranges = source_config.get('ranges', {})
    use_gid = source_config.get('use_gid', False)
    
    if not sheet_identifiers:
        logger.info("   ⚠️ Нет идентификаторов листов")
        return

    # Для исторических данных может быть несколько листов (например, по годам)
    # Проходим по всем
    for sheet_id in sheet_identifiers:
        range_name = ranges.get(sheet_id)
        logger.info(f"   📄 Лист: {sheet_id}...")
        
        try:
            data = read_sheet_data(gc, spreadsheet_id, sheet_id, range_name, use_gid)
            if not data or len(data) < 2:
                logger.info("      ⚠️ Нет данных или пустой лист")
                continue
                
            headers = data[0]
            
            # Уникализация заголовков
            seen = {}
            unique_headers = []
            for h in headers:
                h = str(h).strip()
                if h in seen:
                    seen[h] += 1
                    unique_headers.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    unique_headers.append(h)
            headers = unique_headers
            
            rows = data[1:]
            
            # Выравнивание колонок
            max_cols = len(headers)
            for row in rows:
                if len(row) > max_cols:
                    max_cols = len(row)
            
            if max_cols > len(headers):
                for i in range(len(headers) + 1, max_cols + 1):
                    headers.append(f"col_{i}")
                    
            padded_rows = []
            for row in rows:
                if len(row) < max_cols:
                    row = row + [None] * (max_cols - len(row))
                padded_rows.append(row)
                
            df = pd.DataFrame(padded_rows, columns=headers)
            
            # МАППИНГ КОЛОНОК
            rename_map = {}
            used_clean_names = {}
            
            # Специальная логика для trainings_hst?
            # В inferred_schema у нас trainings_hst имеет нормальные имена (data, nachalo...),
            # значит там заголовки были нормальные, а не даты.
            # Но проверим, если вдруг там тоже динамика.
            # Судя по схеме, там статические заголовки.
            
            for col in df.columns:
                clean = clean_column_name(col)
                
                if clean in used_clean_names:
                    used_clean_names[clean] += 1
                    clean = f"{clean}_{used_clean_names[clean]}"
                else:
                    used_clean_names[clean] = 0
                
                rename_map[col] = clean
                
            df_renamed = df.rename(columns=rename_map)
            
            # Добавляем метаданные
            df_renamed['source_row_id'] = range(2, len(df_renamed) + 2)
            
            # ОЧИСТКА ДАННЫХ
            df_cleaned = clean_dataframe(df_renamed, target_table)
            
            # Загрузка
            loader.load_staging(df_cleaned, target_table, source_name)
            
        except Exception as e:
            logger.info(f"❌ Ошибка обработки листа {sheet_id}:")
            traceback.print_exc()

if __name__ == "__main__":
    run_historical_sync()
