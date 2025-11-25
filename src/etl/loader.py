import hashlib
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

from src.logger import get_logger
logger = get_logger(__name__)

class DataLoader:
    def __init__(self, db_engine):
        self.engine = db_engine

    def calculate_row_hash(self, row_dict):
        """
        Вычисляет MD5 хэш строки для инкрементальной загрузки.
        Использует JSON представление словаря для стабильности.
        """
        # Сортируем ключи, чтобы порядок не влиял на хэш
        # Преобразуем все значения в строку, чтобы избежать проблем с типами
        row_str = json.dumps(row_dict, sort_keys=True, default=str, ensure_ascii=False)
        return hashlib.md5(row_str.encode('utf-8')).hexdigest()

    def load_staging(self, df, table_name, source_name):
        """
        Загружает DataFrame в staging таблицу с инкрементальной логикой.
        
        Args:
            df (pd.DataFrame): Данные для загрузки
            table_name (str): Имя таблицы в staging схеме (без префикса staging.)
            source_name (str): Имя источника (для логов)
        """
        if df.empty:
            logger.info(f"⚠️  Нет данных для загрузки в {table_name}")
            return 0

        logger.info(f"📥 Загрузка {len(df)} строк в staging.{table_name}...")

        # 1. Подготовка данных
        # Добавляем row_hash
        records = df.to_dict(orient='records')
        for row in records:
            row['row_hash'] = self.calculate_row_hash(row)
            
        # Преобразуем обратно в DF для удобства (или можно грузить списком словарей)
        df_to_load = pd.DataFrame(records)
        
        # 2. Получение существующих хэшей (для фильтрации)
        # Это оптимизация: вместо INSERT ON CONFLICT для каждой строки,
        # мы сначала узнаем, какие хэши уже есть, и отфильтруем их.
        # Для очень больших таблиц это может быть накладно, но для 20-50к строк нормально.
        
        existing_hashes = set()
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT row_hash FROM staging.{table_name}"))
                existing_hashes = {row[0] for row in result}
        except Exception as e:
            logger.error(f"⚠️  Ошибка чтения существующих хэшей (возможно таблица пуста): {e}")

        # 3. Фильтрация новых строк
        new_records = [
            row for row in records 
            if row['row_hash'] not in existing_hashes
        ]
        
        if not new_records:
            logger.info(f"   ✅ Нет новых данных для {table_name} (все {len(records)} строк уже в базе)")
            return 0
            
        logger.info(f"   🚀 Найдено {len(new_records)} новых/измененных строк. Вставка...")
        
        # 4. Вставка (Bulk Insert)
        # Используем pandas to_sql или sqlalchemy insert
        # Pandas to_sql с method='multi' работает неплохо для postgres
        
        df_new = pd.DataFrame(new_records)
        
        # Убедимся, что колонки совпадают с БД (лишние колонки pandas могут вызвать ошибку)
        # В идеале нужно маппить колонки, но пока полагаемся на совпадение имен из infer_schema
        
        try:
            df_new.to_sql(
                table_name,
                self.engine,
                schema='staging',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000 # Разбиваем на пачки
            )
            logger.info(f"   ✅ Успешно загружено {len(new_records)} строк.")
            return len(new_records)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при вставке в {table_name}: {e}")
            return 0

    def load_raw_json(self, data_list, table_name, spreadsheet_id, sheet_id):
        """
        Загружает сырые данные в raw таблицу (JSONB).
        Здесь мы обычно делаем TRUNCATE + INSERT или Append Only.
        Для raw истории лучше Append Only или полная перезаливка.
        """
        # Пока пропустим реализацию raw, сосредоточимся на staging
        pass
