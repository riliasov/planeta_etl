"""
Модуль для загрузки данных в БД (Staging Area).
"""
import pandas as pd
import hashlib
import json
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import List, Dict, Any, Optional
from src.logger import get_logger
from src.core.constants import DB_BATCH_SIZE

logger = get_logger(__name__)

class DataLoader:
    """Загрузчик данных в Staging таблицы с поддержкой инкрементальной загрузки."""
    
    def __init__(self, engine: Engine):
        self.engine = engine

    def _calculate_row_hash(self, row: pd.Series) -> str:
        """Считает MD5 хеш строки для дедупликации."""
        # Преобразуем строку в JSON, чтобы гарантировать порядок и формат
        # date_format='iso' важен для дат
        row_json = row.to_json(date_format='iso', force_ascii=False)
        return hashlib.md5(row_json.encode('utf-8')).hexdigest()

    def load_staging(self, df: pd.DataFrame, table_name: str, source_name: str) -> int:
        """
        Загружает данные в staging таблицу.
        1. Считает хеши строк.
        2. Проверяет, какие хеши уже есть в БД.
        3. Загружает только новые.
        
        Args:
            df: DataFrame с данными
            table_name: Имя таблицы в БД
            source_name: Имя источника (для логов)
            
        Returns:
            Количество загруженных строк
        """
        if df.empty:
            logger.info(f"⚠️ Нет данных для загрузки в {table_name}")
            return 0

        # Добавляем хеш
        # Исключаем служебные поля из хеша, если они есть (но source_row_id нам нужен для уникальности?)
        # Обычно хеш считается от бизнес-данных.
        # Но здесь мы считаем от всего, что пришло из очистки.
        df['row_hash'] = df.apply(self._calculate_row_hash, axis=1)
        
        # Получаем существующие хеши из БД
        existing_hashes = set()
        try:
            with self.engine.connect() as conn:
                # Проверяем существование таблицы
                # Используем text() для безопасного выполнения
                check_table = text(f"SELECT to_regclass('staging.{table_name}')")
                if conn.execute(check_table).scalar() is not None:
                    query = text(f"SELECT row_hash FROM staging.{table_name}")
                    result = conn.execute(query)
                    existing_hashes = {row[0] for row in result}
        except Exception as e:
            logger.debug(f"Ошибка при получении хешей (возможно таблица пустая): {e}")

        # Фильтруем новые строки
        # Используем ~ (NOT) и isin
        new_records = df[~df['row_hash'].isin(existing_hashes)]
        
        if new_records.empty:
            logger.info(f"   ✅ Нет новых данных для {table_name} (все {len(df)} строк)")
            return 0
            
        logger.info(f"   🚀 Вставка {len(new_records)} новых строк...")
        
        # Загружаем
        try:
            # chunksize для больших объемов
            new_records.to_sql(
                table_name,
                self.engine,
                schema='staging',
                if_exists='append',
                index=False,
                chunksize=DB_BATCH_SIZE,
                method='multi' 
            )
            logger.info(f"   ✅ Загружено {len(new_records)} строк")
            return len(new_records)
            
        except Exception:
            logger.error(f"❌ Ошибка вставки в {table_name}")
            return 0

    def load_raw_json(self, data_list: List[Dict[str, Any]], table_name: str, spreadsheet_id: str, sheet_id: str) -> None:
        """Загрузка сырого JSON (если понадобится)."""
        pass
