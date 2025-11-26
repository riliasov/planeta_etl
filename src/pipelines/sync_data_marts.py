"""
Sync Data Marts Pipeline - Синхронизация витрин данных с Google Sheets.

Процесс:
1. Читает данные из staging таблиц (sales, trainings)
2. Агрегирует в pandas (локально, минимум нагрузки на Supabase)  
3. Экспортирует витрины в Google Sheets

Витрины:
- Client Balance: баланс занятий по клиентам (приобретено - списано)
"""
import sys
import os
import sqlalchemy
import time
from datetime import datetime

from src.config import load_config
from src.sheets import get_sheets_client
from src.data_marts.aggregator import build_all_datamarts
from src.data_marts.exporter import export_all_datamarts
from src.logger import get_logger

logger = get_logger(__name__)


def log_step(step_name, start_time=None):
    """
    Логирует этап с временной меткой.
    
    Args:
        step_name: Название этапа
        start_time: Время начала этапа (для расчета elapsed time)
    
    Returns:
        float: Текущее время (для следующего вызова)
    """
    current_time = time.time()
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    if start_time is None:
        # Начало этапа
        print(f"⏱️  [{timestamp}] {step_name}")
    else:
        # Конец этапа
        elapsed = current_time - start_time
        print(f"✅ [{timestamp}] {step_name} завершен за {elapsed:.2f}s")
    
    return current_time


def run_sync_data_marts():
    """Запускает синхронизацию всех витрин данных с детальным логированием."""
    script_start = time.time()
    
    logger.info("🚀 Запуск синхронизации Data Marts")
    
    # Инициализация
    step_start = time.time()
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        logger.error("❌ Ошибка: Нет подключения к БД")
        return
    
    engine = sqlalchemy.create_engine(db_url)
    gc = get_sheets_client(config)
    elapsed = time.time() - step_start
    logger.info(f"📦 Инициализация: {elapsed:.1f}s")
    
    try:
        # Построение витрин
        step_start = time.time()
        datamarts = build_all_datamarts(engine)
        elapsed = time.time() - step_start
        logger.info(f"📊 Построение витрин: {elapsed:.1f}s (Sales: {len(datamarts['sales'])}, Trainings: {len(datamarts['trainings'])}, Balance: {len(datamarts['balance'])} строк)")
        
        # Экспорт в Google Sheets
        step_start = time.time()
        export_all_datamarts(gc, datamarts)
        elapsed = time.time() - step_start
        logger.info(f"📤 Экспорт в Google Sheets: {elapsed:.1f}s")
        
        # Итог
        total_time = time.time() - script_start
        logger.info(f"✅ Синхронизация завершена: {total_time:.1f}s")
        
    except Exception as e:
        total_time = time.time() - script_start
        logger.error(f"❌ Ошибка ({total_time:.1f}s): {e}")
        import traceback
        logger.debug(traceback.format_exc())
    finally:
        engine.dispose()


if __name__ == "__main__":
    run_sync_data_marts()
