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

# Добавляем корень
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.config import load_config
from src.sheets import get_sheets_client
from src.data_marts.aggregator import build_all_datamarts
from src.data_marts.exporter import export_all_datamarts


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
    
    print("=" * 70)
    print("🚀 Запуск синхронизации Data Marts")
    print(f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # Этап 0: Инициализация
    step_start = log_step("📦 Инициализация подключений")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ Ошибка: Нет подключения к БД")
        return
    
    engine = sqlalchemy.create_engine(db_url)
    gc = get_sheets_client(config)
    log_step("📦 Инициализация", step_start)
    print()
    
    try:
        # Этап 1: Построение витрин
        step_start = log_step("📊 Построение витрин данных")
        datamarts = build_all_datamarts(engine)
        log_step("📊 Построение витрин", step_start)
        
        # Показываем статистику
        print()
        print("📈 Статистика витрин:")
        print(f"   • Sales Summary: {len(datamarts['sales'])} строк")
        print(f"   • Trainings Summary: {len(datamarts['trainings'])} строк")
        print(f"   • Client Balance: {len(datamarts['balance'])} строк")
        print()
        
        # Preview
        print("📋 Preview витрин:\n")
        print("  💰 Sales Summary (топ 5):")
        print(datamarts['sales'].head().to_string(index=False))
        print("\n  🏊 Trainings Summary (топ 5):")
        print(datamarts['trainings'].head().to_string(index=False))
        print("\n  📊 Client Balance (топ 10):")
        print(datamarts['balance'].head(10).to_string(index=False))
        print()
        
        # Этап 2: Экспорт в Google Sheets
        step_start = log_step("📤 Экспорт в Google Sheets")
        export_all_datamarts(gc, datamarts)
        log_step("📤 Экспорт в Google Sheets", step_start)
        
        # Итоговая статистика
        total_time = time.time() - script_start
        print()
        print("=" * 70)
        print("✅ Синхронизация завершена успешно!")
        print(f"⏱️  Общее время выполнения: {total_time:.2f}s ({total_time/60:.1f}m)")
        print("=" * 70)
        
    except Exception as e:
        total_time = time.time() - script_start
        print()
        print("=" * 70)
        print(f"❌ Ошибка при синхронизации: {e}")
        print(f"⏱️  Время до ошибки: {total_time:.2f}s")
        print("=" * 70)
        import traceback
        traceback.print_exc()
    finally:
        engine.dispose()


if __name__ == "__main__":
    run_sync_data_marts()
