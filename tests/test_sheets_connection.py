"""
Тестовый скрипт для проверки подключения к Google Sheets и чтения данных.
Читает только первые несколько строк из каждого источника.
"""
import sys
import os

# Добавляем корневую папку в путь для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.config import load_config
from src.sheets import get_sheets_client
import pandas as pd


def test_read_sheets():
    """
    Тестирует подключение к Google Sheets и читает первые 10 строк из каждого источника.
    """
    print("=" * 80)
    print("Тестовое чтение данных из Google Sheets")
    print("=" * 80)
    
    try:
        # Загружаем конфигурацию
        config = load_config()
        print(f"\n✓ Конфигурация загружена")
        print(f"  - Credentials: {config['GOOGLE_SHEETS_CREDENTIALS_FILE']}")
        print(f"  - Источников данных: {len(config.get('SOURCES', {}))}")
        
        # Подключаемся к Google Sheets
        gc = get_sheets_client(config)
        print(f"\n✓ Подключение к Google Sheets установлено")
        
        sources = config.get('SOURCES', {})
        
        if not sources:
            print("\n⚠ Файл sources.json не найден или пуст")
            print("  Создайте secrets/sources.json из sources.json.example")
            return
        
        # Читаем данные из каждого источника
        for source_name, source_config in sources.items():
            print(f"\n{'=' * 80}")
            print(f"Источник: {source_name}")
            print(f"{'=' * 80}")
            
            spreadsheet_id = source_config.get('spreadsheet_id')
            sheet_names = source_config.get('sheet_names', [])
            ranges = source_config.get('ranges', {})
            
            if spreadsheet_id == "SPREADSHEET_ID_HERE":
                print(f"⚠ Пропускаем - не настроен spreadsheet_id")
                continue
            
            try:
                spreadsheet = gc.open_by_key(spreadsheet_id)
                print(f"✓ Таблица открыта: {spreadsheet.title}")
                
                for sheet_name in sheet_names:
                    print(f"\n  Лист: {sheet_name}")
                    
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                        
                        # Определяем range для чтения
                        # Для теста читаем только первые 10 строк
                        if sheet_name in ranges:
                            custom_range = ranges[sheet_name]
                            # Ограничиваем до 10 строк для теста
                            if ':' in custom_range:
                                cols = custom_range.split(':')[0][0]  # Берем букву столбца
                                test_range = f"{sheet_name}!{cols}1:{cols[-1] if cols[-1].isalpha() else cols}10"
                            else:
                                test_range = f"{sheet_name}!A1:Z10"
                        else:
                            test_range = f"{sheet_name}!A1:Z10"
                        
                        # Читаем данные
                        data = worksheet.get_all_values()
                        
                        if not data:
                            print(f"    ⚠ Лист пустой")
                            continue
                        
                        # Создаем DataFrame
                        df = pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame()
                        
                        # Ограничиваем до 5 строк для вывода
                        df_sample = df.head(5)
                        
                        print(f"    ✓ Строк всего: {len(data) - 1}")
                        print(f"    ✓ Столбцов: {len(data[0]) if data else 0}")
                        print(f"\n    Первые 5 строк:")
                        print(f"    {'-' * 70}")
                        
                        if not df_sample.empty:
                            # Выводим информацию о колонках
                            print(f"    Колонки: {', '.join(df_sample.columns.tolist())}")
                            print(f"\n    Пример данных:")
                            # Показываем первые 2 строки для компактности
                            for idx, row in df_sample.head(2).iterrows():
                                print(f"      Строка {idx + 1}:")
                                for col, val in row.items():
                                    if val:  # Показываем только непустые значения
                                        print(f"        {col}: {val[:50] if len(str(val)) > 50 else val}")
                        
                    except Exception as e:
                        print(f"    ✗ Ошибка при чтении листа {sheet_name}: {e}")
                
            except Exception as e:
                print(f"✗ Ошибка при открытии таблицы: {e}")
        
        print(f"\n{'=' * 80}")
        print("Тестирование завершено")
        print(f"{'=' * 80}")
        
    except Exception as e:
        print(f"\n✗ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    test_read_sheets()
