"""
Скрипт для проверки чтения данных с конкретными ranges из sources.json.
Показывает структуру данных для согласования.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.config import load_config
from src.sheets import get_sheets_client
import pandas as pd
import json


def check_ranges_and_structure():
    """
    Читает данные с указанными ranges и показывает структуру.
    """
    print("=" * 80)
    print("ПРОВЕРКА RANGES И СТРУКТУРЫ ДАННЫХ")
    print("=" * 80)
    
    config = load_config()
    gc = get_sheets_client(config)
    sources = config.get('SOURCES', {})
    
    results = {}
    
    for source_name, source_config in sources.items():
        print(f"\n{'=' * 80}")
        print(f"📊 Источник: {source_name}")
        print(f"{'=' * 80}")
        
        spreadsheet_id = source_config.get('spreadsheet_id')
        sheet_names = source_config.get('sheet_names', [])
        ranges = source_config.get('ranges', {})
        
        if spreadsheet_id == "SPREADSHEET_ID_HERE":
            print("⚠ Пропускаем - не настроен")
            continue
        
        source_results = {}
        
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"✓ Таблица: {spreadsheet.title}")
            
            for sheet_name in sheet_names:
                print(f"\n  📄 Лист: {sheet_name}")
                
                if sheet_name not in ranges:
                    print(f"    ⚠ Range не указан в конфигурации для '{sheet_name}'")
                    continue
                
                range_str = ranges[sheet_name]
                full_range = f"{sheet_name}!{range_str}"
                
                print(f"    📍 Range: {range_str}")
                
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    
                    # Читаем с указанным range
                    data = worksheet.get(range_str)
                    
                    if not data:
                        print(f"    ⚠ Диапазон пустой")
                        continue
                    
                    # Анализируем данные
                    total_rows = len(data)
                    total_cols = len(data[0]) if data else 0
                    
                    print(f"    ✓ Прочитано: {total_rows} строк × {total_cols} столбцов")
                    
                    # Проверяем, есть ли заголовок
                    if total_rows > 0:
                        headers = data[0]
                        print(f"\n    📋 Заголовки ({len(headers)}):")
                        for i, header in enumerate(headers, 1):
                            print(f"       {i:2d}. {header if header else '<пусто>'}")
                        
                        # Показываем первую строку данных
                        if total_rows > 1:
                            print(f"\n    📝 Первая строка данных:")
                            first_row = data[1]
                            for i, (header, value) in enumerate(zip(headers, first_row), 1):
                                if value:  # Показываем только непустые
                                    display_value = str(value)[:50]
                                    print(f"       {header if header else f'Col{i}'}: {display_value}")
                    
                    # Сохраняем результат
                    source_results[sheet_name] = {
                        'range': range_str,
                        'rows': total_rows,
                        'columns': total_cols,
                        'headers': data[0] if data else []
                    }
                    
                except Exception as e:
                    print(f"    ✗ Ошибка при чтении: {e}")
            
            results[source_name] = source_results
            
        except Exception as e:
            print(f"✗ Ошибка: {e}")
    
    # Сохраняем результаты
    output_path = 'tests/ranges_validation.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'=' * 80}")
    print(f"✓ Результаты сохранены в: {output_path}")
    print(f"{'=' * 80}")


if __name__ == '__main__':
    check_ranges_and_structure()
