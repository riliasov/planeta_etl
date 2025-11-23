"""
Скрипт для детального анализа структуры данных из Google Sheets.
Создает JSON отчет с информацией о каждом источнике.
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.config import load_config
from src.sheets import get_sheets_client
import pandas as pd


def analyze_data_structure():
    """
    Анализирует структуру данных из всех источников и создает детальный отчет.
    """
    config = load_config()
    gc = get_sheets_client(config)
    sources = config.get('SOURCES', {})
    
    analysis = {
        'total_sources': len(sources),
        'sources': {}
    }
    
    for source_name, source_config in sources.items():
        print(f"Анализирую: {source_name}...")
        
        spreadsheet_id = source_config.get('spreadsheet_id')
        sheet_names = source_config.get('sheet_names', [])
        
        if spreadsheet_id == "SPREADSHEET_ID_HERE":
            continue
        
        source_analysis = {
            'spreadsheet_id': spreadsheet_id,
            'sheets': {}
        }
        
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            source_analysis['spreadsheet_title'] = spreadsheet.title
            
            for sheet_name in sheet_names:
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    data = worksheet.get_all_values()
                    
                    if not data:
                        continue
                    
                    # Создаем DataFrame
                    df = pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame()
                    
                    sheet_analysis = {
                        'total_rows': len(data) - 1,  # Минус заголовок
                        'total_columns': len(data[0]) if data else 0,
                        'columns': data[0] if data else [],
                        'sample_data': []
                    }
                    
                    # Берем первые 3 строки для примера
                    if not df.empty:
                        for idx, row in df.head(3).iterrows():
                            sample_row = {}
                            for col, val in row.items():
                                if val:  # Только непустые значения
                                    sample_row[col] = str(val)[:100]  # Ограничиваем длину
                            sheet_analysis['sample_data'].append(sample_row)
                    
                    # Определяем типы данных (упрощенно)
                    column_types = {}
                    if not df.empty:
                        for col in df.columns:
                            non_empty = df[col][df[col] != ''].head(10)
                            if len(non_empty) > 0:
                                # Проверяем, похоже ли на число
                                try:
                                    pd.to_numeric(non_empty)
                                    column_types[col] = 'numeric'
                                except:
                                    # Проверяем, похоже ли на дату
                                    try:
                                        pd.to_datetime(non_empty, dayfirst=True)
                                        column_types[col] = 'date'
                                    except:
                                        column_types[col] = 'text'
                    
                    sheet_analysis['column_types'] = column_types
                    
                    source_analysis['sheets'][sheet_name] = sheet_analysis
                    
                except Exception as e:
                    print(f"  Ошибка при анализе листа {sheet_name}: {e}")
            
            analysis['sources'][source_name] = source_analysis
            
        except Exception as e:
            print(f"Ошибка при анализе {source_name}: {e}")
    
    # Сохраняем отчет
    output_path = 'tests/data_structure_analysis.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ Анализ завершен. Отчет сохранен в: {output_path}")
    return analysis


if __name__ == '__main__':
    analysis = analyze_data_structure()
    
    # Выводим краткую сводку
    print("\n" + "=" * 80)
    print("КРАТКАЯ СВОДКА")
    print("=" * 80)
    
    for source_name, source_data in analysis['sources'].items():
        print(f"\n{source_name}:")
        print(f"  Таблица: {source_data.get('spreadsheet_title', 'N/A')}")
        for sheet_name, sheet_data in source_data['sheets'].items():
            print(f"  - {sheet_name}: {sheet_data['total_rows']} строк, {sheet_data['total_columns']} столбцов")
