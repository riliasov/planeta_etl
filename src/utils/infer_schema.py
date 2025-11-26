"""Автоматическое определение SQL-типов данных на основе содержимого Google Sheets."""
import pandas as pd
import re
from dateutil import parser

from src.config import load_config
from src.sheets import get_sheets_client, read_sheet_data

def infer_sql_type(series):
    """
    Определяет SQL тип для pandas Series.
    """
    # Удаляем пустые значения
    clean_series = series.dropna().astype(str)
    clean_series = clean_series[clean_series != '']
    
    if len(clean_series) == 0:
        return "TEXT" # По умолчанию, если пусто

    sample = clean_series.tolist()
    
    # 1. Проверка на BOOLEAN (Да/Нет, True/False)
    bool_patterns = {'true', 'false', 'да', 'нет', 'yes', 'no', '+', '-'}
    if all(str(x).lower() in bool_patterns for x in sample):
        return "BOOLEAN"

    # 2. Проверка на INTEGER
    # Разрешаем пробелы как разделители тысяч "1 000"
    try:
        # Убираем пробелы и проверяем, является ли числом
        cleaned_nums = [x.replace(' ', '').replace('\xa0', '') for x in sample]
        if all(x.isdigit() or (x.startswith('-') and x[1:].isdigit()) for x in cleaned_nums):
            return "INTEGER"
    except:
        pass

    # 3. Проверка на NUMERIC/FLOAT
    try:
        # Заменяем запятую на точку, убираем пробелы
        cleaned_floats = [x.replace(',', '.').replace(' ', '').replace('\xa0', '') for x in sample]
        # Проверяем конвертацию
        pd.to_numeric(cleaned_floats)
        return "NUMERIC(10,2)"
    except:
        pass

    # 4. Проверка на DATE / TIMESTAMP
    # Ищем паттерны даты DD.MM.YYYY или YYYY-MM-DD
    date_pattern = re.compile(r'^\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(\s\d{1,2}:\d{2})?$')
    if all(bool(date_pattern.match(str(x).strip())) for x in sample[:50]): # Проверяем первые 50 для скорости
        # Пробуем распарсить
        try:
            for x in sample[:20]:
                parser.parse(str(x), dayfirst=True)
            
            # Если есть время - TIMESTAMP, иначе DATE
            if any(':' in str(x) for x in sample[:20]):
                return "TIMESTAMP"
            return "DATE"
        except:
            pass

    # 5. Fallback
    return "TEXT"

def clean_column_name(col_name):
    """Превращает 'Дата рождения' в 'data_rozhdeniya' или транслит"""
    if not col_name:
        return "col_unknown"
    
    # Простая транслитерация и очистка
    mapping = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        ' ': '_', '-': '_', '.': '', ',': '', '/': '_', '(': '', ')': ''
    }
    
    clean = str(col_name).lower()
    result = ''
    for char in clean:
        result += mapping.get(char, char)
    
    # Убираем лишние символы
    result = re.sub(r'[^a-z0-9_]', '', result)
    result = re.sub(r'_+', '_', result).strip('_')
    
    # Если начинается с цифры, добавляем префикс
    if result and result[0].isdigit():
        result = 'col_' + result
        
    return result or "col_unnamed"

def analyze_sources():
    print("🕵️‍♂️ Анализ типов данных во всех источниках...\n")
    
    config = load_config()
    gc = get_sheets_client(config)
    sources = config.get('SOURCES', {})
    
    schema_definitions = {}
    
    for source_name, source_config in sources.items():
        print(f"📦 Анализ {source_name}...")
        
        spreadsheet_id = source_config.get('spreadsheet_id')
        sheet_identifiers = source_config.get('sheet_identifiers', [])
        ranges = source_config.get('ranges', {})
        use_gid = source_config.get('use_gid', False)
        
        if not sheet_identifiers or spreadsheet_id.startswith("УКАЖИТЕ"):
            print(f"   ⚠️ Пропуск (не настроен)")
            continue
            
        # Берем первый лист источника для анализа схемы
        sheet_id = sheet_identifiers[0]
        range_name = ranges.get(sheet_id)
        
        try:
            data = read_sheet_data(gc, spreadsheet_id, sheet_id, range_name, use_gid)
            if not data or len(data) < 2:
                print("   ⚠️ Нет данных")
                continue
                
            headers = data[0]
            # Создаем DataFrame
            df = pd.DataFrame(data[1:], columns=headers)
            
            table_schema = []
            
            for col in df.columns:
                sql_type = infer_sql_type(df[col])
                clean_name = clean_column_name(col)
                
                # Обработка дубликатов имен колонок
                original_clean_name = clean_name
                counter = 1
                existing_names = [x['name'] for x in table_schema]
                while clean_name in existing_names:
                    clean_name = f"{original_clean_name}_{counter}"
                    counter += 1
                
                table_schema.append({
                    "original": col,
                    "name": clean_name,
                    "type": sql_type
                })
            
            schema_definitions[source_name] = table_schema
            print(f"   ✅ Определено {len(table_schema)} колонок")
            
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")

    # Генерация SQL
    print("\n" + "="*50)
    print("ПРЕДЛАГАЕМАЯ СХЕМА STAGING (SILVER LAYER)")
    print("="*50 + "\n")
    
    sql_output = ""
    
    for table, columns in schema_definitions.items():
        sql_output += f"-- Таблица для {table}\n"
        sql_output += f"CREATE TABLE IF NOT EXISTS staging.{table} (\n"
        sql_output += "    id SERIAL PRIMARY KEY,\n"
        sql_output += "    source_row_id INTEGER,\n"
        
        for col in columns:
            sql_output += f"    {col['name']:<30} {col['type']},\n"
            
        sql_output += "    imported_at TIMESTAMP DEFAULT NOW()\n"
        sql_output += ");\n\n"
        
        # Вывод маппинга для пользователя
        print(f"📌 {table}")
        for col in columns:
            print(f"   {col['original']:<30} -> {col['name']:<30} {col['type']}")
        print("-" * 50)

    # Сохраняем в файл
    with open('src/db/inferred_schema.sql', 'w', encoding='utf-8') as f:
        f.write("CREATE SCHEMA IF NOT EXISTS staging;\n\n")
        f.write(sql_output)
        
    print("\n💾 SQL схема сохранена в src/db/inferred_schema.sql")

if __name__ == '__main__':
    analyze_sources()
