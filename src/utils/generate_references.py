
import json
import pandas as pd
from collections import Counter

from src.config import load_config
from src.sheets import get_sheets_client, read_sheet_data

def normalize_name(name):
    if not name:
        return None
    return str(name).strip()

def generate_references():
    print("🚀 Запуск генерации справочников из исторических данных...")
    
    config = load_config()
    gc = get_sheets_client(config)
    
    # 1. Читаем Продажи (historical_sales)
    print("\n📊 Чтение исторических продаж...")
    sales_config = config['SOURCES'].get('historical_sales')
    if not sales_config:
        print("❌ Конфигурация historical_sales не найдена")
        return

    # Берем первый лист (обычно он один)
    sheet_id = sales_config['sheet_identifiers'][0]
    data_sales = read_sheet_data(
        gc, 
        sales_config['spreadsheet_id'], 
        sheet_id, 
        sales_config['ranges'][sheet_id], 
        sales_config['use_gid']
    )
    
    df_sales = pd.DataFrame(data_sales[1:], columns=data_sales[0])
    print(f"   Прочитано {len(df_sales)} строк продаж")

    # 2. Читаем Тренировки (historical_trainings)
    print("\n🏋️‍♂️ Чтение исторических тренировок...")
    trainings_config = config['SOURCES'].get('historical_trainings')
    if not trainings_config:
        print("❌ Конфигурация historical_trainings не найдена")
        return

    sheet_id_tr = trainings_config['sheet_identifiers'][0]
    data_trainings = read_sheet_data(
        gc, 
        trainings_config['spreadsheet_id'], 
        sheet_id_tr, 
        trainings_config['ranges'][sheet_id_tr], 
        trainings_config['use_gid']
    )
    
    df_trainings = pd.DataFrame(data_trainings[1:], columns=data_trainings[0])
    print(f"   Прочитано {len(df_trainings)} строк тренировок")

    # --- АНАЛИЗ ДАННЫХ ---
    
    # --- АНАЛИЗ ДАННЫХ ---
    
    # Используем Counter для подсчета частоты
    stats = {
        "employees": {
            "trainers": Counter(),
            "admins": Counter()
        },
        "products": Counter(),
        "types": Counter(),
        "categories": Counter()
    }
    
    # 1. Сотрудники
    if 'Тренер' in df_sales.columns:
        trainers = df_sales['Тренер'].dropna().apply(normalize_name).tolist()
        stats['employees']['trainers'].update(trainers)
        
    if 'Админ' in df_sales.columns:
        admins = df_sales['Админ'].dropna().apply(normalize_name).tolist()
        stats['employees']['admins'].update(admins)
        
    if 'Сотрудник' in df_trainings.columns:
        trainers_tr = df_trainings['Сотрудник'].dropna().apply(normalize_name).tolist()
        stats['employees']['trainers'].update(trainers_tr)

    # 2. Продукты
    if 'Продукт' in df_sales.columns:
        products = df_sales['Продукт'].dropna().apply(normalize_name).tolist()
        stats['products'].update(products)
        
    # 3. Типы и Категории
    if 'Тип' in df_sales.columns:
        stats['types'].update(df_sales['Тип'].dropna().apply(normalize_name).tolist())
    if 'Категория' in df_sales.columns:
        stats['categories'].update(df_sales['Категория'].dropna().apply(normalize_name).tolist())
        
    if 'Тип' in df_trainings.columns:
        stats['types'].update(df_trainings['Тип'].dropna().apply(normalize_name).tolist())
    if 'Категория' in df_trainings.columns:
        stats['categories'].update(df_trainings['Категория'].dropna().apply(normalize_name).tolist())

    # --- СОХРАНЕНИЕ ---
    
    # Формируем красивый вывод: "Значение (Кол-во)"
    def format_counter(counter):
        return [f"{k} ({v})" for k, v in counter.most_common()]

    output = {
        "employees": {
            "trainers_by_count": format_counter(stats['employees']['trainers']),
            "admins_by_count": format_counter(stats['employees']['admins'])
        },
        "products_by_count": format_counter(stats['products']),
        "types_by_count": format_counter(stats['types']),
        "categories_by_count": format_counter(stats['categories'])
    }
    
    output_file = 'references_stats.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
        
    print(f"\n✅ Статистика справочников сохранена в '{output_file}'")
    print("\n🏆 Топ-5 Тренеров:")
    for item in output['employees']['trainers_by_count'][:5]:
        print(f"   - {item}")
        
    print("\n🏆 Топ-5 Продуктов:")
    for item in output['products_by_count'][:5]:
        print(f"   - {item}")

if __name__ == '__main__':
    generate_references()
