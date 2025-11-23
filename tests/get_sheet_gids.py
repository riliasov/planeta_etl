"""
Тестовый скрипт для проверки работы с gid (Sheet ID).
Показывает gid для всех листов в таблице.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.config import load_config
from src.sheets import get_sheets_client


def show_all_sheet_gids():
    """
    Показывает gid для всех листов в каждой таблице из sources.json.
    """
    print("=" * 80)
    print("ПОЛУЧЕНИЕ GID ДЛЯ ВСЕХ ЛИСТОВ")
    print("=" * 80)
    print("\nGID (Sheet ID) - это уникальный идентификатор листа.")
    print("Он НЕ меняется при переименовании листа.\n")
    
    config = load_config()
    gc = get_sheets_client(config)
    sources = config.get('SOURCES', {})
    
    # Группируем по spreadsheet_id
    spreadsheets = {}
    for source_name, source_config in sources.items():
        spreadsheet_id = source_config.get('spreadsheet_id')
        if spreadsheet_id == "SPREADSHEET_ID_HERE":
            continue
        
        if spreadsheet_id not in spreadsheets:
            spreadsheets[spreadsheet_id] = []
        
        spreadsheets[spreadsheet_id].append({
            'source_name': source_name,
            'sheet_names': source_config.get('sheet_names', [])
        })
    
    # Читаем информацию о каждой таблице
    for spreadsheet_id, sources_list in spreadsheets.items():
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"\n{'=' * 80}")
            print(f"📊 Таблица: {spreadsheet.title}")
            print(f"   ID: {spreadsheet_id}")
            print(f"{'=' * 80}\n")
            
            # Получаем все листы
            worksheets = spreadsheet.worksheets()
            
            print(f"Всего листов: {len(worksheets)}\n")
            
            for worksheet in worksheets:
                gid = worksheet.id
                title = worksheet.title
                
                # Проверяем, используется ли этот лист в sources
                is_used = False
                for source_info in sources_list:
                    if title in source_info['sheet_names']:
                        is_used = True
                        print(f"✅ {title:<30} → gid: {gid:<15} (используется в '{source_info['source_name']}')")
                        break
                
                if not is_used:
                    print(f"   {title:<30} → gid: {gid}")
            
        except Exception as e:
            print(f"Ошибка при чтении таблицы {spreadsheet_id}: {e}")
    
    print(f"\n{'=' * 80}")
    print("ГОТОВО")
    print(f"{'=' * 80}\n")
    print("Теперь вы можете использовать gid вместо названий листов в sources.json")
    print("Подробнее: docs/sheet_naming.md")


if __name__ == '__main__':
    show_all_sheet_gids()
