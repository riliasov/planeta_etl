"""
Exporter - Экспорт витрин данных в Google Sheets.
"""
import pandas as pd
import gspread
from typing import Optional


def export_dataframe_to_sheet(
    gc: gspread.Client,
    df: pd.DataFrame,
    spreadsheet_id: str,
    gid: str,
    clear_first: bool = True
) -> None:
    """
    Экспортирует pandas DataFrame в Google Sheets по gid.
    
    Args:
        gc: Google Sheets client
        df: DataFrame для экспорта
        spreadsheet_id: ID целевой таблицы
        gid: GID целевого листа
        clear_first: Очищать лист перед записью
    """
    # Открываем spreadsheet
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # Находим worksheet по gid
    worksheet = None
    for ws in spreadsheet.worksheets():
        if str(ws.id) == str(gid):
            worksheet = ws
            break
    
    if worksheet is None:
        raise ValueError(f"Лист с gid={gid} не найден в spreadsheet {spreadsheet_id}")
    
    print(f"📤 Экспорт в лист: {worksheet.title} (gid: {gid})")
    
    # Очистка листа
    if clear_first:
        worksheet.clear()
        print("   🧹 Лист очищен")
    
    # Подготовка данных: headers + rows
    values = [df.columns.tolist()] + df.values.tolist()
    
    # Обновление всех ячеек разом (batch update)
    worksheet.update(range_name='A1', values=values, value_input_option='RAW')
    
    print(f"   ✅ Экспортировано {len(df)} строк, {len(df.columns)} колонок")
    
    # Форматирование заголовков (опционально)
    try:
        worksheet.format('A1:Z1', {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
        })
        print("   ✨ Заголовки отформатированы")
    except Exception as e:
        print(f"   ⚠️  Ошибка форматирования: {e}")


def export_balance_to_sheets(
    gc: gspread.Client,
    balance_df: pd.DataFrame,
    spreadsheet_id: str = "1-kEt2r-mzqI6PmtFqcFaS7XVAPdlde5FxYMv4DXwd94",
    gid: str = "1868616984"
) -> None:
    """
    Экспортирует витрину баланса клиентов в Google Sheets.
    
    Args:
        gc: Google Sheets client
        balance_df: DataFrame с балансом
        spreadsheet_id: ID целевой таблицы (по умолчанию из sources.json)
        gid: GID целевого листа
    """
    export_dataframe_to_sheet(gc, balance_df, spreadsheet_id, gid)


def export_all_datamarts(
    gc: gspread.Client,
    datamarts: dict,
    spreadsheet_id: str = "1-kEt2r-mzqI6PmtFqcFaS7XVAPdlde5FxYMv4DXwd94",
    balance_gid: str = "1868616984"
) -> None:
    """
    Экспортирует все витрины в Google Sheets.
    
    Args:
        gc: Google Sheets client  
        datamarts: dict с ключами 'sales', 'trainings', 'balance'
        spreadsheet_id: ID целевой таблицы
        balance_gid: GID для листа с балансом
    """
    print("\n📊 Экспорт витрин в Google Sheets...")
    
    # Пока экспортируем только balance
    # В будущем можно добавить отдельные листы для sales и trainings
    export_balance_to_sheets(
        gc,
        datamarts['balance'],
        spreadsheet_id,
        balance_gid
    )
    
    print("✅ Экспорт завершен\n")
