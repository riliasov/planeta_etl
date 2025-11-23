"""Модуль для работы с Google Sheets API (только чтение)."""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os


def get_sheets_client(config):
    """
    Создает и возвращает авторизованный клиент gspread для чтения Google Sheets.
    
    Используются read-only права (spreadsheets.readonly, drive.readonly).
    
    Args:
        config (dict): Словарь конфигурации с ключом 'GOOGLE_SHEETS_CREDENTIALS_FILE'
    
    Returns:
        gspread.Client: Авторизованный клиент для работы с Google Sheets
    
    Raises:
        FileNotFoundError: Если файл с credentials не найден
        Exception: При ошибке авторизации
    """
    creds_file = config['GOOGLE_SHEETS_CREDENTIALS_FILE']
    
    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"Credentials file not found at: {creds_file}")
        
    # Обновленные scopes для записи в Sheets (data marts экспорт)
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        raise Exception(f"Error connecting to Google Sheets: {e}")


def get_worksheet(spreadsheet, sheet_identifier, use_gid=False):
    """
    Получает worksheet по названию или gid.
    
    Args:
        spreadsheet: Объект spreadsheet от gspread
        sheet_identifier (str): Название листа или gid
        use_gid (bool): True - использовать gid, False - использовать название
    
    Returns:
        gspread.Worksheet: Объект листа
    
    Raises:
        Exception: Если лист не найден
    """
    try:
        if use_gid:
            # Получаем лист по gid
            worksheet = spreadsheet.get_worksheet_by_id(int(sheet_identifier))
            if worksheet is None:
                raise ValueError(f"Лист с gid={sheet_identifier} не найден")
        else:
            # Получаем лист по названию
            worksheet = spreadsheet.worksheet(sheet_identifier)
        
        return worksheet
    except Exception as e:
        raise Exception(f"Ошибка при получении листа '{sheet_identifier}' (use_gid={use_gid}): {e}")


def read_sheet_data(gc, spreadsheet_id, sheet_identifier, range_str=None, use_gid=False):
    """
    Читает данные из листа Google Sheets.
    
    Args:
        gc: Авторизованный gspread клиент
        spreadsheet_id (str): ID таблицы
        sheet_identifier (str): Название листа или gid
        range_str (str): Диапазон для чтения (например, "A1:Z100")
        use_gid (bool): True - использовать gid, False - использовать название
    
    Returns:
        list: Список списков с данными
        
    Example:
        >>> data = read_sheet_data(gc, "abc123", "Sheet1", "A1:C10")
        >>> data = read_sheet_data(gc, "abc123", "0", use_gid=True)
    """
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = get_worksheet(spreadsheet, sheet_identifier, use_gid)
        
        if range_str:
            data = worksheet.get(range_str)
        else:
            data = worksheet.get_all_values()
        
        return data
    except Exception as e:
        raise Exception(f"Ошибка при чтении данных: {e}")
