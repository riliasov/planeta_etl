import pandas as pd
from typing import Dict, List, Optional, Any
from src.sheets import get_sheets_client, read_sheet_data
from src.utils.infer_schema import clean_column_name
from src.logger import get_logger

logger = get_logger(__name__)

class SheetsProcessor:
    """Обработчик данных из Google Sheets."""
    
    def __init__(self, config: Dict):
        self.gc = get_sheets_client(config)
    
    def read_and_transform(
        self,
        source_config: Dict,
        target_table: str,
        column_mapping: Optional[Dict[str, str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Читает данные из Sheets и трансформирует их.
        
        Returns:
            DataFrame или None если нет данных
        """
        spreadsheet_id = source_config.get('spreadsheet_id')
        sheet_identifiers = source_config.get('sheet_identifiers', [])
        ranges = source_config.get('ranges', {})
        use_gid = source_config.get('use_gid', False)
        
        if not sheet_identifiers:
            logger.debug(f"⚠️ Нет листов для {target_table}")
            return None
        
        # Собираем данные со всех листов
        all_dfs = []
        for sheet_id in sheet_identifiers:
            df = self._read_sheet(
                spreadsheet_id, sheet_id, ranges.get(sheet_id), use_gid
            )
            if df is not None:
                all_dfs.append(df)
        
        if not all_dfs:
            return None
        
        # Объединяем
        result_df = pd.concat(all_dfs, ignore_index=True)
        
        # Применяем маппинг колонок
        if column_mapping:
            result_df = result_df.rename(columns=column_mapping)
        
        # Добавляем метаданные
        result_df['source_row_id'] = range(2, len(result_df) + 2)
        
        return result_df
    
    def _read_sheet(
        self, spreadsheet_id: str, sheet_id: str,
        range_name: Optional[str], use_gid: bool
    ) -> Optional[pd.DataFrame]:
        """Читает один лист и превращает в DataFrame."""
        try:
            data = read_sheet_data(self.gc, spreadsheet_id, sheet_id, range_name, use_gid)
            if not data or len(data) < 2:
                return None
            
            headers = self._normalize_headers(data[0])
            rows = self._align_rows(data[1:], len(headers))
            
            return pd.DataFrame(rows, columns=headers)
        except Exception:
            return None
    
    def _normalize_headers(self, headers: List[Any]) -> List[str]:
        """Нормализует заголовки (уникализация, транслитерация)."""
        seen: Dict[str, int] = {}
        unique_headers: List[str] = []
        
        for h in headers:
            h_str = str(h).strip()
            clean = clean_column_name(h_str)
            
            if clean in seen:
                seen[clean] += 1
                clean = f"{clean}_{seen[clean]}"
            else:
                seen[clean] = 0
            
            unique_headers.append(clean)
        
        return unique_headers
    
    def _align_rows(self, rows: List[List[Any]], expected_cols: int) -> List[List[Any]]:
        """Выравнивает строки по количеству колонок."""
        aligned = []
        for row in rows:
            if len(row) < expected_cols:
                row = row + [None] * (expected_cols - len(row))
            elif len(row) > expected_cols:
                row = row[:expected_cols]
            aligned.append(row)
        return aligned
