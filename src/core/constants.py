"""Константы проекта."""

# Database
DB_BATCH_SIZE = 1000
DB_CONNECTION_POOL_SIZE = 5
DB_MAX_OVERFLOW = 10

# Data Processing
DATE_FORMAT = '%d.%m.%Y'
DATETIME_FORMAT = '%d.%m.%Y %H:%M:%S'
DEFAULT_ENCODING = 'utf-8'

# Logging
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FILE_PREFIX = 'etl_'

# Timeouts
SHEETS_READ_TIMEOUT = 30
DB_QUERY_TIMEOUT = 60

# Column keywords
NUMERIC_KEYWORDS = [
    'stoimost', 'summa', 'kolichestvo', 'bonus',
    'nalichnye', 'perevod', 'terminal', 'vdolg',
    'zp', 'oplata', 'stavka', 'spisano', 'god',
    'mesyats', 'chasy'
]

DATE_KEYWORDS = ['data', 'date', 'zapis']

BOOLEAN_COLUMNS = [
    'probili_na_evotore', 'vnesli_v_crm',
    'relevant', 'zamena'
]

SERVICE_COLUMNS = ['source_row_id', 'row_hash', 'id', 'imported_at']
