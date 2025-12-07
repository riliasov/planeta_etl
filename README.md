# Planeta ETL

ETL-система для загрузки данных из Google Sheets в Supabase.

## 🚀 Быстрый запуск

```bash
./run.sh current    # Текущие данные
./run.sh historical # Исторические данные  
./run.sh references # Справочники
./run.sh all        # Всё
```

## 📁 Структура

```
src/
├── config.py          # Конфигурация
├── db.py              # Подключение к БД
├── sheets.py          # Google Sheets API
├── pipelines/         # ETL пайплайны
└── sources.json       # Источники данных

secrets/               # Credentials (не в git)
├── .env               # SUPABASE_DB_URL
└── *.json             # Google Service Account
```

## ⚙️ Первоначальная настройка

```bash
# 1. Создать виртуальное окружение
python3 -m venv venv

# 2. Установить зависимости
source venv/bin/activate
pip install -r requirements.txt

# 3. Настроить secrets/.env
SUPABASE_DB_URL=postgresql://user:pass@host:port/db
GOOGLE_SHEETS_CREDENTIALS_FILE=secrets/your_creds.json
```

## 🧪 Тесты

```bash
./run.sh test
```
