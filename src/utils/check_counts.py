import sqlalchemy
from sqlalchemy import text

from src.config import load_config

def check_counts():
    print("📊 Проверка количества строк в таблицах staging...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    engine = sqlalchemy.create_engine(db_url)
    
    tables = [
        'staging.sales_cur', 'staging.sales_hst',
        'staging.expenses_cur', 'staging.expenses_hst',
        'staging.trainings_cur', 'staging.trainings_hst',
        'staging.clients_hst'
    ]
    
    with engine.connect() as connection:
        for table in tables:
            try:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"   ✅ {table}: {count} строк")
            except Exception as e:
                print(f"   ❌ {table}: Ошибка ({e})")

if __name__ == "__main__":
    check_counts()
