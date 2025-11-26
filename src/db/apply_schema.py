import sqlalchemy
from sqlalchemy import text

from src.config import load_config

def apply_schema():
    print("🏗️ Применение схемы базы данных...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ Ошибка: SUPABASE_DB_URL не найден.")
        return

    try:
        # Создаем engine
        # Для Transaction Pooler (6543) важно отключить prepared statements в некоторых драйверах,
        # но для psycopg2 обычно работает нормально.
        # Важно: Transaction Pooler не поддерживает LISTEN/NOTIFY и некоторые другие фичи,
        # но CREATE TABLE должен работать, если это не Session mode pooler.
        # Если возникнет ошибка "prepared statement ... already exists", нужно добавить connect_args.
        
        engine = sqlalchemy.create_engine(db_url, isolation_level="AUTOCOMMIT")
        
        schema_path = os.path.join(os.path.dirname(__file__), 'final_schema.sql')
        
        print(f"   📖 Чтение схемы из {schema_path}...")
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        print("   🚀 Выполнение SQL скрипта...")
        with engine.connect() as connection:
            # Разделяем скрипт на команды, если драйвер не поддерживает выполнение всего скрипта сразу
            # Но sqlalchemy обычно умеет выполнять блоки.
            # Для надежности выполним как один блок text()
            
            connection.execute(text(sql_script))
            
            print("✅ Схема успешно применена!")
            
            # Проверка создания таблиц
            result = connection.execute(text("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema IN ('raw', 'staging', 'core', 'references')
                ORDER BY table_schema, table_name;
            """))
            
            print("\n📊 Созданные таблицы:")
            for row in result:
                print(f"   - {row[0]}.{row[1]}")

    except Exception as e:
        print(f"❌ Ошибка при применении схемы: {e}")

if __name__ == "__main__":
    apply_schema()
