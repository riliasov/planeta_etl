import sqlalchemy
from sqlalchemy import text

from src.config import load_config
from src.data.reference_data import (
    TRAINERS, ADMINS, 
    PRODUCT_NAMES, SALES_TYPES, SALES_CATEGORIES,
    EXPENSE_TYPES, TRAINING_TYPES, TRAINING_CATEGORIES
)

def seed_references():
    print("🌱 Наполнение справочников...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ Ошибка: SUPABASE_DB_URL не найден.")
        return

    engine = sqlalchemy.create_engine(db_url, isolation_level="AUTOCOMMIT")
    
    with engine.connect() as connection:
        # 1. Сотрудники
        print("   👤 Сотрудники...")
        # Очистка
        connection.execute(text('TRUNCATE TABLE "references".employees RESTART IDENTITY CASCADE;'))
        
        employees_data = []
        # Тренеры
        for name in TRAINERS:
            employees_data.append({'name': name, 'role': 'trainer'})
        # Админы
        for name in ADMINS:
            # Проверяем, не добавлен ли уже как тренер
            if name not in TRAINERS:
                employees_data.append({'name': name, 'role': 'admin'})
            else:
                # Если уже есть, обновляем роль на 'both' (сложнее в bulk insert, упростим)
                # Пока просто добавим уникальных админов
                pass
                
        # Вставка
        if employees_data:
            connection.execute(
                text('INSERT INTO "references".employees (name, role) VALUES (:name, :role) ON CONFLICT (name) DO NOTHING'),
                employees_data
            )
            
        # 2. Продукты
        print("   🏷️  Продукты...")
        connection.execute(text('TRUNCATE TABLE "references".products RESTART IDENTITY CASCADE;'))
        
        products_data = [{'name': p, 'type': 'subscription'} for p in PRODUCT_NAMES]
        if products_data:
            connection.execute(
                text('INSERT INTO "references".products (name, type) VALUES (:name, :type) ON CONFLICT (name) DO NOTHING'),
                products_data
            )
            
        # 3. Категории расходов
        print("   💸 Категории расходов...")
        connection.execute(text('TRUNCATE TABLE "references".expense_categories RESTART IDENTITY CASCADE;'))
        
        expenses_data = [{'name': e, 'type': 'expense'} for e in EXPENSE_TYPES]
        if expenses_data:
            connection.execute(
                text('INSERT INTO "references".expense_categories (name, type) VALUES (:name, :type) ON CONFLICT (name) DO NOTHING'),
                expenses_data
            )
            
        print("✅ Справочники успешно наполнены!")

if __name__ == "__main__":
    seed_references()
