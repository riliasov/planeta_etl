#!/bin/bash
# Универсальный скрипт запуска ETL
# Использование: ./run.sh [current|historical|references|all|test]

set -e

# Определяем директорию скрипта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Проверяем наличие venv
if [ ! -d "venv" ]; then
    echo "❌ Виртуальное окружение не найдено!"
    echo "   Создайте его: python3 -m venv venv"
    echo "   Затем: source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Активируем venv
source venv/bin/activate

# Проверяем аргумент
SCOPE="${1:-current}"

case "$SCOPE" in
    current|historical|references|all)
        echo "🚀 Запуск ETL: --scope $SCOPE"
        python main.py --scope "$SCOPE"
        ;;
    test)
        echo "🧪 Запуск тестов..."
        python -m pytest -v
        ;;
    *)
        echo "Использование: ./run.sh [current|historical|references|all|test]"
        echo ""
        echo "  current     - Текущие данные (по умолчанию)"
        echo "  historical  - Исторические данные"
        echo "  references  - Справочники"
        echo "  all         - Все источники"
        echo "  test        - Запуск тестов"
        exit 1
        ;;
esac
