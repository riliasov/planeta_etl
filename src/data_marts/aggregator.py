"""
Aggregator - Pandas-based агрегация данных для витрин.

Использует pandas для локальной обработки (минимум нагрузки на Supabase).
"""
import pandas as pd
from sqlalchemy import Engine


def aggregate_client_sales(engine: Engine) -> pd.DataFrame:
    """
    Агрегирует продажи по клиентам из sales_hst + sales_cur.
    
    Returns:
        DataFrame с колонками: klient, tip, total_kolichestvo, total_summa
    """
    # Простой SELECT без GROUP BY (вся агрегация в pandas)
    query = """
    SELECT klient, tip, kolichestvo, okonchatelnaya_stoimost
    FROM staging.sales_hst
    WHERE klient IS NOT NULL AND tip IS NOT NULL
    
    UNION ALL
    
    SELECT klient, tip, kolichestvo, okonchatelnaya_stoimost
    FROM staging.sales_cur
    WHERE klient IS NOT NULL AND tip IS NOT NULL
    """
    
    df = pd.read_sql(query, engine)
    
    # Агрегация в pandas
    result = df.groupby(['klient', 'tip'], as_index=False).agg({
        'kolichestvo': 'sum',
        'okonchatelnaya_stoimost': 'sum'
    })
    
    result.columns = ['klient', 'tip', 'total_kolichestvo', 'total_summa']
    
    return result


def aggregate_client_trainings(engine: Engine) -> pd.DataFrame:
    """
    Агрегирует списания тренировок по клиентам из trainings_hst.
    
    Только для tip = 'Бассейн' или 'Ванны'.
    
    Returns:
        DataFrame с колонками: klient, tip, total_spisano
    """
    query = """
    SELECT klient, tip, spisano
    FROM staging.trainings_hst
    WHERE klient IS NOT NULL 
      AND tip IN ('Бассейн', 'Ванны')
      AND spisano IS NOT NULL
    """
    
    df = pd.read_sql(query, engine)
    
    # Агрегация в pandas
    result = df.groupby(['klient', 'tip'], as_index=False).agg({
        'spisano': 'sum'
    })
    
    result.columns = ['klient', 'tip', 'total_spisano']
    
    return result


def calculate_client_balance(sales_df: pd.DataFrame, trainings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Рассчитывает баланс занятий (приобретено - списано) по клиентам.
    
    Args:
        sales_df: результат aggregate_client_sales()
        trainings_df: результат aggregate_client_trainings()
        
    Returns:
        DataFrame с колонками: klient, tip, priobreteno, spisano, ostatok
    """
    # Фильтруем sales только для Бассейн/Ванны
    sales_filtered = sales_df[sales_df['tip'].isin(['Бассейн', 'Ванны'])].copy()
    sales_filtered = sales_filtered[['klient', 'tip', 'total_kolichestvo']]
    sales_filtered.columns = ['klient', 'tip', 'priobreteno']
    
    trainings_filtered = trainings_df.copy()
    trainings_filtered.columns = ['klient', 'tip', 'spisano']
    
    # FULL OUTER JOIN через pd.merge
    balance = pd.merge(
        sales_filtered, 
        trainings_filtered, 
        on=['klient', 'tip'], 
        how='outer'
    )
    
    # Заполняем NaN нулями
    balance['priobreteno'] = balance['priobreteno'].fillna(0).astype(int)
    balance['spisano'] = balance['spisano'].fillna(0).astype(int)
    
    # Расчет остатка
    balance['ostatok'] = balance['priobreteno'] - balance['spisano']
    
    # Сортировка
    balance = balance.sort_values(['tip', 'klient']).reset_index(drop=True)
    
    return balance


def build_all_datamarts(engine: Engine) -> dict:
    """
    Строит все витрины данных.
    
    Returns:
        dict с ключами:
            - 'sales': aggregate_client_sales()
            - 'trainings': aggregate_client_trainings()
            - 'balance': calculate_client_balance()
    """
    print("📊 Построение витрин данных...")
    
    print("  1️⃣  Агрегация продаж по клиентам...")
    sales = aggregate_client_sales(engine)
    print(f"      ✅ {len(sales)} строк")
    
    print("  2️⃣  Агрегация списаний по клиентам...")
    trainings = aggregate_client_trainings(engine)
    print(f"      ✅ {len(trainings)} строк")
    
    print("  3️⃣  Расчет баланса занятий...")
    balance = calculate_client_balance(sales, trainings)
    print(f"      ✅ {len(balance)} строк")
    
    return {
        'sales': sales,
        'trainings': trainings,
        'balance': balance
    }
