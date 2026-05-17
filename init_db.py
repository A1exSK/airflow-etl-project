import sqlite3

# --- source.db (исходные данные) ---
source_db_path = "/opt/airflow/data/source.db"
conn = sqlite3.connect(source_db_path)
cursor = conn.cursor()

# Создаём таблицу orders (если её нет)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        order_date TEXT,
        category TEXT,
        amount REAL
    )
""")

# Удаляем таблицу orders:
# cursor.execute("DROP TABLE orders")
cursor.execute("DELETE FROM orders")  # очистить старые данные

orders_data = [
    ("2024-01-01", "retail", 100),
    ("2024-01-02", "retail", 150),
    ("2024-01-03", "retail", 120),
    ("2024-01-01", "wholesale", 300),
    ("2024-01-02", "wholesale", 450),
    ("2024-01-03", "wholesale", 320),
    ("2024-01-01", "test", 50),
    ("2024-01-02", "test", 30),
    ("2025-02-03", "dust", 333),
]

cursor.executemany(
    "INSERT INTO orders (order_date, category, amount) VALUES (?, ?, ?)", orders_data
)
conn.commit()
conn.close()

print(f"source.db initialized with orders table ({len(orders_data)} rows)")

# --- result.db (логи) ---
result_db_path = "/opt/airflow/data/result.db"
conn = sqlite3.connect(result_db_path)
cursor = conn.cursor()

# Создаём таблицу etl_log (если её нет)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,                 -- уникальный ID запуска DAG
    task_name TEXT,
    status TEXT,                 -- STARTED / SUCCESS / ERROR
    message TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
    )
""")

# Удаляем таблицу etl_log:
# cursor.execute("DROP TABLE etl_log")
cursor.execute("DELETE FROM etl_log")  # очистить старые данные

conn.commit()
conn.close()

print("result.db initialized with etl_log table")
