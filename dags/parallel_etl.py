from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
import sqlite3

default_args = {
    "owner": "Me",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Прямой путь к source.db для чтения
SOURCE_DB_PATH = "/opt/airflow/data/source.db"

# Московское время (UTC+3)
MSK = timezone(timedelta(hours=3))


def get_msk_time():
    return datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def write_log(task_name, status, run_id, message="", start_time=None, end_time=None):
    """Пишет лог через Hook result_db"""
    try:
        hook = SqliteHook(sqlite_conn_id="result_db")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO etl_log (task_name, status, run_id, message, start_time, end_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (task_name, status, run_id, message, start_time, end_time),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to write log: {e}")


def generate_run_id(**context):
    counter = int(Variable.get("etl_run_counter", default="1"))
    run_id = f"ETL_{counter:03d}"
    Variable.set("etl_run_counter", str(counter + 1))
    context["task_instance"].xcom_push(key="run_id", value=run_id)

    start_time = get_msk_time()
    write_log(
        "DAG: parallel_etl",
        "STARTED",
        run_id,
        f"Run ID: {run_id}",
        start_time=start_time,
        end_time=None,
    )
    print(f"Generated run_id: {run_id}")
    return run_id


def update_dag_success(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    end_time = get_msk_time()
    try:
        hook = SqliteHook(sqlite_conn_id="result_db")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE etl_log 
            SET status = 'SUCCESS', end_time = ?, message = 'Pipeline completed successfully'
            WHERE run_id = ? AND task_name = 'DAG: parallel_etl'
            """,
            (end_time, run_id),
        )
        conn.commit()
        conn.close()
        print(f"DAG {run_id} finished SUCCESS at {end_time}")
    except Exception as e:
        print(f"Failed to update DAG status: {e}")


def update_dag_failure(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    end_time = get_msk_time()
    try:
        hook = SqliteHook(sqlite_conn_id="result_db")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE etl_log 
            SET status = 'ERROR', end_time = ?, message = 'Pipeline failed'
            WHERE run_id = ? AND task_name = 'DAG: parallel_etl'
            """,
            (end_time, run_id),
        )
        conn.commit()
        conn.close()
        print(f"DAG {run_id} finished ERROR at {end_time}")
    except Exception as e:
        print(f"Failed to update DAG status: {e}")


def check_orders_function(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    task_name = "check_orders"
    start_time = get_msk_time()
    try:
        # Читаем через прямой путь
        conn = sqlite3.connect(SOURCE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders")
        count = cursor.fetchone()[0]
        conn.close()

        if count == 0:
            msg = "Table orders is empty"
            write_log(
                task_name,
                "FAILED",
                run_id,
                message=msg,
                start_time=start_time,
                end_time=get_msk_time(),
            )
            raise ValueError(msg)
        else:
            msg = f"Orders table contains {count} rows"
            write_log(
                task_name,
                "SUCCESS",
                run_id,
                message=msg,
                start_time=start_time,
                end_time=get_msk_time(),
            )
            print(f"Таблица orders содержит {count} строк. Продолжаем...")
    except Exception as e:
        write_log(
            task_name,
            "ERROR",
            run_id,
            message=str(e),
            start_time=start_time,
            end_time=get_msk_time(),
        )
        raise


def extract_from_source(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    task_name = "extract"
    start_time = get_msk_time()
    try:
        # Читаем через прямой путь
        conn = sqlite3.connect(SOURCE_DB_PATH)
        df = pd.read_sql_query("SELECT order_date, category, amount FROM orders", conn)
        conn.close()

        msg = f"Extracted {len(df)} rows"
        write_log(
            task_name,
            "SUCCESS",
            run_id,
            message=msg,
            start_time=start_time,
            end_time=get_msk_time(),
        )
        print(f"Extracted {len(df)} rows from source.db")
        return df
    except Exception as e:
        write_log(
            task_name,
            "ERROR",
            run_id,
            message=str(e),
            start_time=start_time,
            end_time=get_msk_time(),
        )
        raise


def daily_stats(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    task_name = "daily_stats"
    start_time = get_msk_time()
    try:
        df = context["task_instance"].xcom_pull(task_ids="extract")
        if df is None:
            df = extract_from_source(**context)

        daily = df.groupby("order_date")["amount"].sum().reset_index()
        daily.columns = ["date", "total_amount"]

        # Запись через Hook result_db
        hook = SqliteHook(sqlite_conn_id="result_db")
        conn = hook.get_conn()
        daily.to_sql("daily_stats", conn, if_exists="replace", index=False)
        conn.close()

        msg = f"Saved {len(daily)} rows to daily_stats"
        write_log(
            task_name,
            "SUCCESS",
            run_id,
            message=msg,
            start_time=start_time,
            end_time=get_msk_time(),
        )
        print("Daily stats saved to result_db")
        print(daily)
        return "daily_stats done"
    except Exception as e:
        write_log(
            task_name,
            "ERROR",
            run_id,
            message=str(e),
            start_time=start_time,
            end_time=get_msk_time(),
        )
        raise


def category_stats(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    task_name = "category_stats"
    start_time = get_msk_time()
    try:
        df = context["task_instance"].xcom_pull(task_ids="extract")
        if df is None:
            df = extract_from_source(**context)

        category = df.groupby("category")["amount"].sum().reset_index()
        category.columns = ["category", "total_amount"]

        # Запись через Hook result_db
        hook = SqliteHook(sqlite_conn_id="result_db")
        conn = hook.get_conn()
        category.to_sql("category_stats", conn, if_exists="replace", index=False)
        conn.close()

        msg = f"Saved {len(category)} rows to category_stats"
        write_log(
            task_name,
            "SUCCESS",
            run_id,
            message=msg,
            start_time=start_time,
            end_time=get_msk_time(),
        )
        print("Category stats saved to result_db")
        print(category)
        return "category_stats done"
    except Exception as e:
        write_log(
            task_name,
            "ERROR",
            run_id,
            message=str(e),
            start_time=start_time,
            end_time=get_msk_time(),
        )
        raise


def final_summary(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="generate_run_id", key="run_id"
    )
    task_name = "final_summary"
    start_time = get_msk_time()
    try:
        # Читаем через Hook result_db
        hook = SqliteHook(sqlite_conn_id="result_db")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_stats")
        daily_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM category_stats")
        cat_count = cursor.fetchone()[0]
        conn.close()

        msg = f"daily_stats: {daily_count} rows, category_stats: {cat_count} rows"
        write_log(
            task_name,
            "SUCCESS",
            run_id,
            message=msg,
            start_time=start_time,
            end_time=get_msk_time(),
        )
        print(f"Final summary: {msg}")
        print("Both parallel tasks completed successfully!")

        update_dag_success(**context)

    except Exception as e:
        write_log(
            task_name,
            "ERROR",
            run_id,
            message=str(e),
            start_time=start_time,
            end_time=get_msk_time(),
        )
        update_dag_failure(**context)
        raise


with DAG(
    dag_id="parallel_etl",
    default_args=default_args,
    description="ETL: read from source.db via direct path, write via result_db hook",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "parallel", "counter"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate_id = PythonOperator(
        task_id="generate_run_id", python_callable=generate_run_id
    )

    check_orders = PythonOperator(
        task_id="check_orders", python_callable=check_orders_function
    )

    extract = PythonOperator(task_id="extract", python_callable=extract_from_source)

    task_daily = PythonOperator(task_id="daily_stats", python_callable=daily_stats)

    task_category = PythonOperator(
        task_id="category_stats", python_callable=category_stats
    )

    final = PythonOperator(task_id="final_summary", python_callable=final_summary)

    end = EmptyOperator(task_id="end")

    (
        start
        >> generate_id
        >> check_orders
        >> extract
        >> [task_daily, task_category]
        >> final
        >> end
    )
