from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time
import os


os.environ["AIRFLOW_CONFIG"] = "C:/Users/spogo/airflow/airflow.cfg"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 2, 0, 0),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

connection_name = "neo_data_host"


def pick_random_medal():
    return random.choice(["Bronze", "Silver", "Gold"])


def generate_delay():
    time.sleep(35)


with DAG(
    "olympic_medal_pipeline",
    default_args=default_args,
    description="DAG for selecting and counting Olympic medals",
    schedule_interval="*/01 * * * *",
    catchup=False,
    tags=["spogoretskyi"],
) as dag:

    # 1: Створення таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """,
        hook_params={"schema": "olympic_dataset"},
    )

    # 2: Випадковий вибір медалі
    pick_medal = PythonOperator(task_id="pick_medal", python_callable=pick_random_medal)

    # 3: Розгалуження на основі вибору медалі
    def branch_based_on_medal(ti):
        chosen_medal = ti.xcom_pull(task_ids="pick_medal")
        if chosen_medal == "Bronze":
            return "calc_Bronze"
        elif chosen_medal == "Silver":
            return "calc_Silver"
        else:
            return "calc_Gold"

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=branch_based_on_medal
    )

    # 4.1: Підрахунок бронзових медалей
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO medal_counts (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
        hook_params={"schema": "olympic_dataset"},
    )

    # 4.2: Підрахунок срібних медалей
    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO medal_counts (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
        hook_params={"schema": "olympic_dataset"},
    )

    # 4.3: Підрахунок золотих медалей
    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO medal_counts (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
        hook_params={"schema": "olympic_dataset"},
    )

    # 5: Затримка
    generate_delay = PythonOperator(
        task_id="generate_delay", python_callable=generate_delay
    )

    # 6: Сенсор для перевірки запису
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=connection_name,
        sql="""
        SELECT 1
        FROM medal_counts
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode="poke",
        poke_interval=5,
        timeout=60,
        hook_params={"schema": "olympic_dataset"},
    )
