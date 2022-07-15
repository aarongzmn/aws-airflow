import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="postgres_operator_example",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    select_all = PostgresOperator(
        task_id="select_all",
        postgres_conn_id="aurora_airflowdb",
        sql="""
        CREATE TABLE IF NOT EXISTS pet (
        pet_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        pet_type VARCHAR NOT NULL,
        birth_date DATE NOT NULL,
        OWNER VARCHAR NOT NULL);
        """,
    )
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="aurora_airflowdb",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id="aurora_airflowdb",
        sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN SYMMETRIC DATE '{{ params.begin_date }}' AND DATE '{{ params.end_date }}';
            """,
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    )

    select_all >> populate_pet_table >> get_birth_date
