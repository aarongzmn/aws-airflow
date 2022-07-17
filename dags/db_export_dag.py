from airflow import DAG, settings

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DAG, DagRun, TaskFail, TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import csv, re
from io import StringIO

MAX_AGE_IN_DAYS = 30
S3_BUCKET = "unique-airflow-bucket-name"
S3_KEY = "files/export/{0}.csv"

OBJECTS_TO_EXPORT = [
    [DagRun, DagRun.execution_date],
    [TaskFail, TaskFail.execution_date],
    [TaskInstance, TaskInstance.execution_date],
]


def export_db_fn(**kwargs):
    session = settings.Session()
    print("session: ", str(session))

    oldest_date = days_ago(MAX_AGE_IN_DAYS)
    print("oldest_date: ", oldest_date)
    s3_hook = S3Hook()
    s3_client = s3_hook.get_conn()
    for x in OBJECTS_TO_EXPORT:
        query = session.query(x[0]).filter(x[1] >= days_ago(MAX_AGE_IN_DAYS))
        print("type", type(query))
        allrows = query.all()
        name = re.sub("[<>']", "", str(x[0]))
        print(name, ": ", str(allrows))
        if len(allrows) > 0:
            outfileStr = ""
            f = StringIO(outfileStr)
            w = csv.DictWriter(f, vars(allrows[0]).keys())
            w.writeheader()
            for y in allrows:
                w.writerow(vars(y))
            outkey = S3_KEY.format(name[6:])
            s3_client.put_object(Bucket=S3_BUCKET, Key=outkey, Body=f.getvalue())

    return "OK"


with DAG(
    dag_id="db_export_dag",
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
) as dag:
    export_db = PythonOperator(
        task_id="export_db", python_callable=export_db_fn, provide_context=True
    )
