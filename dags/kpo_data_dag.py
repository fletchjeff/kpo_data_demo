from pendulum import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('aws_s3')

import os

CLUSTER_CONTEXT = os.environ["CLUSTER_CONTEXT"]

kpo_defaults = {
    "cluster_context": CLUSTER_CONTEXT,
    "namespace": "default",
    "labels": {"airflow_kpo_in_cluster": "False"},
    "get_logs": True,
    "is_delete_operator_pod": True,
    "in_cluster": False,
    "config_file": "/home/astro/config",
}

# instantiate the DAG
@dag(
    start_date=datetime(2023, 3, 14),
    catchup=False,
    schedule=None,
)
def kpo_data_dag():

    @task
    def query_params():
        ## this task would fecth the required filename and query
        return {
            "input_file":"s3://jf-ml-data/all_flight_data.parquet",
            "filter_query":"ORIGINCITYNAME='New York, NY'"
        }

    @task.kubernetes(
        image="fletchjeffastro/kpo-test:0.1.2",
        name="kpo_data_task",
        task_id="kpo_data_task",
        env_vars={
            "AWS_KEY": f"{conn.login}",
            "AWS_SECRET": f"{conn.password}",
            },
        **kpo_defaults,
    )
    def transform_at_task(query_params):
        import os
        import duckdb

        con = duckdb.connect("./tmp.db")
        con.sql('INSTALL httpfs;')
        con.sql('LOAD httpfs;')
        con.sql(f'SET s3_region="eu-central-1"')
        con.sql(f'SET s3_access_key_id="{os.environ["AWS_KEY"]}";')
        con.sql(f'SET s3_secret_access_key="{os.environ["AWS_SECRET"]}";')
        
        input_file = query_params['input_file']
        print(f"reading in {input_file}")
        all_flights = con.read_parquet(query_params['input_file'])
        
        output_val = f"{input_file.split('.')[0]}_filter_ny.{input_file.split('.')[1]}"
        filter_query = query_params['filter_query']
        print(f"writing in {output_val}")
        all_flights.filter(filter_query).write_parquet(output_val)
        con.close()

        return output_val
    task_query_params = query_params()
    transform_at_task.override(do_xcom_push=True)(task_query_params)

kpo_data_dag()

