import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from custom_operators.has_rows import HasRowsOperator


from common import sql_statements


@dag(start_date=pendulum.now(), max_active_runs=1)
def demonstrate_custom_operators():
    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
    )

    copy_trips_task = S3ToRedshiftOperator(
        task_id="load_trips_from_s3_to_redshift",
        table="trips",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="airflow-aws",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
    )

    check_trips_task = HasRowsOperator(
        task_id="count_trips",
        table="trips",
        redshift_conn_id="redshift",
    )

    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    )

    copy_stations_task = S3ToRedshiftOperator(
        task_id="load_stations_from_s3_to_redshift",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="airflow-aws",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
        table="stations",
    )

    check_stations_task = HasRowsOperator(
        task_id="count_stations",
        table="stations",
        redshift_conn_id="redshift",
    )

    create_trips_table >> copy_trips_task
    create_stations_table >> copy_stations_task
    copy_stations_task >> check_stations_task
    copy_trips_task >> check_trips_task


custom_operators_dag = demonstrate_custom_operators()
