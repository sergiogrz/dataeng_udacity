from datetime import datetime, timedelta
from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from plugins.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from plugins.helpers import SqlQueries


default_args = {
    "owner": "sergiogrz",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def project_data_pipelines_airflow():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="airflow-aws",
        s3_key="log-data",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="airflow-aws",
        s3_key="song-data",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="user",
        sql_query=SqlQueries.user_table_insert,
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="song",
        sql_query=SqlQueries.song_table_insert,
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artist",
        sql_query=SqlQueries.artist_table_insert,
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tables=["user", "song", "artist", "time", "songplays"],
    )

    end_operator = DummyOperator(task_id="Stop_execution")

    (
        start_operator
        >> [stage_events_to_redshift, stage_songs_to_redshift]
        >> load_songplays_table
        >> [
            load_song_dimension_table,
            load_user_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ]
        >> run_quality_checks
        >> end_operator
    )


project_data_pipelines_airflow_dag = project_data_pipelines_airflow()
