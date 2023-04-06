from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", dq_checks=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, dq_check in enumerate(self.dq_checks):
            records = redshift.get_records(dq_check["test_sql"])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check #{i} failed. {dq_check['test_sql']} returned no results"
                )
            result = records[0][0]
            expected_result = dq_check["expected_result"]
            comparison = dq_check["comparison"]

            if not eval(f"{result} {comparison} {expected_result}"):
                raise ValueError(f"Data quality check #{i} failed.")
            else:
                self.log.info(f"Data quality check #{i} passed with {result} records.")
