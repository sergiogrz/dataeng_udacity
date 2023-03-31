# Airflow and AWS

## Table of contents

* [Create an IAM user in AWS](#create-an-iam-user-in-aws).
* [Add Airflow connections](#add-airflow-connections).
* [Configure variables - S3 paths](#configure-variables---s3-paths).
* [Configure AWS Redshift Serverless](#configure-aws-redshift-serverless).
* [Add Airflow Connections to AWS Redshift](#add-airflow-connections-to-aws-redshift).
* [PostgresHook and PostgresOperator](#postgreshook-and-postgresoperator).


## Create an IAM user in AWS

Steps to follow via the Console:
* Identity and Access Management (IAM) > Access management > Users > Add users.
* Specify user details: user name.
* Set permissions: Attach policies directly:
    + AdministratorAccess.
    + AmazonRedshiftFullAccess.
    + AmazonS3FullAccess.
* Review and create.
* Security credentials > Access keys > Create access key.
* Retrieve access keys > Download .csv file.



## Add Airflow connections

Here, we'll use Airflow's UI to configure our AWS credentials:

* Admin > Connections > +.
* Connection Id: `aws_credentials`.
* Connection Type: `Amazon Web Services`.
* AWS Access Key ID: [key from the csv file we have downloaded].
* AWS Secret Access Key: [key from the csv file we have downloaded].



Now we can reference our AWS Credentials without storing them in the code, by using `S3Hook`.

```python
from airflow.hooks.S3_hook import S3Hook
. . .
hook = S3Hook(aws_conn_id='aws_credentials')
        bucket = Variable.get('s3_bucket')
        prefix = Variable.get('s3_prefix')
        logging.info(f"Listing Keys from {bucket}/{prefix}")
        keys = hook.list_keys(bucket, prefix=prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
    list_keys()

list_keys_dag = list_keys()
```



## Configure variables - S3 paths

We use Airflow's UI again to configure our S3 path variables:
* Admin > Variables > +.
* Key: `s3_bucket`; Value: `airflow-aws` (the name of a bucket we have previously created).
* Key: `s3_prefix`; Value: `data-pipelines`.


Now we can reference these variables in our code using `Variable` like this:

```python
from airflow.models import Variable
. . .
bucket = Variable.get('s3_bucket')
prefix = Variable.get('s3_prefix')
```



## Configure AWS Redshift Serverless

[AWS Redshift Serverless](https://aws.amazon.com/redshift/redshift-serverless/) gives us all the benefits of a Redshift cluster without paying for compute when our servers are idle. Follow along the steps below to set up AWS Redshift Serverless.

* From the AWS Cloudshell, create a Redshift role with S3 full access:
    * Create a Redshift role called `my-redshift-service-role`.
        ```bash
        aws iam create-role --role-name my-redshift-service-role --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "redshift.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }'
        ```
    
    * Give the role S3 Full Access.
        ```bash
        aws iam attach-role-policy --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess --role-name my-redshift-service-role
        ```


* From the AWS Console:
    * Amazon Redshift > Redshift Serverless.
    * Customize settings.
        + Go with the default namespace name.
        + Check Customize admin user credentials.
        + Admin user name: `airflow_user` (the one we have created).
        + Admin user password: [password chosen].
        + Associated IAM Roles: add `my-redshift-service-role`.
        + Accept the defaults for Security and encryption.
        + Accept the default Workgroup settings.
        + Select Turn on enhanced VPC routing.
        + Click Save configuration nd wait for Redshift Serverless setup to finish.
    * On succesful completion, you will see Status Available.
    * Next, we are going to make this cluster publicly accessible as we would like to connect to this cluster via Airflow.
        + Click the default Workgroup.
        + Network and Security > Edit.
        + Select Turn on Publicly accessible and save changes.
        + Choose the link labeled VPC security group to open the Amazon Elastic Compute Cloud (Amazon EC2) console.
        + Inbound Rules > Edit inbound rules.
        + Add an inbound rule, with the following parameters:
            - Type = Custom TCP.
            - Port range = 0 - 5500.
            - Source = Anywhere-iPv4.
        + Now Redshift Serverless should be accessible from Airflow.
        + Go back to the Redshift Workgroup and copy the endpoint. Store this locally as we will need this while configuring Airflow.



## Add Airflow Connections to AWS Redshift

Here, we'll use Airflow's UI to connect to AWS Redshift:

* Admin > Connections > +.
* Connection Id: `redshift`.
* Connection Type: `Amazon Redshift`.
* Host: enter the endpoint of your Redshift Serverless workgroup, excluding the port and schema name at the end (it should be something like `default.111222333444.us-east-1.redshift-serverless.amazonaws.com`)
* Database: `dev`. This is the Redshift database we want to connect to.
* User: `airflow_user`.
* Password: enter the password created when launching Redshift serverless.
* Port: `5439`.



## PostgresHook and PostgresOperator

The `MetastoreBackend` Python class connects to the **Airflow Metastore Backend** to retrieve credentials and other data needed to connect to outside systems.

The below code creates an `aws_connection` object:
* `aws_connection.login` contains the **Access Key ID**.
* `aws_connection.password` contains the **Secret Access Key**.

```python
from airflow.decorators import dag
from airflow.secrets.metastore import MetastoreBackend

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():

    @task
    def copy_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))
```


### Using PostgresHook

The `PostgresHook` class is a superclass of the Airflow `DbApiHook`. When you instantiate the class, it creates an object that contains all the connection details for the Postgres database. It retrieves the details from the Postgres connection you created earlier in the Airflow UI.


Just pass the connection id that you created in the Airflow UI.

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator
. . .
    redshift_hook = PostgresHook("redshift")
```

Call `.run()` on the returned `PostgresHook` object to execute SQL statements.

```python
    redhisft_hook.run("SELECT * FROM trips")
```


### Using PostgresOperator

The `PostgresOperator` class executes sql, and accepts the following parameters:
* A `postgres_conn_id`.
* A `task_id`.
* The sql statement.
* Optionally a dag.

```python
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():

. . .
    create_table_task=PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
. . .
    create_table_task >> copy_data
```
