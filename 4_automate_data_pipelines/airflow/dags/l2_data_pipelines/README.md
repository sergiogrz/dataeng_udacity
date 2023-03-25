# Data pipelines and Airflow DAGs

* [Callables and decorators](#callables-and-decorators).
* [DAGs, Operators and Tasks](#dags-operators-and-tasks).
* [Task dependencies](#task-dependencies).
* [Airflow hooks](#airflow-hooks).
* [Context and templating](#context-and-templating).


## Callables and decorators

The [`l2_e1_airflow_dags.py`](./l2_e1_airflow_dags.py) script takes advantage of _callables_ and _decorators_.

**Callables**  
Callables are an example of functional programming, and can be thought of as passing functions for inclusion as arguments to other functions. Examples of callables are map, reduce, filter.

Here is the link to the [Python documentation on callables](https://docs.python.org/3/library/functools.html).

**Decorators**  
A decorator is a design pattern that allows you to modify the functionality of a function by wrapping it in another function.

Check [this documentation](https://docs.astronomer.io/learn/airflow-decorators) for more information about Airflow decorators.



## DAGs, Operators and Tasks

### DAGs

There are 3 ways to declare a DAG ([documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)):
* Context manager.
* Standard constructor.
* @dag decorator.

**Using `@dag` decorators to create DAGs.**  
A **DAG Decorator** is an annotation used to mark a function as the definition of a DAG. You can set attributes about the DAG, like: name,  description, start date, and interval. The function itself marks the beginning of the definition of the DAG.

    ```python
    import pendulum
    from airflow.decorators import dag

    @dag(description='Analyzes Divvy Bikeshare Data',
        start_date=pendulum.now(),
        schedule_interval='@daily')
    def divvy_dag():
    ```

**Schedules.**  
Schedules are optional and may be defined with cron strings or Airflow Presets, which are: `@once`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`, `None` (only run the DAG when the user initiates it).

* Start date: If your start date is in the past, Airflow will run your DAG as many times as there are schedule intervals between that start date and the current date.
* End date: Unless you specify an optional end date, Airflow will continue to run your DAGs until you disable or delete the DAG.



### Operators

[Operators](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html) define the atomic steps of work that make up a DAG. An Operator is conceptually a template for a predefined Task, that you can just define declaratively inside your DAG. Airflow comes with many Operators that can perform common operations: `PythonOperator`, `BashOperator`, `PostgresOperator`, `S3ToRedshiftOperator`, `RedshiftToS3Operator`, `SimpleHttpOperator`, etc.



### Tasks

Instantiated operators are referred to as Tasks.

**Using Operators to define Tasks.**  

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print(“Hello World”)

divvy_dag = DAG(...)
task = PythonOperator(
    task_id=’hello_world’,
    python_callable=hello_world,
    dag=divvy_dag)
```


**Using `@task` decorators to define tasks.**  
A **Task Decorator** is an annotation used to mark a function as a **custom operator**, that generates a task.

    ```python
        @task()
        def hello_world_task():
        logging.info("Hello World")
    ```



## Task dependencies

In Airflow DAGs:
* Nodes = Tasks
* Edges = Ordering and dependencies between tasks

Task dependencies can be described programmatically in Airflow using `>>` and `<<`. They can also be set with “set_downstream” and “set_upstream”
* a `>>` b, or `a.set_downstream(b)`, means a comes before b
* a `<<` b, or `a.set_upstream(b)`, means a comes after b

```python
hello_world_task = PythonOperator(task_id=’hello_world’, ...)
goodbye_world_task = PythonOperator(task_id=’goodbye_world’, ...)
...
# Use >> to denote that goodbye_world_task depends on hello_world_task
hello_world_task >> goodbye_world_task 
# hello_world_task.set_downstream(goodbye_world_task)

```



## Airflow hooks

Airflow allows users to [manage connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) and configuration of DAGs via the UI or via the CLI.

Connections can be accessed in code via [hooks](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html#). Hooks provide a reusable interface to external systems and databases. With hooks, you don’t have to worry about how and where to store these connection strings and secrets in your code.

```python
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

def load():
# Create a PostgresHook option using the `demo` connection
    db_hook = PostgresHook(‘demo’)
    df = db_hook.get_pandas_df('SELECT * FROM rides')
    print(f'Successfully used PostgresHook to return {len(df)} records')

load_task = PythonOperator(task_id=’load’, python_callable=hello_world, ...)
```

Airflow comes with many Hooks that can integrate with common systems. Here are a few common ones: 
* `HttpHook`.
* `PostgresHook` (works with RedShift).
* `MySqlHook`.
* `SlackHook`.
* `PrestoHook`.



## Context and templating

Airflow leverages templating to allow users to “fill in the blank” with important runtime variables for tasks. We use the `**kwargs` parameter to accept the runtime variables in our task.

```python
from airflow.decorators import dag, task

@dag(
  schedule_interval="@daily";
)
def template_dag(**kwargs):

  @task
  def hello_date():
    print(f“Hello {kwargs['ds']}}”)


```

[Here](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html) is the Apache Airflow **templates reference**.

