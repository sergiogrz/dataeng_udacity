# Set up Airflow environment with Docker Compose

Airflow docs reference: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

* [Airflow setup (full version)](#airflow-setup-full-version).
* [Airflow setup (light version)](#airflow-setup-light-version).
* [Execution](#execution).


## Airflow setup (full version)

1. Create a new sub-directory called `airflow` in your project dir and do `cd` into it.

    ```bash
    mkdir -p airflow && cd "$_"
    ```

1. **Set the airflow user**  
On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. Otherwise the files created in `dags`, `logs` and `plugins` will be created with root user ownership. You have to make sure to configure them for the docker-compose:

    ```bash
    # Inside airflow dir
    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

    * `airflow` project structure:
        + `dags`: for DAG (Airflow pipelines) files.
        + `logs`: contains logs from task execution and scheduler.
        + `plugins`: custom plugins or helper functions that we may need.

1. **Import the official Docker Compose setup file** from the latest Airflow version:

   ```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.2/docker-compose.yaml'
   ```

1. Change the `AIRFLOW__CORE__LOAD_EXAMPLES` value to 'false'. This will prevent Airflow from populating its interface with DAG examples.

1. Additional notes:
    * The YAML file uses `CeleryExecutor` as its [executor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html) type, which means that tasks will be pushed to workers (external Docker containers) rather than running them locally (as regular processes). You can change this setting by modifying the `AIRFLOW__CORE__EXECUTOR` environment variable to `LocalExecutor`, under the `x-airflow-common` environment definition.


The [Airflow YAML file](./airflow/docker-compose.yaml) is ready for deployment.


## Airflow setup (light version)

The current [`docker-compose.yaml`](./airflow/docker-compose.yaml) file we've generated will deploy multiple containers which will require lots of resources. This is the correct approach for running multiple DAGs accross multiple nodes in a Kubernetes deployment, but it's very taxing on a regular local computer such as a laptop.

If you want a less overwhelming YAML that only runs the webserver and the scheduler and runs the DAGs in the scheduler rather than running them in external workers, please modify the `docker-compose.yaml` file following these steps:

1. Remove the `redis`, `airflow-worker`, `airflow-triggerer` and `flower` services.
1. Change the `AIRFLOW__CORE__EXECUTOR` environment variable from `CeleryExecutor` to `LocalExecutor` .
1. At the end of the `x-airflow-common` definition, within the `depends-on` block, remove these 2 lines:
    ```yaml
    redis:
      condition: service_healthy
    ```
1. Comment out the `AIRFLOW__CELERY__RESULT_BACKEND` and `AIRFLOW__CELERY__BROKER_URL` environment variables.

You should now have a [simplified Airflow light YAML file](./airflow/docker-compose-light.yaml) ready for deployment and may continue to the next section.


## Execution

1. Before starting all services, we need to initialize the Airflow database:

    ```bash
    docker compose up airflow-init
    # docker compose -f docker-compose-light.yaml up airflow-init
    ```

1. Then, we can run Airlow:

    ```bash
    docker compose up -d
    # docker compose -f docker-compose-light.yaml up -d
    ```

1. We may now access the Airflow UI via `localhost:8080`. Default _username_ and _password_ are both `airflow`.

    >**IMPORTANT**: this is **NOT** a production-ready setup. The username and password for Airflow have not been modified in any way; we can find them by searching for `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` inside the `docker-compose.yaml` file.

1. To close our containers:

    ```bash
    docker compose down
    # docker compose -f docker-compose-light.yaml down
    ```

    To stop and delete containers, delete volumes with database data, and download images, run:

    ```bash
    docker compose down --volumes --rmi all
    # docker compose -f docker-compose-light.yaml down --volumes --rmi all
    ```
