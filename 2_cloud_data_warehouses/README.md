# Cloud Data Warehouses

1. [Introduction to Data Warehouses](#introduction-to-data-warehouses).
2. ELT and Data Warehouse Technology in the Cloud
3. AWS Data Warehouse Technologies
4. [Implementing a Data Warehouse on AWS](#implementing-a-data-warehouse-on-aws).
5. [Project: Data Warehouse](./project_data_warehouse/).


## Requirements

* System requirements:
    + docker
    + docker-compose
    + python >= 3.9
    + postgresql-client
* Python package requirements:
    + notebook
    + ipython-sql
    + psycopg2-binary
    + prettytable
    + pandas
    + boto3
    + matplotlib


## Introduction to Data Warehouses

Note: to make pgAdmin configuration persistent, create a folder `pgadmin_data` inside `data`. Change its permission via:

```bash
sudo chown 5050:5050 data/pgadmin_data
```

Run Docker Compose in detached mode:

```bash
docker-compose up -d
```
To populate `pagila` database:

Create all schema objetcs (tables, etc):

```bash
cat pagila_data/pagila-schema.sql | docker exec -i pg_pagila_db psql -U root pagila_db
```

Insert all data:

```bash
cat pagila_data/pagila-data.sql | docker exec -i pg_pagila_db psql -U root pagila_db
```

To run `psql` Postgres client from the Docker container:

```bash
docker exec -it pg_pagila_db psql pagila_db
```

To shut it down:

```bash
docker-compose down
```

To run `psql` locally, we have to install the client in our computer:

```bash
sudo apt install postgresql-client
```

Then we can run it:

```bash
psql -h localhost -U root pagila_db
```

Related notebooks in the [`notebooks`](./notebooks) directory:
* L1_E1_Introduction_to_Data_Warehouses_P1.ipynb
* L1_E1_Introduction_to_Data_Warehouses_P2.ipynb
* L1_E2_OLAP_Cubes_Operations.ipynb



## Implementing a Data Warehouse on AWS

Related notebooks in the [`notebooks`](./notebooks) directory:
* L4_E1_Create_a_Redshift_cluster_via_SDK.ipynb
* L4_E2_Parallel_ETL.ipynb
* L4_E3_Optimizing_Redshift_Table_Design.ipynb


## Project: Data Warehouse

[Link](./project_data_warehouse/).