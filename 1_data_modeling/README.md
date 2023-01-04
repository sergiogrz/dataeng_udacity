# Data Modeling

1. Introduction to Data Modeling
2. Relational Data Models
3. NoSQL Data Models
4. Project: Data Modeling with Apache Cassandra

## Requirements
python==3.9  
notebook  
psycopg2-binary  
cassandra-driver

## Relational Data Models

Run Docker Compose in detached mode:

```bash
docker-compose up -d
```
To shut it down:

```bash
docker-compose down
```

Note: to make pgAdmin configuration persistent, create a folder `pgadmin_data` inside `data`. Change its permission via:

```bash
sudo chown 5050:5050 data/pgadmin_data
```

and mount it to the `/var/lib/pgadmin` folder.

Once Docker containers are running, it is possible to access Postgres database via:
* psycopg2: PostgreSQL adapter for Python (see notebooks).
* pgAdmin GUI, at `localhost:8080`.


## NoSQL Data Models

Start a Cassandra server instance with Docker:

```bash
docker run \
    -v $(pwd)/data/cassandra_data:/var/lib/cassandra \
    -p 9042:9042 \
    --name cassandra_db \
    cassandra:4.1
```

Once the container is running, we can access Cassandra database via:
* CQL shell, cqlsh (`docker exec -it cassandra_db cqlsh`).
* cassandra-driver, a Python driver for Apache Cassandra (see notebooks).
