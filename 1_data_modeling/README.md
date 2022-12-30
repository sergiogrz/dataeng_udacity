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

Note: to make pgAdmin configuration persistent, create a folder `pgadmin_data` inside `data`. Change its permission via:

```bash
sudo chown 5050:5050 data/pgadmin_data
```

and mount it to the `/var/lib/pgadmin` folder.