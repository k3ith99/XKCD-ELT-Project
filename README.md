# XKCD-ELT-Project
This project implements a complete end to end ELT pipeline for collecting comics from the XKCD website.
This repository is structured so that anyone can run in the pipeline locally.

## Project goal
1. Extract XKCD comic metadata from the public XKCD API
2. Validate raw JSON using Pydantic and Pandas
3. Load raw data into Postgres (bronze layer)
4. Transform into clean dimensional + fact tables using dbt (silver layer)
5. Produce stakeholder ready table (gold layer)
6. Automate and orchestrate pipelines using airflow
7. Automate data quality checks
8. Implement dimensional modelling
9. Implement polling to listen to new API Data

## Architecture overview


## Database Design

<img width="1810" height="546" alt="Untitled (1)" src="https://github.com/user-attachments/assets/4bb5615d-b389-4eeb-b569-783e91b5ff8c" />

## Tech Stack
- Pydantic
- Pandas
- Airflow
- dbt
- Postgres
- Docker
- Docker Compose
- Astro
    


## Prerequistes
- Python
- SQL
- Docker Desktop
- Astro CLI
- 
## How to run this project
1. Clone the repository
2. Create your .env file at the
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
BASE_URL=
PGADMIN_DEFAULT_EMAIL=
PGADMIN_DEFAULT_PASSWORD=
4. Inside the db folder, run docker compose up -d
5. Inside the airflow folder run, astro dev start
6. Go to http://localhost:8080/ and unpause comics_initial_load and trigger it manually once
7. Unpause comics_incremental and now it will run based on sensor polling or schedule

## test
1. Clone the Repository
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

2. Create Your .env File

Create a .env file with the following variables:

POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
BASE_URL=
PGADMIN_DEFAULT_EMAIL=
PGADMIN_DEFAULT_PASSWORD=

3. Start PostgreSQL and PGAdmin

From the db/ directory, run:

cd db
docker-compose up -d


This starts:

PostgreSQL at localhost:5440

PGAdmin at localhost:5050

4. Start Airflow

From the airflow/ directory, run:

cd ../airflow
astro dev start


Airflow UI will be available at:

👉 http://localhost:8080/

5. Run the Initial Load Pipeline

In the Airflow UI:

Find the DAG comics_initial_load

Unpause it

Trigger it manually once

This performs the full historical load and builds all dbt models.

6. Enable Incremental Ingestion

When the initial load completes:

Unpause comics_incremental

It will now run automatically based on:

Sensor polling, or

The CRON schedule you set
## Future enhancements
- Implement multithreading to speed up initial load
- Add CI/CD pipeline for changes to pipeline
- Use metabase for dashboarding
- Implement incremental building of dbt tables as opposed to rebuilding each time
- 
