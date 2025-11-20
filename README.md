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

## How to run this project

### 1. Clone the Repository
```bash
git clone 
cd XKCD-ELT-Project
```

### 2. Configure Environment Variables
Create a `.env` file in the db and airflow folder with the following variables:
```env
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
BASE_URL=
PGADMIN_DEFAULT_EMAIL=
PGADMIN_DEFAULT_PASSWORD=
```

### 3. Start the Database
Navigate to the `db` folder and start the Docker containers:
```bash
cd db
docker compose up -d
```

### 4. Start Airflow
Navigate to the `airflow` folder and start the Astro development environment:
```bash
cd airflow
astro dev start
```

### 5. Initialize the Data Pipeline
1. Open your browser and go to http://localhost:8080/
2. Find the `comics_initial_load` DAG and unpause it
3. Trigger it manually once to perform the initial data load

### 6. Enable Incremental Updates
Unpause the `comics_incremental` DAG. It will now run automatically based on sensor polling or the schedule.

---

## Future enhancements
- Implement multithreading to speed up initial load
- Add CI/CD pipeline for changes to pipeline
- Use metabase for dashboarding
- Implement incremental building of dbt tables as opposed to rebuilding each time
- 
