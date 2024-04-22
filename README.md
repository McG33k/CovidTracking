# CovidTracking

## Introduction
Welcome to the Covid Tracking ETL Pipeline project repository. This project is part of a DataOps engineer challenge designed to demonstrate capabilities in integrating data from public APIs into a centralized database using modern data operations tools and best practices.

## Project Structure
This project uses Docker Compose to orchestrate an environment consisting of Python for scripting, Apache Airflow for task scheduling, and PostgreSQL for data storage.

### Components
- **Python**: Scripting language used to extract data from the API.
- **Airflow**: Manages task scheduling, workflow automation, and data pipelines.
- **PostgreSQL**: Database used for storing extracted data.
- **Docker Compose**: Used to define and run multi-container Docker applications.

## API Reference
The data is sourced from the Covid Tracking Project's historical data API, specifically the endpoint:
curl https://api.covidtracking.com/v2/us/daily/2021-01-01/simple.json

## Setup Instructions
1. **Clone the repository:**
git clone https://github.com/McG33k/CovidTracking.git
2. **Navigate to the project directory:**
cd CovidTracking
3. **Build and run the Docker containers:**
docker-compose up --build
4. **Access the Airflow web interface:**
- Open a web browser and navigate to `http://localhost:8080`.
- Use the default credentials if required (airflow/airflow).
5. **Trigger the data pipeline:**
- In Airflow's web interface, find the DAG named covidtracking_pipeline, and manually trigger it or wait for its scheduled run (currently configured to run after 1 hour).

## Database Configuration
The PostgreSQL database is configured with the following credentials:
- **Database Server**: dataops
- **Database Name**: airflow
- **Table Name**: covidtracking
- **User**: airflow
- **Password**: airflow

## How to Use the Data
Once the data pipeline runs successfully, data will be available in the `covidtracking` table. You can connect to the PostgreSQL database using any PostgreSQL client with the above credentials to query or manage the data.

## Considerations and Assumptions
- **API Limitations**: Since data collection has stopped as of March 2021, this system is built to handle historical data extraction. Should the API resume updates, the system is designed to incorporate new data seamlessly.
- **System Scalability**: Designed to handle increases in data volume with minimal configuration adjustments.
- **Error Handling**: Includes basic error handling for API connectivity issues, with potential for more sophisticated mechanisms.



