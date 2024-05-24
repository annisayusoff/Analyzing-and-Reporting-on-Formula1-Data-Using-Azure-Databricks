# Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks

In this project, we will implement Azure Databricks, Azure Data Lake Storage Gen2 and Azure Data Factory for analysing and reporting on Formula1 motor racing data.

The data that we are going to use in this project is from http://ergast.com/mrd/.

The project requirements are as follow:

**1. Data Ingestion Requirements**
-	Ingest all files into the data lake.
-	Ingested data must have the schema applied.
-	Ingested data must have audit columns.
-	Ingested data must be stored in columnar format (i.e. Parquet).
-	Must be able to analyze the ingested data via SQL.
-	Ingestion logic must be able to handle incremental load.
  
**2. Data Transformation Requirements**
-	Join the key information required for reporting to create a new table.
-	Join the key information required for Analysis to create a new table.
-	Transformed tables must have audit columns.
-	Must be able to analyze the transformed data via SQL.
-	Transformed data must be stored in columnar format (i.e. Parquet).
-	Transformation logic must be able to handle incremental load.

**3. Reporting Requirements**
-	Driver Standings.
-	Constructor Standings.

**4. Analysis Requirements**
-	Dominant Drivers
-	Dominant Teams.
-	Visualize the outputs.
-	Create Databricks Dashboards.

**5. Scheduling Requirements**
-	Scheduled to run every Sunday 10PM.
-	Ability to monitor pipelines.
-	Ability to re-run failed pipelines.
-	Ability to set-up alerts on failures.

**6. Other Non-Functional Requirements**
-	Ability to delete individual records.
-	Ability to see history and time travel.
-	Ability to roll back to a previous version.

The solution architecture is as below image:
