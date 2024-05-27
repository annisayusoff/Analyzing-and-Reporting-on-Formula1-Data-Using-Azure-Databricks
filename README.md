# Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks

In this project, we will implement Azure Databricks, Azure Data Lake Storage Gen2 and Azure Data Factory for analysing and reporting on Formula1 motor racing data.

The data that we are going to use in this project is from http://ergast.com/mrd/. The language that we will be using are pyspark and SQL.

## Project Requirements

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

## Solution Architecture
The solution architecture that will be implemented in this project is as below image:
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/ec9c4496d91100b1d639d5034b9636e39fbd8a35/solution%20architecture.png?raw=true)

## Databricks Report
Finally, the output from the analysis that we have done is shown in below report:
1. Dominant Drivers:
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/1774c2cf390b3f3fb9aa85229f641d8de48cd0aa/report/Dominant%20Drivers%20Report.png?raw=true)

2. Dominant Teams:
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/1774c2cf390b3f3fb9aa85229f641d8de48cd0aa/report/Dominant%20Teams%20Report.png?raw=true)


## Pipeline in ADF
The pipeline can be automatically invoked using trigger in ADF. Below is the final pipeline to process the formula1 data. You can read more about the process to create this pipeline here : https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/3ae23c99197d9e2456a6c11c3513ef30b0517e37/Azure%20Data%20Factory/ADF.md. 

### pl_process_formula1_data
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/3ae23c99197d9e2456a6c11c3513ef30b0517e37/Azure%20Data%20Factory/pl_process.png?raw=true)

### pl_ingest_formula1_data
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/3ae23c99197d9e2456a6c11c3513ef30b0517e37/Azure%20Data%20Factory/pl_ingest_2.png?raw=true)

### pl_transform_formula1_data
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/3ae23c99197d9e2456a6c11c3513ef30b0517e37/Azure%20Data%20Factory/pl_transform.png?raw=true)

<br>
<br>

**- To read on:**

**1. How to link Databricks to ADLS Gen2:**

- https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/3ae23c99197d9e2456a6c11c3513ef30b0517e37/Accessing%20ADLS%20Gen2%20from%20Databricks/Accessing%20ADLS%20Gen2%20from%20Databricks.md

**2. How to develop pipeline in ADF:**

- https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/311150646033342eea17cf33ffcd4335b7656dec/Azure%20Data%20Factory/ADF.md

<br>
<br>
<br>
Thank you!!
