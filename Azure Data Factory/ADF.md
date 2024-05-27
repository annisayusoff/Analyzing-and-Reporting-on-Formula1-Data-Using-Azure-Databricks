# Orchestration in Azure Data Factory

In this section, I will explain on how to automate the process of ETL in Databricks using ADF.

## Steps:

### 1. Create a Data Factory
### 2. Create a Linked Service
-  To access Databricks and ADLS Gen2 from ADF, you need to use Linked Service.

#### i. Linked Service to Databricks (use Managed Service Identity):

For the MSI authentication:
1. Copy the ADF managed identity name (highlighted in yellow)
2. Go to IAM access control in Databricks workspace
3. Click **"+ Add"** button.
4. Select **"Add role assignment"**
5. Select **"contributor"** option in **"Role"** field.
6. Paste the ADF managed identity name in **"Select"** field.
7. Click **"Save"**

Now the ADF has the contributor access to the Databricks workspace.

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/1284ec8f0562674052a081f574d7c90e9df1b3c0/Azure%20Data%20Factory/linked%20service-databricks.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/88141689381782a072fe2f2d1221dba39db7e9f4/Azure%20Data%20Factory/IAM%20Databricks.png?raw=true)

                           
#### ii. Linked Service to ADLS Gen2:
1. Name the Linked Service as ls_formula1dl_storage.
2. Select **"Account key"** in the **"Authentication method"** field.
3. Select **"formula1dl"** in the **"Storage account name"** field.



### 3. Create a Dataset
- To access ADLS Gen2, ADF needs both Linked Service and Dataset (to define the folder of the file).
- Steps:
  1. Select Dataset > ADLS Gen2 > JSON. Name it as ds_formula1_raw.
  2. Select the Linked service that we have created in 2.ii.

     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d4f3531c10c959f25f23db791384a1c8dceb99c8/Azure%20Data%20Factory/linked%20service-ADLS.png?raw=true)
     
     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d4f3531c10c959f25f23db791384a1c8dceb99c8/Azure%20Data%20Factory/linked%20service-ADLS3.png?raw=true)
     
     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d4f3531c10c959f25f23db791384a1c8dceb99c8/Azure%20Data%20Factory/linked%20service-ADLS2.png?raw=true)


### 4. Create Pipeline

#### i. Pipeline to ingest file from ADLS Gen2 using Databricks Notebook.

1. Create a pipeline and name it **pl_ingest_formula1_data**. Select Pipeline > Activities > Databricks > Notebook
2. Select the linked service that has been created previously in **"Azure Databricks"** tab.
3. In **"Settings"** tab, specify the Notebook path in Databricks workspace. (I have uploaded the notebook in **"ingestion"** folder)
4. Create a pipeline variable by clicking anywhere outside the activity for dynamic parsing of parameter value for **file_dat**e and **data_source** that we have created in Databricks notebook.
   
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/94f25675f8f377ea19e0afeae6c5f4bcaf822f95/Azure%20Data%20Factory/pipeline%20variable%20(v_data_source).png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/9623bff8579399be6514a2e7dd343d3514a7f782/Azure%20Data%20Factory/pipeline%20variable%20%20(p_file_date).png?raw=true)
   
5. Since we use databricks widgets parameter, **"p_data_source"** and **"p_file_date"**, we need to specify this in the configuration of the pipeline **activity** in the **"Base parameters"** section in **"Settings"** tab. To make it dynamic, we will define a dynamic content by using the variable and parameter of the pipeline that we have created in step 4, instead of hard code it as follows:
   - p_data_source : @variables('v_data_source')
   - p_file_date : @formatDateTime(pipeline().parameters.p_window_end_date, 'yyy-MM-dd')
     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/02451a4b8bea7fdfd2d8a47d8c4ac884833b06ff/Azure%20Data%20Factory/Databricks%20activity%20parameters.png?raw=true)

6. Repeat the same for other ingestion files. Make sure that there is no dependencies between each of the activities. Run it in parallel.


#### ii. Pipeline to ingest file from ADLS Gen2 using Databricks Notebook.
Since the formula1 race will not be conducted every week for the whole year, and we are going to trigger by weekly, we need to ensure that the pipeline can handle missing data (week with no race). Thus, we will use **"Get Metadata"** activity to retrieve the date metadata from the race file in ADLS.

1. Using the same pipeline, add **Get Metadata** activity.
2. 


