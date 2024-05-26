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


### 3. Create Pipeline

#### i. Pipeline to ingest file from ADLS Gen2 using Databricks Notebook.

1. Select Pipeline > Activities > Databricks > Notebook
2. Select the linked service that has been created previously in **"Azure Databricks"** tab.
3. In **"Settings"** tab, specify the Notebook path in Databricks workspace. (I have uploaded the notebook in **"ingestion"** folder)
4. Create a pipeline variable by clicking anywhere outside the activity for dynamic parsing of parameter value for **file_dat**e and **data_source** that we have created in Databricks notebook.
   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/94f25675f8f377ea19e0afeae6c5f4bcaf822f95/Azure%20Data%20Factory/pipeline%20variable%20(v_data_source).png?raw=true)

   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/tree/0f65de7ab375021e7d856bf6d7674c1dc691074d/Azure%20Data%20Factory?raw=true)
   
5. Since we use databricks widgets parameter, **"p_data_source"** and **"p_file_date"**, we need to specify this in the configuration of the pipeline **activity** in the **"Base parameters"** section in **"Settings"** tab. To make it dynamic, we will define a dynamic content by using the variable and parameter of the pipeline that we have created in step 4, instead of hard code it as follows:
   i. p_data_source : @variables('v_data_source')
   ii. p_file_date : @formatDateTime(pipeline().parameters.p_window_end_date, 'yyy-MM-dd')

   [!alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/2fc0bb2b00e4dd6ddd8cf4fcffc8a83f2520edca/Azure%20Data%20Factory/Databricks%20activity%20parameters.png?raw=true)

6. 
