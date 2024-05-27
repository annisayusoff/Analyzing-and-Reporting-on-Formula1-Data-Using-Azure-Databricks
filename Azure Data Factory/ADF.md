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
  
       ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d4f3531c10c959f25f23db791384a1c8dceb99c8/Azure%20Data%20Factory/linked%20service-ADLS.png?raw=true)
       
       ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d4f3531c10c959f25f23db791384a1c8dceb99c8/Azure%20Data%20Factory/linked%20service-ADLS3.png?raw=true)
       
       ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d4f3531c10c959f25f23db791384a1c8dceb99c8/Azure%20Data%20Factory/linked%20service-ADLS2.png?raw=true)


### 3. Create a Dataset
- To access ADLS Gen2, ADF needs both Linked Service and Dataset (to define the folder of the file).
- Steps:
  1. Select Dataset > ADLS Gen2 > JSON. Name it as ds_formula1_raw.
  2. Select the Linked service that we have created in 2.ii. (**"ls_formula1dl_storage"**)
  3. For the file path, we want to make it dynamic. Thus, we need to create a parameter in **"Parameters"** tab. Name it **"p_window_end_date"**.
  - /raw/@formatDateTime(dataset().p_window_end_date, 'yyy-MM-dd')

    ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/2c703aa62e44e0cb867705ba88db511ab849daf1/Azure%20Data%20Factory/dataset1-ADLS.png?raw=true)
    
    ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/2c703aa62e44e0cb867705ba88db511ab849daf1/Azure%20Data%20Factory/dataset2-ADLS.png?raw=true)


### 4. Create Pipeline for Data Ingestion

  #### i. Pipeline to ingest file from ADLS Gen2 using Databricks Notebook.
  
  1. Create a pipeline and name it **pl_ingest_formula1_data**. Select Pipeline > Activities > Databricks > Notebook
  2. Select the linked service that has been created previously in **"Azure Databricks"** tab.
  3. In **"Settings"** tab, specify the Notebook path in Databricks workspace. (I have uploaded the notebook in **"ingestion"** folder)
  4. Create a pipeline variable by clicking anywhere outside the activity for dynamic parsing of parameter value for **file_date** and **data_source** that we have created in Databricks notebook.
     
  ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/94f25675f8f377ea19e0afeae6c5f4bcaf822f95/Azure%20Data%20Factory/pipeline%20variable%20(v_data_source).png?raw=true)
  
  ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/9623bff8579399be6514a2e7dd343d3514a7f782/Azure%20Data%20Factory/pipeline%20variable%20%20(p_file_date).png?raw=true)
     
  5. Since we use databricks widgets parameter, **"p_data_source"** and **"p_file_date"**, we need to specify this in the configuration of the pipeline **activity** in the **"Base parameters"** section in **"Settings"** tab. To make it dynamic, we will define a dynamic content by using the variable and parameter of the pipeline that we have created in step 4, instead of hard code it as follows:
     - p_data_source : @variables('v_data_source')
     - p_file_date : @formatDateTime(pipeline().parameters.p_window_end_date, 'yyy-MM-dd')
       ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/02451a4b8bea7fdfd2d8a47d8c4ac884833b06ff/Azure%20Data%20Factory/Databricks%20activity%20parameters.png?raw=true)
  
  6. Repeat the same for other ingestion files. Make sure that there is no dependencies between each of the activities. Run it in parallel.
  
  
  #### ii. Pipeline to ingest file from ADLS Gen2 using Databricks Notebook.
  Since the formula1 race will not be conducted every week for the whole year, and we are going to trigger by weekly, we need to ensure that the pipeline can handle missing data (week with no race). Thus, we will use **"Get Metadata"** activity to retrieve the date metadata from the race file in ADLS.
  
  1. Using the same pipeline, add **Get Metadata** activity and name it **"Get Folder Details"**
  
  2. In the **"Dataset"** tab, select the dataset that we have created in Part 3, **ds_formula1_raw**. Parse the pipeline parameter to the dataset parameter and select **"Exists"** in the **"argument"** field as below image:
     p_window_end_date = @pipeline().parameters.p_window_end_date
     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/f7c91cb7168feb257922b8adca06d92b3c91f892/Azure%20Data%20Factory/get-metadata1.png?raw=true)
  
  3. Add **If Condition** activity. Define the **Expression** as **@activity("Get Folder Details").output.Exists**
  
     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/c8ea5b030e90752a12f57b08bae79ef0d849c42b/Azure%20Data%20Factory/pl_ingest_1.png?raw=true)
  
  4. Copy all the activities that we have created in Part 4 and paste it in the True statement of the if condition. Leave the False statement empty. The pipeline should look like below image (you can see that there are 8 activities in True statement. This is because we have 8 files to be ingested):
     ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/c8ea5b030e90752a12f57b08bae79ef0d849c42b/Azure%20Data%20Factory/pl_ingest_2.png?raw=true)


### 5. Create Pipeline for Data Transformation
1. Clone the pipeline we have created in Part 4 and name it **"pl_transform_formula1_data"**.
2. Change the configuration of the Databricks Notebook activity so that it will link to the correct notebook for transformation.
3. The final pipeline should look like this:

   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/c8ea5b030e90752a12f57b08bae79ef0d849c42b/Azure%20Data%20Factory/pl_transform.png?raw=true)


### 6. Master Pipeline
1. Create a new pipeline and name it "pl_process_formula1_data".
2. Create a pipeline parameter **"p_window_end_date"**.
3. Add **"Execute Pipeline"** activity and name it **Execute Ingestion**. Select pl_ingest_formula1_data pipeline in the **"Invoked pipeline"** field in the **"Settings"** tab. Define the parameter as @pipeline().parameters.p_window_end_date.
4. Repeat for pl_transform_formula1_data pipeline and name it **"Execut Transformation"** and link these 2 pipelines.
5. The final pipeline should look like this:
   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/9c4faa38f3c42375543a2641bfc76273e81cb279/Azure%20Data%20Factory/pl_process.png?raw=true)


### 7. Create a Trigger
- To run the pipeline every week automatically, we will use trigger.

1. In ADF, go to Manage > Author > Triggers > + New.
2. Name the trigger as tr_process_formula1_data.
3. Select the **"Tumbling window"** trigger. The date should have a 1 week duration. The end date must match the day that the race file will come in into the ADLS. 
4. The trigger configuration should look like this:
   
   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/7428787e08d558df42f50fde304c700f1ea16fb8/Azure%20Data%20Factory/trigger1.png?raw=true)
   
   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/7428787e08d558df42f50fde304c700f1ea16fb8/Azure%20Data%20Factory/trigger2.png?raw=true)
   
6. Add the created trigger to the pl_process_formula1_data.
7. A configuration window of the trigger will pop up and you will be prompted to edit the trigger and pipeline parameter (p_window_end_date)
8. Since the file date is the same as the end date of the trigger duration, we will make this parameter dynamic by parsing the end date of the trigger duration to the pipeline parameter.
   p_window_end_date = @trigger().outputs.windowEndTime

   ![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/eeed038a29ebd16fee22e32d1985a05da43477c1/Azure%20Data%20Factory/trigger3.png?raw=true)


**Now, your pipeline will be automatically run.**
