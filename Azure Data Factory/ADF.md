# Orchestration in Azure Data Factory

In this section, I will explain on how to automate the process of ETL in Databricks using ADF.

## Steps:

### 1. Create a Data Factory
### 2. Create a Linked Service
-  To access Databricks and ADLS Gen2 from ADF, you need to use Linked Service.

#### i. Linked Service to Databricks (use Managed Service Identity):

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/1284ec8f0562674052a081f574d7c90e9df1b3c0/Azure%20Data%20Factory/linked%20service-databricks.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/88141689381782a072fe2f2d1221dba39db7e9f4/Azure%20Data%20Factory/IAM%20Databricks.png?raw=true)

For the MSI authentication:
1. Copy the ADF managed identity name (highlighted in yellow)
2. Go to IAM access control in Databricks workspace
3. Click **"+ Add"** button.
4. Select **"Add role assignment"**
5. Select **"contributor"** option in **"Role"** field.
6. Paste the ADF managed identity name in **"Select"** field.
7. Click **"Save"**

Now the ADF has the contributor access to the Databricks workspace.
                                     
#### i. Linked Service to ADLS Gen2:
