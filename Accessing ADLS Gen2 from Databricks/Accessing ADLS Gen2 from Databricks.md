### This section will provide you the steps to access Azure Data Lake Gen2 from Azure Databricks
You can use Access Keys, SAS Token or Service Principle for the authentication to access the ADLS Gen2 from Azure Databricks. But for this session, we will use Service Principal as it will provide full access to the storage account. Service Principal can be registered in Azure Active Directory and assign the permissions required to access the services in the Azure Subscription via Role Based Access Control (RBAC).

![alt_text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/Service%20Principal.png?raw=true)

## Pre-requisite:
1. ADLS Gen2
2. Azure Key Vault
3. Azure AD Application

## Steps:
1. Register Service Principal/Azure AD Application. Generate the secret for the application and store the secret value in the Azure Key Vault together with the Application ID & Tenant ID.

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/Azure%20AD.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/azure%20AD%20secret.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/client%20id.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/tenant%20id.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/client%20secret.png?raw=true)


2. Create a secret scope in Databricks (to link Databricks with Azure Key Vault). You can create the Secret Scope by going to the Databricks homepage and adding ‘secrets/createScope’ at the end of the URL. This need to be done manually because the Databricks keeps the Secret Scope in a hidden user interface.

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/url.png?raw=true)

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/create%20Scope.png?raw=true)


3. Copy the value of Vault URI (from properties in Azure Key Vault page) and paste it in DNS Name and copy the Resource ID (from properties in Azure Key Vault page) and paste it in Resource ID in the Databricks scope configuration.

![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/KeyVault%20properties.png?raw=true)


4. Configure Databricks to access the storage account via Service Principal using Spark Config. Use Databricks Secrets Utility to retrieve the secret value from Azure Key Vault.(dbutils.secrets.get(scope=’<Databricks secret scope>’, key=’<secret key>’))
![alt_text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/d5c27ee5a63bac89c8dcb02d5947538cdfba37f2/Accessing%20ADLS%20Gen2%20from%20Databricks/mount%20ADLS%20(python).png?raw=true)
