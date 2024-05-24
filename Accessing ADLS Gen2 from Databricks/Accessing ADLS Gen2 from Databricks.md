### This section will provide you the steps to access Azure Data Lake Gen2 from Azure Databricks
You can use Access Keys, SAS Token or Service Principle for the authentication to access the ADLS Gen2 from Azure Databricks. But for this session, we will use Service Principal as it will provide full access to the storage account. Service Principal can be registered in Azure Active Directory and assign the permissions required to access the services in the Azure Subscription via Role Based Access Control (RBAC).

## Pre-requisite:
1. ADLS Gen2
2. Azure Key Vault
3. Azure AD Application

## Steps:
1. Register Service Principal/Azure AD Application. Generate the secret for the application and store the secret value in the Azure Key Vault together with the Application ID & Tenant ID.
2. Create a secret scope in Databricks (to link Databricks with Azure Key Vault). You can create the Secret Scope by going to the Databricks homepage and adding ‘secrets/createScope’ at the end of the URL. This need to be done manually because the Databricks keeps the Secret Scope in a hidden user interface.
3. Copy the value of Vault URI (from properties in Azure Key Vault page) and paste it in DNS Name and copy the Resource ID (from properties in Azure Key Vault page) and paste it in Resource ID in the Databricks scope configuration.
   
