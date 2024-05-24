### This section will provide you the steps to create ADLS Gen2 to be used in this project

1. We will use Azure Blob Storage and serve it as our data lake. Azure blob storage account can be created in Azure Portal as below.
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/cc5ff3beea3e95e74fdf32c6c3312b1d5667457a/Azure%20Data%20Lake%20Gen2/ADLS%201.png?raw=true)

2. Ensure that you enable the below option to create ADLS Gen2. Else, you will only create the ADLS Gen1. You may wonder the difference between ADLS Gen2 and ADLS. Basically, ADLS Gen2 provide hierarchical namespace instead of flat namespace that is offered in Gen1. This means that ADLS Gen2 uses directories to organize data, which provides a more efficient way to manage large amounts of data.
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/cc5ff3beea3e95e74fdf32c6c3312b1d5667457a/Azure%20Data%20Lake%20Gen2/ADLS%202.png?raw=true) 

3. Then, you can create the containers to store the data and finally you can start uploading your data to the container.
![alt text](https://github.com/annisayusoff/Analysing-and-Reporting-on-Formula1-Data-Using-Azure-Databricks/blob/cc5ff3beea3e95e74fdf32c6c3312b1d5667457a/Azure%20Data%20Lake%20Gen2/ADLS%203.png?raw=true)
