# README
This repo outlines the process of handling GDELT data using Databricks and PySpark. The provided scripts are designed to process Event, Mention, and Global Knowledge Graph (GKG) datasets. Key operations include data filtering, aggregation, and exporting the final results to Azure Blob Storage for further analysis.

## Databricks Free Edition for Academia

Databricks is progressively collaborating with universities worldwide to provide access to the **Free Edition**.  

> ⚠️ Users are limited to smaller compute and warehouse sizes.

🔗 [Learn more about Free Edition](https://www.databricks.com/learn/free-edition)

## Files Added
1. **`updated_event.py`**
   - Processes GDELT event data.
   - Mounts Azure Blob Storage containers.
   - Filters and aggregates event data based on specific criteria (e.g., country codes, Goldstein scale).
   - Saves processed data to Yuqi's storage account.

2. **`updated_mention.py`**
   - Processes GDELT mention data.
   - Mounts Azure Blob Storage containers.
   - Filters mention data by specific columns (e.g., confidence, mention time).
   - Aggregates confidence scores by date and saves results to Yuqi's storage account.

3. **`updated_gkg.py`**
   - Processes GDELT GKG data.
   - Mounts Azure Blob Storage containers.
   - Filters GKG data for themes containing "ENV" and calculates average tone.
   - Aggregates report counts and average tone by date and saves results to Yuqi's storage account.

## Key Features
- **Azure Blob Storage Integration**: The scripts mount containers from two storage accounts (`yuqidatafile` and `sandboxtestingyuqi`) and save processed data back to the storage.
- **Dynamic Column Handling**: Handles varying column counts in CSV files and dynamically assigns column names.
- **Date-Based Filtering**: Processes data within a specified date range (2019-06-30 to 2024-06-30).
- **Data Aggregation**:
  - Event data: Aggregates Goldstein scale by day.
  - Mention data: Aggregates confidence scores by day.
  - GKG data: Aggregates average tone and report counts by day.
- **Environment-Specific Filtering**: Filters GKG data for themes related to the environment (`ENV`).

## Step-by-Step Instructions

### 1. Prepare the Azure Blob Storage
1. **Upload Data**:
   - Log in to your Azure portal: [Azure Portal](https://portal.azure.com/).
   - Navigate to the **Storage Accounts** section and locate the accounts `yuqidatafile` and `sandboxtestingyuqi`.
   - Create containers for the data:
     - `gdeltmencsvcombined`
     - `gdelteventcsvcombined`
     - `gdeltgkgcsvcombined`
   - Upload the raw GDELT data files into the respective containers. For more details, refer to the [Azure Blob Storage Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/).

2. **Generate Access Keys**:
   - Go to the **Access Keys** section of each storage account.
   - Copy the `key1` or `key2` value for use in the scripts. Refer to [Manage Access Keys](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage).

### 2. Set Up Databricks
1. **Create a Databricks Workspace**:
   - Log in to your Azure portal.
   - Create a new Databricks workspace if you don’t already have one. Follow the [Azure Databricks Quickstart Guide](https://docs.azure.cn/en-us/databricks/getting-started).

2. **Create a Cluster**:
   - In the Databricks workspace, navigate to the **Clusters** section.
   - Create a new cluster with the following configurations:
     - Runtime Version: Select a version that supports PySpark (e.g., 11.x or later).
     - Node Type: Choose an appropriate node type based on your data size.
     - Autoscaling: Enable autoscaling for better resource management.
   - For more details, refer to the [Databricks Cluster Configuration Guide](https://learn.microsoft.com/en-us/azure/databricks/clusters/).

3. **Install Required Libraries**:
   - Go to the **Libraries** tab of your cluster.
   - Install the following libraries:
     - `pyspark`
     - `azure-storage-blob` (if needed for additional Azure operations).
   - Refer to [Managing Libraries in Databricks](https://learn.microsoft.com/en-us/azure/databricks/libraries/).

### 3. Import and Attach Notebooks
1. **Import Python Scripts as Notebooks**:
   - In the Databricks workspace, navigate to the **Workspace** section.
   - Click **Import** and upload the Python scripts (`updated_event.py`, `updated_mention.py`, `updated_gkg.py`).
   - Databricks will convert these scripts into notebooks. Refer to [Importing Notebooks](https://docs.databricks.com/aws/en/notebooks/share-code).

2. **Attach Notebooks to the Cluster**:
   - Open each notebook.
   - In the top-right corner, select the cluster you created earlier and attach the notebook to it.

### 4. Run the Notebooks
1. **Mount Azure Blob Storage**:
   - Ensure the `mount_containers` functions in the scripts are updated with the correct storage account names and keys.
   - Run the cells that mount the containers to Databricks. Refer to [Mount Azure Blob Storage](https://learn.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage).

2. **Process the Data**:
   - Execute the cells in sequence to process the data:
     - **`updated_event.py`**: Processes event data and saves results to `dbfs:/mnt/results/yuqi_test/event/`.
     - **`updated_mention.py`**: Processes mention data and saves results to `dbfs:/mnt/results/yuqi_test/mention/`.
     - **`updated_gkg.py`**: Processes GKG data and saves results to `dbfs:/mnt/results/yuqi_test/gkg/`.

3. **Verify Outputs**:
   - Use the Databricks file system (`dbfs:/`) to verify that the processed data is saved in the expected locations. Refer to [Databricks File System (DBFS)](https://learn.microsoft.com/en-us/azure/databricks/dbfs/).

## Output
- Processed data is saved to the following paths in Yuqi's storage account:
  - **Event Data**: `dbfs:/mnt/results/yuqi_test/event/`
  - **Mention Data**: `dbfs:/mnt/results/yuqi_test/mention/`
  - **GKG Data**: `dbfs:/mnt/results/yuqi_test/gkg/`

## Notes
- The scripts include error handling for mounting storage containers and loading data.
- Ensure the required libraries (`pyspark`, `os`, `datetime`) are available in the Databricks environment.
- The scripts are modular and can be extended for additional processing tasks.

## Useful Links
- [Azure Blob Storage Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/)
- [Azure Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/)
- [Databricks File System (DBFS)](https://learn.microsoft.com/en-us/azure/databricks/dbfs/)
- [Databricks Cluster Configuration Guide](https://learn.microsoft.com/en-us/azure/databricks/clusters/)
- [Importing Notebooks in Databricks](https://docs.databricks.com/aws/en/notebooks/share-code)
- [Managing Libraries in Databricks](https://learn.microsoft.com/en-us/azure/databricks/libraries/)

## Citation

If you use this repository or build on our work, please cite the following paper:

> Yuqi Zheng, Brian Lucey.  
> *What REALLY drives clean energy stocks - Fear or Fundamentals?*  
> Energy Economics, Volume 148, 2025, 108558.  
> [https://doi.org/10.1016/j.eneco.2025.108558]

**BibTeX:**
```bibtex
@article{zheng_and_lucey_2025,
  title = {What REALLY drives clean energy stocks - Fear or Fundamentals?},
  author = {Zheng, Yuqi and Lucey, Brian},
  journal = {Energy Economics},
  volume = {148},
  pages = {108558},
  year = {2025},
  issn = {0140-9883},
  doi = {10.1016/j.eneco.2025.108558},
  url = {https://www.sciencedirect.com/science/article/pii/S0140988325003822}
}
