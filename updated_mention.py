# Databricks notebook source
- update to write to yuqi's storage account
- remove the collase 1 part, we can write out the csv files in one directory
- the ways of mount will require both Yuqi's and yuqi's storge account

# COMMAND ----------

def mount_containers_yuqi(container_name, mount_name, storage_account_name="yuqidatafile", storage_account_key=""):
    
    # Define the source URL for the container
    source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
    
    # Define the configuration for mounting
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
    
    # Check if the mount point already exists
    mounted = False
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == mount_name:
            print(f"Container {container_name} is already mounted at {mount_name}.")
            mounted = True
            break
    
    if not mounted:
        # Mount the container to the specified mount point
        try:
            dbutils.fs.mount(
                source = source_url,
                mount_point = mount_name,
                extra_configs = extra_configs
            )
            print(f"Successfully mounted {container_name} to {mount_name}.")
        except Exception as e:
            print(f"Failed to mount {container_name}. Error: {str(e)}")

# COMMAND ----------

def mount_containers_yuqi(container_name, mount_name, storage_account_name="sandboxtestingyuqi", storage_account_key=""):
    
    # Define the source URL for the container
    source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
    
    # Define the configuration for mounting
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
    
    # Check if the mount point already exists
    mounted = False
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == mount_name:
            print(f"Container {container_name} is already mounted at {mount_name}.")
            mounted = True
            break
    
    if not mounted:
        # Mount the container to the specified mount point
        try:
            dbutils.fs.mount(
                source = source_url,
                mount_point = mount_name,
                extra_configs = extra_configs
            )
            print(f"Successfully mounted {container_name} to {mount_name}.")
        except Exception as e:
            print(f"Failed to mount {container_name}. Error: {str(e)}")

# COMMAND ----------

mount_containers_yuqi("results", "/mnt/results")

# COMMAND ----------

mount_containers_yuqi("gdeltmencsvcombined", "/mnt/gdeltmen_csv_combined")
mount_containers_yuqi("gdelteventcsvcombined", "/mnt/gdeltevent_csv_combined")
mount_containers_yuqi("gdeltgkgcsvcombined", "/mnt/gdeltgkg_csv_combined")

# COMMAND ----------

from pyspark.sql.functions import col
import os
from datetime import datetime, timedelta

# Define the base path
base_path = "/dbfs/mnt/gdeltmen_csv_combined"

output_path = "dbfs:/mnt/results/yuqi_test/mention/gdeltmen_2_cutfile/"

# Function to generate a list of paths based on a date range
def generate_date_paths(base_path, start_date, end_date):
    date_paths = []
    current_date = start_date
    while current_date <= end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")
        date_path = f"{base_path}/{year}/{month}/{day}"
        if os.path.exists(date_path):  # Check if the directory exists
            date_paths.append(date_path)
        current_date += timedelta(days=1)
    return date_paths

# Input the date range
start_date_str = "2019-06-30"
end_date_str = "2024-06-30"

# Convert strings to datetime objects
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

# Generate all possible date paths within the range
date_paths = generate_date_paths(base_path, start_date, end_date)

# Initialize list to store all file paths
all_files = []

# Traverse the directory structure based on the date paths
for date_path in date_paths:
    for root, dirs, files in os.walk(date_path):
        for file in files:
            if file.endswith(".mentions.CSV"):
                # Convert local path to DBFS path
                dbfs_path = root.replace("/dbfs", "dbfs:") + "/" + file
                all_files.append(dbfs_path)

# Check if any files were found
if not all_files:
    print("No files found matching the pattern.")
else:
    # Read all CSV files matching the pattern
    df_all = spark.read.csv(all_files, sep="\t", header=False).toDF(
         "Global_Event_ID", "EventTimeDate", "MentionTimeDate", "MentionType", "MentionSourceName", "MentionIdentifier","SentenceID", "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset","InRawText", "Confidence", "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo","Extras"
    )

    # Check if data was loaded
    if df_all.isEmpty():
        print("No files found matching the pattern.")
    else:
        print(f"Loaded {df_all.count()} rows of data.")

        # Save the combined DataFrame to a single CSV file
        # df_all.coalesce(1).write.option("header", "true").csv(output_path, mode="overwrite")
        df_all.write.option("header", "true").csv(output_path, mode="overwrite")
        # print(f"Successfully saved the combined data: {output_path} +"/"+ {output_filename} ")

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/mention/gdeltmen_2_cutfile/", header=True)
# display(df)

# COMMAND ----------

#筛选ID,时间和置信区间
from pyspark.sql import functions as F

# Load the CSV file
file_path = "dbfs:/mnt/results/yuqi_test/mention/gdeltmen_2_cutfile/"  # Update this path

try:
    # Read the CSV file into a DataFrame
    df = spark.read.csv(file_path, header=True)

    # Select the desired columns, ensuring to escape the column name with special characters
    result_df = df.select(F.col("Global_Event_ID"), F.col('MentionTimeDate'), F.col('MentionIdentifier'), F.col('Confidence'))

    # Specify the path where you want to save the filtered DataFrame for review
    output_path = "dbfs:/mnt/results/yuqi_test/mention/gdeltmen_3_filtered/"
    # display(result_df)

    result_df.write.option("header", "true").csv(output_path, mode="overwrite")
    print(f"Filtered DataFrame saved to: {output_path}")

except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/mention/gdeltmen_3_filtered/", header=True)
# display(df)

# COMMAND ----------

from pyspark.sql import functions as F

# 加载 CSV 文件
df = spark.read.csv("dbfs:/mnt/results/yuqi_test/mention/gdeltmen_3_filtered/", header=True, inferSchema=True)

# 提取 mentiontimedate 的前八位 (yymmdd)
df = df.withColumn("yymmdd", F.col("mentiontimedate").substr(1, 8))

# 计算相同 yymmdd 的平均 confidence
result = df.groupBy("yymmdd").agg(F.avg("confidence").alias("average_confidence"))

# 显示结果
result.show()

result.write.option("header", "true").csv("dbfs:/mnt/results/yuqi_test/mention/gdeltmen_4_avgconfidence/", mode="overwrite")

# COMMAND ----------

