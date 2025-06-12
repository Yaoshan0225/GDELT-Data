# Databricks notebook source
2019-06-30 to 2024-06-30

# COMMAND ----------

def mount_containers(container_name, mount_name, storage_account_name="yuqidatafile", storage_account_key=""):
    
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

# Example usage:
# mount_containers(container_name="gdeltgkg", mount_name="/mnt/gdeltgkg")
# If you want to provide a different storage account name and key, you can call:
# mount_containers(container_name="gdeltgkg", mount_name="/mnt/gdeltgkg", storage_account_name="your_storage_account", storage_account_key="your_storage_key")

# COMMAND ----------

mount_containers("gdeltmencsvcombined", "/mnt/gdeltmen_csv_combined")
mount_containers("gdelteventcsvcombined", "/mnt/gdeltevent_csv_combined")
mount_containers("gdeltgkgcsvcombined", "/mnt/gdeltgkg_csv_combined")

# COMMAND ----------

mount_containers("gdeltevent2cutfile", "/mnt/gdeltevent2cutfile")

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

from pyspark.sql.functions import col
from datetime import datetime, timedelta
import os

# Define the base path
base_path = "/dbfs/mnt/gdeltevent_csv_combined"
# output_path = "dbfs:/mnt/gdeltevent2cutfile/yuqi_test/gdeltevent2cutfile.csv"

output_path = "dbfs:/mnt/results/yuqi_test/event/gdeltevent_2_cutfile/"

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
            if file.endswith(".export.CSV"):
                # Convert local path to DBFS path
                dbfs_path = root.replace("/dbfs", "dbfs:") + "/" + file
                all_files.append(dbfs_path)

# Check if any files were found
if not all_files:
    print("No files found matching the pattern.")
else:
    # Read the first CSV file to determine the correct number of columns
    sample_df = spark.read.csv(all_files[0], sep="\t", header=False)
    num_columns = len(sample_df.columns)

    # Dynamically generate column names based on the number of columns
    column_names = [
        "Global_Event_ID", "Day", "YYYYMM", "YYYY", "FRACTIONDATE_4", 
        "Actor_1_Code_5", "Actor_1_Name_6", "Actor_1_Country_CODE_7", 
        "Actor_1_Known_Group_Code_8", "Actor_1_Ethnic_Code_9", 
        "Actor_1_Religion_Code_10", "Actor_1_Religion_2_Code_11", 
        "Actor_TYPE1_CODE_12", "Actor_TYPE2_CODE_13", "Actor_TYPE3_CODE_14", 
        "Actor2_Code_15", "Actor2_Name_16", "Actor2_Country_CODE_17", 
        "Actor_2_Know_Group_Code_18", "Actor_2_Ethnic_Code_19", 
        "Actor_2_Religion_Code_20", "Actor_2_Religion_2_Code_21", 
        "Actor2_TYPE1_CODE_22", "Actor2_TYPE2_CODE_23", "Actor2_TYPE3_CODE_24", 
        "Is_Root_Event_25", "Event_Code_26", "Event_Base_Code_27", 
        "Event_Root_Code_28", "Quad_Class_29", "Goldstein_Scale_30", 
        "Num_Mentions_31", "Num_Sources_32", "Num_Articles_33", 
        "AVG_TONE_34", "Actor_1_Geo_Type_35", "Actor_1_Geo_FullName_36", 
        "Actor_1_Geo_Country_Code_37", "Actor1Geo_ADM1Code_38", 
        "Actor1Geo_LAT_39", "Actor1Geo_Long_40", "Actor1Geo_FeatureID_41", 
        "Actor_2_Geo_Type_42", "Actor_2_Geo_FullName_43", 
        "Actor_2_Geo_Country_Code_44", "Actor2Geo_ADM1Code_45", 
        "Actor2Geo_Lat_46", "Actor2Geo_Long_47", "Actor2Geo_FeatureID_48", 
        "ACTION_GEO_TPYE_49", "ACTION_GEO_FULLNAME_50", "ACTION_COUNTRY_CODE_51", 
        "ACTION_GEO_ADM1Code_52", "ACTION_GEO_LAT_53", "ACTION_GEO_Long_54", 
        "ACTION_GEO_FeaturID_55", "Date_Added_56", "Source_URL_57", 
        "V1THEMES"
    ]

    # Adjust the length of the column name list to match the actual number of columns
    column_names = column_names[:num_columns] if num_columns <= len(column_names) else column_names + [f"col_{i+len(column_names)+1}" for i in range(num_columns - len(column_names))]

    # Read all matching CSV files into a single DataFrame
    df_all = spark.read.csv(all_files, sep="\t", header=False).toDF(*column_names)

    # Check if any data was loaded
    if df_all.isEmpty():
        print("No data found in the specified date range.")
    else:
        print(f"Loaded {df_all.count()} rows of data.")

        # Save the combined DataFrame to a single CSV file
        df_all.write.option("header", "true").mode("overwrite").csv(output_path)
        # df_all.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
        print("Successfully saved the combined data.")

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/event/gdeltevent_2_cutfile/", header=True)
# display(df)

# COMMAND ----------

# Filter Chinese media reports about the United States
from pyspark.sql import functions as F

output_path = "dbfs:/mnt/results/yuqi_test/event/gdeltevent_3_filteredchnusa/"

# Assume df has already been defined
# Use filter to subset the data
filtered = df.filter((F.col('Actor_1_Country_CODE_7') == 'CHN') & (F.col('Actor2_Country_CODE_17') == 'USA'))

# Save the filtered DataFrame as a CSV file
filtered.write.option("header", "true").csv(output_path, mode="overwrite")

# COMMAND ----------

dffiltered = spark.read.csv("dbfs:/mnt/results/yuqi_test/event/gdeltevent_3_filteredchnusa/", header=True)
# display(dffiltered)

# COMMAND ----------

# Filter U.S. reports on China
from pyspark.sql import functions as F

output_path = "dbfs:/mnt/results/yuqi_test/event/gdeltevent_4_filteredusachn/"

# Assume df has already been defined
# Use filter to subset the data
filtered = df.filter((F.col('Actor_1_Country_CODE_7') == 'USA') & (F.col('Actor2_Country_CODE_17') == 'CHN'))

# Save the filtered DataFrame as a CSV file
filtered.write.option("header", "true").csv(output_path, mode="overwrite")

# COMMAND ----------

dffiltered = spark.read.csv("dbfs:/mnt/results/yuqi_test/event/gdeltevent_4_filteredusachn/", header=True)
# display(dffiltered)

# COMMAND ----------

from pyspark.sql import functions as F

output_path_final = "dbfs:/mnt/results/yuqi_test/event/gdeltevent_5_goldsteinscale/"

# Load the first CSV file
df1 = spark.read.csv("dbfs:/mnt/results/yuqi_test/event/gdeltevent_3_filteredchnusa/", header=True, inferSchema=True)
# Load the second CSV file
df2 = spark.read.csv("dbfs:/mnt/results/yuqi_test/event/gdeltevent_4_filteredusachn/", header=True, inferSchema=True)

# Merge two DataFrames
df_combined = df1.union(df2)

# Calculate the daily average of Goldstein_Scale_30
result = df_combined.groupBy("Day").agg(F.avg("Goldstein_Scale_30").alias("average_goldstein_scale"))

# Display the results
result.show()

# Save the merged results
# result.write.option("header", "true").csv(output_path_final, mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ad-hoc

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/event/gdeltevent_5_goldsteinscale/", header=True, inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import min, max

# Calculate the minimum and maximum date from the DataFrame
min_max_date = df.select(min("Day").alias("min_date"), max("Day").alias("max_date"))

# Display the result
display(min_max_date)

# COMMAND ----------

# Filter the DataFrame for the record with Day 19200101
record_for_day = df.filter(df.Day == "20240101")

# Display the filtered record
display(record_for_day)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

# Count the number of unique dates in the DataFrame
unique_dates_count = df.select(countDistinct("Day").alias("unique_dates_count"))

# Display the result
display(unique_dates_count)

# COMMAND ----------

