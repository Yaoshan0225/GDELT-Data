# Databricks notebook source
from pyspark.sql.functions import col
import os
from datetime import datetime, timedelta

# Define the base path
base_path = "/dbfs/mnt/gdeltgkg_csv_combined"

output_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_2_cutfile/"

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
            if file.endswith(".gkg.csv"):
                # Convert local path to DBFS path
                dbfs_path = root.replace("/dbfs", "dbfs:") + "/" + file
                all_files.append(dbfs_path)

# Check if any files were found
if not all_files:
    print("No files found matching the pattern.")
else:
    df_all = spark.read.csv(all_files, sep="\t", header=False).toDF(
        "GKGRECORDID", "V2.1DATE", "V2SOURCECOLLECTIONIDENTIFIER", "V2SOURCECOMMONNAME", 
        "V2DOCUMENTIDENTIFIER", "V1COUNTS", "V2.1COUNTS", "V1THEMES", "V2ENHANCEDTHEMES", 
        "V1LOCATIONS", "V2ENHANCEDLOCATIONS", "V1PERSONS", "V2ENHANCEDPERSONS", 
        "V1ORGANIZATIONS", "V2ENHANCEDORGANIZATIONS", "V1.5TONE", "V2.1ENHANCEDDATES", 
        "V2GCAM", "V2.1SHARINGIMAGE", "V2.1RELATEDIMAGES", "V2.1SOCIALIMAGEEMBEDS", 
        "V2.1SOCIALVIDEOEMBEDS", "V2.1QUOTATIONS", "V2.1ALLNAMES", "V2.1AMOUNTS", 
        "V2.1TRANSLATIONINFO", "V2EXTRASXML"
    )

    # Check if data was loaded
    if df_all.isEmpty():
        print("No files found matching the pattern.")
    else:
        print(f"Loaded {df_all.count()} rows of data.")

        # Save the combined DataFrame to a single CSV file
        df_all.write.option("header", "true").csv(output_path, mode="overwrite")

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_2_cutfile/", header=True)
# display(df)

# COMMAND ----------

# Filter by topic and tone
from pyspark.sql import functions as F

# Load the CSV file
file_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_2_cutfile/"  # Update this path

try:
    # Read the CSV file into a DataFrame
    df = spark.read.csv(file_path, header=True)

    # Check if 'V1THEMES' column exists in the DataFrame
    if 'V1THEMES' in df.columns:
        # Filter the DataFrame where 'V1THEMES' contains 'ENV'
        filtered_df = df.filter(F.col('V1THEMES').contains('ENV'))

        # Select the desired columns, ensuring to escape the column name with special characters
        result_df = filtered_df.select('GKGRECORDID', F.col('`V1.5TONE`'), 'V1THEMES')

        # Specify the path where you want to save the filtered DataFrame for review
        output_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_3_filtered_ENV/"
        # display(result_df)
        
        result_df.write.option("header", "true").csv(output_path, mode="overwrite")
        print(f"Filtered DataFrame saved to: {output_path}")
    else:
        print("The column 'V1THEMES' does not exist in the CSV file. Please check the column names.")
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Function to filter the dataset
def filter_dataset(file_path):
    # Load the dataset
    data = spark.read.csv(file_path, header=True)

    # Retain only the first group of data from 'V1.5TONE'
    split_col = F.split(F.col('`V1.5TONE`'), ',')[0]
    data = data.withColumn('V1_5TONE', split_col)

    # Filter 'V1THEMES' to include rows with themes containing "ENV"
    data_filtered = data.filter(F.col('V1THEMES').contains("ENV"))

    # Define a UDF to filter 'V1THEMES' column to keep only themes containing "ENV"
    def filter_themes(themes):
        if themes:
            return ';'.join([theme for theme in themes.split(';') if "ENV" in theme])
        return None

    filter_themes_udf = F.udf(filter_themes, StringType())

    # Apply the UDF to 'V1THEMES'
    data_filtered = data_filtered.withColumn('V1THEMES', filter_themes_udf(F.col('V1THEMES')))

    # Specify the path to save the filtered data
    filtered_file_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_3_filtered_ENVthemesAvgtone/"
    data_filtered.write.csv(filtered_file_path, header=True, mode='overwrite')

    return filtered_file_path

# Example usage
file_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_3_filtered_ENV/"  # Update this to your dataset's actual path
filtered_file_path = filter_dataset(file_path)
print(f"Filtered data saved to: {filtered_file_path}")


# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_3_filtered_ENVthemesAvgtone/", header=True)
# display(df)

# COMMAND ----------


def process_data(input_file, output_file):
    # Load the dataset
    data = spark.read.csv(input_file, header=True)

    # Extract the first eight characters of 'GKGRECORDID' as the date
    data = data.withColumn('Date', F.col('GKGRECORDID').substr(1, 8))

    # Convert 'V1.5TONE' to numeric, handling non-numeric values by setting them to null
    # data = data.withColumn('V1_5TONE', F.col('V1_5TONE').cast('float'))

    # Group by 'Date' and calculate the mean of 'V1.5TONE' and count of reports
    aggregated_data = data.groupBy('Date').agg(
        F.avg('V1_5TONE').alias('AverageTone'),
        F.count('V1_5TONE').alias('ReportCount')
    )

    # Save the results to the specified output file path
    aggregated_data.write.csv(output_file, header=True, mode='overwrite')
    print("the above shit file is saved onto:" + output_file)


# COMMAND ----------

# Example usage
input_file = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_3_filtered_ENVthemesAvgtone/"
output_file = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_4_countreportsandavgtone/"
process_data(input_file, output_file)

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_4_countreportsandavgtone/", header=True)
# display(df)

# COMMAND ----------

# Count the total number of reports for each day
from pyspark.sql import functions as F

def count_reports_by_day(input_file_path, output_file_path):
    # Load the dataset
    data = spark.read.csv(input_file_path, header=True)

    # Extract the first eight characters of 'GKGRECORDID' to use as the date
    data = data.withColumn('Date', F.col('GKGRECORDID').substr(1, 8))

    # Count the number of records for each unique date
    date_counts = data.groupBy('Date').count().withColumnRenamed('count', 'ReportCount')

    # Save the results to the specified output file path
    date_counts.write.csv(output_file_path, header=True, mode='overwrite')

# Example usage
input_file_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_2_cutfile/"
output_file_path = "dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_5_wholecountreports/"
count_reports_by_day(input_file_path, output_file_path)

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/results/yuqi_test/gkg/gdeltgkg_5_wholecountreports/", header=True)
# display(df)

# COMMAND ----------

