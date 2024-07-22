# Databricks notebook source
# MAGIC %md
# MAGIC Setup some Logger formatting and information

# COMMAND ----------


import logging

# Create a Catalog for our Schemas
catalog_eval = "evaluation"
# Create a Schema for our Tables
schema_mh = "mentalhealth"
# Create a Volume in Schema to hold our data 
volume_data = "evaluation_data"

# Define the root path for our tables, make a variable for ease of configuration
root_file_path = f"{catalog_eval}/{schema_mh}"
root_table_path = f"{catalog_eval}.{schema_mh}"

logging.info('Checking to see if logger is working:::')

logger = logging.getLogger("log4j")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s]:::%(name)s:::%(levelname)s:::%(message)s:::')
for handler in logger.handlers:
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)



# COMMAND ----------

# MAGIC %md
# MAGIC Logger Testing - this will show up in the logs

# COMMAND ----------

import logging

logger.info("TEST INFO LOG:::")
logger.warning("TEST WARNING LOG:::")
logger.error("TEST ERROR LOG:::")

# COMMAND ----------

# MAGIC %md
# MAGIC Use the Shell option of databricks is an easy to to quickly access some built in functionality, it could be done in python as a code block buy shell commands are better suited.
# MAGIC
# MAGIC Idealling this would goto Volumes, but its not available in this version of databricks

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip -O /tmp/data.zip

# COMMAND ----------

print(root_table_path)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {root_table_path}")


# COMMAND ----------

print(volume_data)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {root_table_path}.{volume_data}")

# COMMAND ----------

# The DBFS had got into a buggy state -- the Volumns were not mounted correctly

# Unmount all mounts then the DBFS can be unmounted for all mounts
print('Unmounting all mounts beginning with /mnt/')
dbutils.fs.mounts()
for mount in dbutils.fs.mounts():
  if mount.mountPoint.startswith('/mnt/'):
    dbutils.fs.unmount(mount.mountPoint)

# Re-list all mount points
print('Re-listing all mounts')
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC rm -f /Volumes/evaluation/mentalhealth/evaluation_data/mhcld_puf_2021.csv
# MAGIC unzip /tmp/data.zip -d /Volumes/evaluation/mentalhealth/evaluation_data
# MAGIC head /Volumes/evaluation/mentalhealth/evaluation_data/mhcld_puf_2021.csv -n 5
# MAGIC ls /Volumes/evaluation/mentalhealth/evaluation_data/
# MAGIC pwd

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# YEAR,AGE,EDUC,ETHNIC,RACE,GENDER,SPHSERVICE,CMPSERVICE,OPISERVICE,RTCSERVICE,IJSSERVICE,MH1,MH2,MH3,SUB,MARSTAT,SMISED,SAP,EMPLOY,DETNLF,VETERAN,LIVARAG,NUMMHS,TRAUSTREFLG,ANXIETYFLG,ADHDFLG,CONDUCTFLG,DELIRDEMFLG,BIPOLARFLG,DEPRESSFLG,ODDFLG,PDDFLG,PERSONFLG,SCHIZOFLG,ALCSUBFLG,OTHERDISFLG,STATEFIP,DIVISION,REGION,CASEID

schema_dataset = StructType(
    [
        StructField("YEAR",IntegerType(),False),
        StructField("AGE",IntegerType(),False),
        StructField("EDUC",IntegerType(),False),
        StructField("ETHNIC",IntegerType(),False),
        StructField("RACE",IntegerType(),False),
        StructField("GENDER",IntegerType(),False),
        StructField("SPHSERVICE",IntegerType(),False),
        StructField("CMPSERVICE",IntegerType(),False),
        StructField("OPISERVICE",IntegerType(),False),
        StructField("RTCSERVICE",IntegerType(),False),
        StructField("IJSSERVICE",IntegerType(),False),
        StructField("MH1",IntegerType(),False),
        StructField("MH2",IntegerType(),False),
        StructField("MH3",IntegerType(),False),
        StructField("SUB",IntegerType(),False),
        StructField("MARSTAT",IntegerType(),False),
        StructField("SMISED",IntegerType(),False),
        StructField("SAP",IntegerType(),False),
        StructField("EMPLOY",IntegerType(),False),
        StructField("DETNLF",IntegerType(),False),
        StructField("VETERAN",IntegerType(),False),
        StructField("LIVARAG",IntegerType(),False),
        StructField("NUMMHS",IntegerType(),False),
        StructField("TRAUSTREFLG",IntegerType(),False),
        StructField("ANXIETYFLG",IntegerType(),False),
        StructField("ADHDFLG",IntegerType(),False),
        StructField("CONDUCTFLG",IntegerType(),False),
        StructField("DELIRDEMFLG",IntegerType(),False),
        StructField("BIPOLARFLG",IntegerType(),False),
        StructField("DEPRESSFLG",IntegerType(),False),
        StructField("ODDFLG",IntegerType(),False),
        StructField("PDDFLG",IntegerType(),False),
        StructField("PERSONFLG",IntegerType(),False),
        StructField("SCHIZOFLG",IntegerType(),False),
        StructField("ALCSUBFLG",IntegerType(),False),
        StructField("OTHERDISFLG",IntegerType(),False),
        StructField("STATEFIP",IntegerType(),False),
        StructField("DIVISION",IntegerType(),False),
        StructField("REGION", IntegerType(),False),
        StructField("CASEID",DoubleType(),False),
]
)

# COMMAND ----------

# Drop the table if it exist, otherwise the write will fail with a "table already exists" error
spark.sql(f"DROP TABLE {root_table_path}.mhcld_bronze")


# COMMAND ----------

import os

# Retrieve the path of the temporary directory, which corresponds to the /tmp directory within your workspace, not the systems environment
file_path = f"/Volumes/{root_file_path}/{volume_data}/mhcld_puf_2021.csv"
print(f"File Path: {file_path}")

# Read the dataset into the session storage. In Databricks notebooks, the SparkSession is automatically instantiated and stored in the variable `spark`
df_raw = spark.read.csv(file_path, 
                        header=True, 
                        inferSchema=False, 
                        schema=schema_dataset, 
                        multiLine="false", 
                        escape='"')

display(df_raw)

# Create Bronze Layer table with schema of raw data and partitioning by YEAR and STATE
df_raw.write.format("delta").mode("overwrite").partitionBy("YEAR","STATEFIP").saveAsTable(f"{root_table_path}.mhcld_bronze")

# display(df_raw.summary())
# No null values

# -9 is used to represent missing data


# COMMAND ----------

# Validates the schema of the Bronze table
df_raw.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Fetch data out of Bronze table

# COMMAND ----------


df_bronze = spark.sql(f"SELECT * FROM {root_table_path}.mhcld_bronze")

# COMMAND ----------


from collections import Counter

# Validates 2 lists to make sure they have the same content
def validate_lists(list1, list2)->bool:
    expected_counter = Counter(list1)
    received_counter = Counter(list2)
    if expected_counter == received_counter:
        print("SUCCESS, COLUMNS MATCH")
        return True
    else:
        print("ERROR, COLUMNS ARE DISCONCORDANT")
        return False


# COMMAND ----------

# Get a list of columns names from original raw csv file
expected_columns = []
with open(file_path,"r") as fs:
    header = fs.readline().strip()
    expected_columns = header.split(",")
    print("Number of Columns::", len(expected_columns))
    print(expected_columns)


# COMMAND ----------

# Get a list of columns names from Bronze table
dataset_columns = list(df_bronze.columns)
print("Number of Dataset Columns::", len(dataset_columns))
print(dataset_columns)
try:
    valid = validate_lists(dataset_columns, expected_columns)
except Exception as e:  
    logger.warning("Bronze Table Column Validate Headers:::")
    logger.error(f"Column Validation Failed on Bronze Table:::{e}")
    raise ValueError("Bronze Data Column Mismatch:::")  

print(f"Number of Rows in dataset {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Preform data wrangling in preparation for Silver Layer table creation

# COMMAND ----------

# CODIFY - (GENDER, RACE, ETHNICITY, MARITAL, EMPLOY (DETNLF is related to employ), INCOME (Not in Dataset))

categorical_conversion = [
        {"COLUMN":"GENDER","VALUE":1, "LABEL":"Male"},
        {"COLUMN":"GENDER","VALUE":2, "LABEL":"Female"},
        {"COLUMN":"GENDER","VALUE":-9, "LABEL":"Missing/unknown/not collected/invalid"},

        {"COLUMN":"RACE","VALUE":1, "LABEL":"American Indian/Alaska Native"},
        {"COLUMN":"RACE","VALUE":2, "LABEL":"Asian"},
        {"COLUMN":"RACE","VALUE":3, "LABEL":"Black or African American"},
        {"COLUMN":"RACE","VALUE":4, "LABEL":"Native Hawaiian or Other Pacific Islander"},
        {"COLUMN":"RACE","VALUE":5, "LABEL":"White"},
        {"COLUMN":"RACE","VALUE":6, "LABEL":"Some other race alone/two or more races"},
        {"COLUMN":"RACE","VALUE":-9, "LABEL":"Missing/unknown/not collected/invalid"},

        {"COLUMN":"ETHNIC","VALUE":1, "LABEL": "Mexican"},
        {"COLUMN":"ETHNIC","VALUE":2, "LABEL": "Puerto Rican"},
        {"COLUMN":"ETHNIC","VALUE":3, "LABEL": "Other Hispanic or Latino origin"},
        {"COLUMN":"ETHNIC","VALUE":4, "LABEL": "Not of Hispanic or Latino origin"},
        {"COLUMN":"ETHNIC","VALUE":-9, "LABEL": "Missing/unknown/not collected/invalid"},

         {"COLUMN":"MARSTAT","VALUE":1, "LABEL": "Never married"},
         {"COLUMN":"MARSTAT","VALUE":2, "LABEL": "Now married"},
         {"COLUMN":"MARSTAT","VALUE":3, "LABEL": "Separated"},
         {"COLUMN":"MARSTAT","VALUE":4, "LABEL": "Divorced, widowed"},
         {"COLUMN":"MARSTAT","VALUE":-9, "LABEL": "Missing/unknown/not collected/invalid "},

         {"COLUMN":"EMPLOY","VALUE":1, "LABEL": "Full-time"},
         {"COLUMN":"EMPLOY","VALUE":2, "LABEL": "Part-time"},
         {"COLUMN":"EMPLOY","VALUE":3, "LABEL": "Employed full-time/part-time not differentiated"},
         {"COLUMN":"EMPLOY","VALUE":4, "LABEL": "Unemployed"},
         {"COLUMN":"EMPLOY","VALUE":5, "LABEL": "Not in labor force"},
         {"COLUMN":"EMPLOY","VALUE":-9, "LABEL": "Missing/unknown/not collected/invalid "},

         {"COLUMN":"DETNLF","VALUE":1, "LABEL": "Retired, disabled"},
         {"COLUMN":"DETNLF","VALUE":2, "LABEL": "Student"},
         {"COLUMN":"DETNLF","VALUE":3, "LABEL": "Homemaker"},
         {"COLUMN":"DETNLF","VALUE":4, "LABEL": "Sheltered/non-competitive employment "},
         {"COLUMN":"DETNLF","VALUE":5, "LABEL": "Other"},
         {"COLUMN":"DETNLF","VALUE":-9, "LABEL": "Missing/unknown/not collected/invalid "},
    ]

# Create dataframe to store the codified columns
df_codify = spark.createDataFrame(categorical_conversion)
display(df_codify)

# # Create a table to store the codified columns
# df_codify.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.mhcld_codify")

logger.info(f"created df_codify:::{df_codify.count()}")

# COMMAND ----------

from pyspark.sql.functions import col

df_codify_columns = df_codify.select(col("COLUMN")).distinct()
ls_codify_columns = df_codify_columns.rdd.map(lambda row: row[0]).collect()
print(ls_codify_columns)
logger.info(f"created codify_columns:::")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write the JOIN in SQL format to understand how it works
# MAGIC -- SELECT s.*, label as EMPOLY_LABEL from df_raw_copy as s
# MAGIC -- LEFT JOIN df_codify ON df_codify.VALUE = s.EMPLOY and df_codify.COLUMN = 'EMPLOY'

# COMMAND ----------


df_codes = df_bronze
updated_expected_columns = expected_columns.copy()
for c in ls_codify_columns:
    df_codes = df_codes.alias("s")
    df_codes = df_codes.join(df_codify,
                               (df_codify["COLUMN"] == c) &
                               (df_codify['VALUE'] == df_codes[c]),"left").select("s.*",col("LABEL").alias(f"{c}_LABEL"))
    updated_expected_columns.append(f"{c}_LABEL")
    # Compute frequency of value "c" in column
    frequency_value_c = df_codes.select(col(c), col(f"{c}_LABEL")).groupBy(c, f"{c}_LABEL").count()
    display(frequency_value_c)

display(df_codes)
new_column_list = list(df_codes.columns)
valid_new_list = validate_lists(new_column_list, updated_expected_columns)

# COMMAND ----------

from pyspark.sql.types import FloatType
float_types = ["MH1","MH2","MH3"]

# Convert the numeric variables (MENHLTH, PHYHLTH, POORHLTH) to float data types.
# PHYHLTH, POORHLTH do not exist in the dataset
df_cast = df_codes
for f in float_types:
    df_cast = df_cast.withColumn(f, df_cast[f].cast(FloatType()))
display(df_cast)
logger.info(f"created df_cast:::{df_cast}")

# COMMAND ----------

# Validates the casting of the numeric variables
df_cast.printSchema()

# COMMAND ----------

# Displays summary statistics for the casted dataframe
display(df_cast.summary())

# COMMAND ----------

from pyspark.sql.functions import col

# min/max scalling for specific columns
def normalize_with_mapping(val_list, new_min, new_max):
    """
    Normalize a list of values to a specified range [new_min, new_max]
    and return a dictionary mapping old values to new normalized values.
    
    :param values: List of values to normalize.
    :param new_min: Minimum value of the new range.
    :param new_max: Maximum value of the new range.
    :return: Dictionary mapping old values to normalized values.
    """
    old_min = min(val_list)
    old_max = max(val_list)
    
    # Handle the case where all values are the same
    if old_min == old_max:
        return {x: new_min for x in val_list}
    
    # Create a mapping of old values to normalized values
    mapping = {
        x: new_min + (x - old_min) * (new_max - new_min) / (old_max - old_min)
        for x in val_list
    }
    return mapping

try:
    # Sample data 
    min_max_columns = ['MH1','MH2','Mh3']
    df_norm = df_cast
    for mm in min_max_columns: 
        df_filter = df_norm.select(col(mm)).distinct().filter(df_norm[mm] >= 0) 
        # -9 is not a valid for this normalization option so we are going to ignore that
        col_vals = df_filter.select(col(mm)).collect()
        col_list = [row[mm] for row in col_vals]
        nums = normalize_with_mapping(col_list, 0, 1)
        mm_map = []
        for k in nums:
            mm_map.append({"key":k, "value":nums[k]})
        df_min_max = spark.createDataFrame(mm_map)
        df_norm = df_norm.alias("n")
        df_norm = df_norm.join(df_min_max,(df_norm[mm] == df_min_max['key']), 'left').select("n.*", col("value").alias(f"{mm}_MM"))
except Exception as e:
    logger.error(f"normalize_with_mapping failed with error: {e}")
    raise ValueError(f"normalize_with_mapping failed with error: {e}")

display(df_norm)

# COMMAND ----------

def normalize_with_z_score(val_list):
    """
    Normalize a list of values using z-score normalization (mean of 0 and standard deviation of 1)
    and return a dictionary mapping old values to new normalized values.
    
    :param val_list: List of values to normalize.
    :return: Dictionary mapping old values to normalized values.
    """
    mean = sum(val_list) / len(val_list)
    variance = sum((x - mean) ** 2 for x in val_list) / len(val_list)
    std_dev = variance ** 0.5
    
    # Handle the case where all values are the same
    if std_dev == 0:
        return {x: 0 for x in val_list}
    
    # Create a mapping of old values to z-score normalized values
    mapping = {
        x: (x - mean) / std_dev
        for x in val_list
    }
    
    return mapping

try:
    zscore_columns = ['MH1','MH2','MH3']
    df_z1 = df_norm
    for mm in min_max_columns: 
        df_filter = df_z1.select(col(mm)).distinct().filter(df_cast[mm] >= 0) # -9 is not a valid for this normalization option so we are going to ignore that
        col_vals = df_filter.select(col(mm)).collect()
        col_list = [row[mm] for row in col_vals]
        nums = normalize_with_z_score(col_list)
        mm_map = []
        for k in nums:
            mm_map.append({"key":k, "value":nums[k]})
        df_min_max = spark.createDataFrame(mm_map)
        df_z1 = df_z1.alias("n")
        df_z1 = df_z1.join(df_min_max,(df_z1[mm] == df_min_max['key']), 'left').select("n.*", col("value").alias(f"{mm}_ZS"))
except Exception as e:
    logger.error(f"normalize_with_z_score failed with error: {e}")
    raise ValueError(f"normalize_with_z_score failed with error: {e}")

display(df_z1)

# COMMAND ----------

from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StringType
## Create a stratify column with the columns concatenated to create a single column to stratify the data by
df_stratify = df_z1.withColumn("stratify",
                               concat_ws("-",col("GENDER"),col("RACE"),col("ETHNIC"),col("MARSTAT"),col("EMPLOY")).cast(StringType()))
display(df_stratify)
# Write the table, partition by stratify so its faster on the subsequent queries
df_stratify.write.format("delta").mode("overwrite").partitionBy("stratify").saveAsTable(f"{root_table_path}.stratified")

# COMMAND ----------


# Get the distrinct statify codes
distinct_values = [row['stratify'] for row in spark.sql(f"select distinct stratify from {root_table_path}.stratified").collect()]
print(f"Number of Distinct Stratifications {len(distinct_values)}")


# COMMAND ----------

from pyspark.sql.functions import col, lit
# Training Ratio (Test Ratio, is assumbed 1-train_ratio)
train_ratio = 0.8


# If we requery the dataset here, it takes advantage of the partitioning
df_strat = spark.sql(f"select * from {root_table_path}.stratified")
category_distribution = df_strat.groupBy(col('stratify')).count()


# Create a fractions dictionary for the sampleBy
fractions = {(row['stratify']): train_ratio
                                       for row in category_distribution.collect() }
# # Sample indices based on category distributions
df_train_and_validate = df_strat.stat.sampleBy(
                                'stratify',
                                fractions=fractions,
                                seed=42)

# Create a fractions dictionary for the sampleBy
fractions_validate = {(row['stratify']): train_ratio
                                       for row in category_distribution.collect() } 
# Resample the df_train_and_validate to get the validation set
df_train = df_train_and_validate.stat.sampleBy(
                                'stratify',
                                fractions=fractions_validate,
                                seed=42)

# Subtract the df_train from the train and validate so we have the validate df
df_train_cid = df_train.select("CASEID")
df_validate =  df_train_and_validate.join(df_train_cid, on="CASEID", how="left_anti")   

print(f"Number of Train Records {df_train.count()}")
print(f"Number of Validate Records {df_validate.count()}")
# Subtract the train and validate from the df_strat, so we have our test dataset
print(df_train_and_validate.count())
df_train_and_validate_cid = df_train_and_validate.select("CASEID")
df_test = df_strat.join(df_train_and_validate_cid, on="CASEID", how="left_anti")
print(f"Number of Test Records {df_test.count()}")


# COMMAND ----------

# Add the Split Group Column to stratify
df_train = df_train.withColumn("split", lit("train"))
df_validate = df_validate.withColumn("split", lit("validate"))
df_test =  df_test.withColumn("split", lit("test"))

df_silver = df_train.union(df_validate)
df_silver = df_silver.union(df_test)


df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.mhcld_silver")


# COMMAND ----------

df_train.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.train")
df_validate.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.validate")
df_test.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.test")

# COMMAND ----------

display(df_train)
display(df_validate)
display(df_test)

# COMMAND ----------

total_count = df_strat.count()

# Calculate count for each distinct value
category_counts = df_strat.groupBy('GENDER').count()

# Calculate percentage for each value
category_counts_with_percentage = category_counts.withColumn('percentage', col('count') * 100 / total_count)

# Show the result
category_counts_with_percentage.show()

# COMMAND ----------

total_count = df_train.count()

# Calculate count for each distinct value
category_counts = df_train.groupBy('GENDER').count()

# Calculate percentage for each value
category_counts_with_percentage = category_counts.withColumn('percentage', col('count') * 100 / total_count)

# Show the result
category_counts_with_percentage.show()

# COMMAND ----------

total_count = df_test.count()

# Calculate count for each distinct value
category_counts = df_test.groupBy('GENDER').count()

# Calculate percentage for each value
category_counts_with_percentage = category_counts.withColumn('percentage', col('count') * 100 / total_count)

# Show the result
category_counts_with_percentage.show()

# COMMAND ----------

df_gold = spark.sql(f"SELECT * FROM {root_table_path}.mhcld_silver")
df_gold.write.format("delta").mode("overwrite").partitionBy("YEAR","REGION").saveAsTable(f"{root_table_path}.mhcld_gold")
