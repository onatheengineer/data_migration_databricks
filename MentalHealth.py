# Databricks notebook source
# MAGIC %md
# MAGIC Setup some Logger formatting and information

# COMMAND ----------

# MAGIC %md
# MAGIC Documentation
# MAGIC https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-info/MH-CLD-2021-DS0001-info-codebook.pdf
# MAGIC
# MAGIC
# MAGIC Dataset
# MAGIC https://www.google.com/url?sa=D&q=https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip&ust=1721825520000000&usg=AOvVaw0IPzUCKetW6vLY7z8GzTT_&hl=en&source=gmail

# COMMAND ----------

# MAGIC %md
# MAGIC Initial configuration and logging settings..
# MAGIC
# MAGIC Logging here would have more locations if needed depending on the reporting needs and analysis tools. 
# MAGIC Ideally it is connected to a logging service like datadog or splunk that allows for indept log dashboard creation
# MAGIC and reporting/alerting metrics

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
# MAGIC Later as we process the file it will target to a Volume.

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip -O /tmp/data.zip

# COMMAND ----------

# MAGIC %md
# MAGIC Create the root table path in the catalogue if it doesn't exist.

# COMMAND ----------

print(root_table_path)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {root_table_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC Create the main volume to be used to hold our files under our root catalogue

# COMMAND ----------

print(volume_data)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {root_table_path}.{volume_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC When creating the volume, our compute lost the mount temporarily. By unmounting all mountpoints, the system refreshed the mounts and corrected the issue allowing us to continue forward with the Volume usage.

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

# MAGIC %md
# MAGIC Here we unzip and store the CSV file for the data, it is unzipped directly onto the volume and then we use the terminal "head" command to preview the beginning of the file, looking mainly for the header and a small sampling of the data. This ensures our file is intact and how we might ingest it later on into pyspark.

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC rm -f /Volumes/evaluation/mentalhealth/evaluation_data/mhcld_puf_2021.csv
# MAGIC unzip /tmp/data.zip -d /Volumes/evaluation/mentalhealth/evaluation_data
# MAGIC head /Volumes/evaluation/mentalhealth/evaluation_data/mhcld_puf_2021.csv -n 5
# MAGIC ls /Volumes/evaluation/mentalhealth/evaluation_data/
# MAGIC pwd

# COMMAND ----------

# MAGIC %md
# MAGIC To ensure our data has the correct schema, we build our discrete pyspark schema structure. When creating the Dataframe the schema is applied.
# MAGIC
# MAGIC we could also have defaulted everything to string and then cast later on if we needed to data wrangle in different ways

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

# MAGIC %md
# MAGIC Drop our bronze table if it exists

# COMMAND ----------

# Drop the table if it exist, otherwise the write will fail with a "table already exists" error
spark.sql(f"DROP TABLE {root_table_path}.mhcld_bronze")


# COMMAND ----------

# MAGIC %md
# MAGIC Convert our CSV file in our volume to our pyspark dataframe, we don't infer the schema because we have defined it discretely. 
# MAGIC
# MAGIC We write the bronze table here as part of the Medallion structure as the raw untouched data.

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

# MAGIC %md
# MAGIC Refetch the bronze data for further processing

# COMMAND ----------


df_bronze = spark.sql(f"SELECT * FROM {root_table_path}.mhcld_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Define our function that is used to compare 2 lists for content matching, this doesn't not validate order of the lists but the content of the 2 lists. Used to validate our column structure

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
        # return Falsen - see bug note below on this fix
        raise ValueError("Columns Do Not Match")


# COMMAND ----------

# MAGIC %md
# MAGIC We process the head of the CSV separately as the CSV is the ultimate source of truth for the data provided. We will use this to compare the header in the CSV file with the list of headers from our bronze table to confirm integrity.

# COMMAND ----------

# Get a list of columns names from original raw csv file
expected_columns = []
with open(file_path,"r") as fs:
    header = fs.readline().strip()
    expected_columns = header.split(",")
    print("Number of Columns::", len(expected_columns))
    print(expected_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC Compare the csv header list with the bronze table column list to ensure, raising the error  where appropriatly .
# MAGIC
# MAGIC there is a bug here, in our validate_lists function we should be raising a validation error rather than returning False

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

# MAGIC %md
# MAGIC Categorical codex processing, we generate a looking dataframe that we built from the provided documentation. This will be used to join to the bronze data to translate the code values to the correct labeled permissible value.
# MAGIC
# MAGIC This is the most efficient method due to the fact we leverage the native pyspark join functionality and can be used across large datasets.

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

# MAGIC %md
# MAGIC We get a distinct column list that will then be used to process our joins later on

# COMMAND ----------

from pyspark.sql.functions import col

df_codify_columns = df_codify.select(col("COLUMN")).distinct()
ls_codify_columns = df_codify_columns.rdd.map(lambda row: row[0]).collect()
print(ls_codify_columns)
logger.info(f"created codify_columns:::")

# COMMAND ----------

# MAGIC %md
# MAGIC This was simply used for some data exploration during development

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write the JOIN in SQL format to understand how it works
# MAGIC -- SELECT s.*, label as EMPOLY_LABEL from df_raw_copy as s
# MAGIC -- LEFT JOIN df_codify ON df_codify.VALUE = s.EMPLOY and df_codify.COLUMN = 'EMPLOY'

# COMMAND ----------

# MAGIC %md
# MAGIC Using the codex table created earlier we process each column to be translated, joining it to the bronze data with the translated permissible value placed into a new column called COLUMN_LABEL.
# MAGIC
# MAGIC We also utilize our validate_list functinality to ensure the column list is updated as expected. 
# MAGIC
# MAGIC During the processing we output hte frequency table that easily allows us to see any misjoins or mistakes using our groupby,count method
# MAGIC
# MAGIC If time was permitted at this point we could write a validate function as well which would looking for misjoins by looking for codes or values that occur with multiple codes/labels.

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

# MAGIC %md
# MAGIC Casting values from one type to we utilize the withColumn and the cast(DataType) method.
# MAGIC
# MAGIC In hind site, it probably would have been better practice to generate a new column and then compare values prior replacing the old set of data.

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

# MAGIC %md
# MAGIC Normalizing data with a new min-max value.   
# MAGIC
# MAGIC using our normalize_with_mapping function we generate a mapping between old values and new values.
# MAGIC
# MAGIC The new values are based on a new minimum and new maximum that can be defined at the function call. 
# MAGIC
# MAGIC The function will assign a new value based on the scale between the new minimum and new maximum.
# MAGIC
# MAGIC for example if there are 4 values and our new min/max is 0 - 1.. each value will be represented by a 0.33 increment (0 and 1 are included)
# MAGIC
# MAGIC example
# MAGIC
# MAGIC values {2, 5, 10, 23}
# MAGIC new values {2:0, 5:0.33 10:0.66, 23: 1}
# MAGIC
# MAGIC We use this value mapping and the pyspark join functionality to normalize the old values ot the new values. New columns are created for this normalized MM data

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
    min_max_columns = ['MH1','MH2','MH3']
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

# MAGIC %md
# MAGIC Normalizing with the Z-score is similar in that we first will generate our mapping from the val_list and calculate the mean and standard deviation of the value set.
# MAGIC
# MAGIC the closer to the mean, the closer to 0 the new value will be. Similar to the minmax, we utilize the mapping and join functionality to create new columns for our Z-score normalized data.
# MAGIC
# MAGIC ### This was fixed to go from the minmax instead of the categorical code data

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
    zscore_columns = ['MH1_MM','MH2_MM','MH3_MM']
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

# MAGIC %md
# MAGIC Stratifying the data new into test and train sets is a key part of AIML work. Here we split the data attempting to maintain our currently distribution from the overall dataset into our Train, Test, Validate sets.
# MAGIC
# MAGIC TO do this we create a stratify looking column that is a concatenation of the demographic columns we are interested in mainting.
# MAGIC
# MAGIC This concatenated value will act as a "key" that we can then use to stratify the data.  
# MAGIC
# MAGIC Because of the size and complexity, we write this keyed data to a table that is partitioned by the key, this allows us for quick lookup based on the stratified key later in the process.
# MAGIC
# MAGIC Note: Could have possibly used  itertools.combinations with UDF

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

# MAGIC %md
# MAGIC Get all distict stratify key values, this is our working list for processing through each distinct stratification set.

# COMMAND ----------


# Get the distrinct statify codes
distinct_values = [row['stratify'] for row in spark.sql(f"select distinct stratify from {root_table_path}.stratified").collect()]
print(f"Number of Distinct Stratifications {len(distinct_values)}")


# COMMAND ----------

# MAGIC %md
# MAGIC  The splitting of hte data is handled by the pyspark.sampleBy function. To use the sampleBy we first generate a fractions dictionary that contains the percentage of each distinct stratification key value.
# MAGIC
# MAGIC  Because we want an even distribution, all of the fractions will be equal to the training ratio desired (0.8). Each stratifcation will than we randomly split out as close to the 0.8 as possible. In some cases this will not be possible, especially in those rare instances where there are a small number of rows assiciated with that particular stratification.
# MAGIC
# MAGIC  as we generate the training dataset we will use a "left_anti" join to remove the training dataset from our overall dataset, this will leave us with our test dataset at a ratio of  1 - train_ratio, or in this case 0.2 (20%).
# MAGIC
# MAGIC  we use the same strategy to segment out a validation set from the training set.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit
# Training Ratio (Test Ratio, is assumbed 1-train_ratio)
train_ratio = 0.8


# If we requery the dataset here, it takes advantage of the partitioning
df_strat = spark.sql(f"select * from {root_table_path}.stratified")

# Get the distict stratify values and their counts
category_distribution = df_strat.groupBy(col('stratify')).count()


# Create a fractions dictionary for the sampleBy
fractions = {(row['stratify']): train_ratio
                                       for row in category_distribution.collect() }
# # Sample indices based on category distributions - can only be on 1 column this is the reason we built the concatenated stratify column
df_train_and_validate = df_strat.stat.sampleBy(
                                'stratify',
                                fractions=fractions,
                                seed=42)

# Create a fractions dictionary for the sampleBy for the validation dataset
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

# MAGIC %md
# MAGIC Final Distributions
# MAGIC
# MAGIC | Set | Totals |Percentage | 
# MAGIC |----------|----------|----------|
# MAGIC | Training  |2982194  | 46%   |
# MAGIC | Validate  |  745516| 11%   | 
# MAGIC | Test  | 2781315 | 43%   |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Adding a column called split with the value designating the Train/Test/Validate dataset each row belongs allows us to analyze the data further within each dataset independently or as a whole.  
# MAGIC
# MAGIC We write this data to our silver table that includes
# MAGIC
# MAGIC 1) The codex translation of certain categorical fields to their Permissible Value Label
# MAGIC 2) MinMax normalization for certain fields that are normalized from 0-1 as the new min max
# MAGIC 3) Z-score normalization of certain fields that are normalized to their mean/std deviation with the mean represented by 0
# MAGIC 4) Each row designated to be part of a Train/Test/Validate dataset
# MAGIC
# MAGIC
# MAGIC Here we could apply several partition strategies depending on what the downstream query needs are. 
# MAGIC
# MAGIC Some parition columns might be "split" column, if we need easy lookup of the split sets
# MAGIC
# MAGIC Year/Region if we are looking to segment out based on year or region for statistics and processing

# COMMAND ----------

# Add the Split Group Column to stratify
df_train = df_train.withColumn("split", lit("train"))
df_validate = df_validate.withColumn("split", lit("validate"))
df_test =  df_test.withColumn("split", lit("test"))

df_silver = df_train.union(df_validate)
df_silver = df_silver.union(df_test)


df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.mhcld_silver")


# COMMAND ----------

# MAGIC %md
# MAGIC For convenience the split datasets are saved out, these are only for convenience with the silver table containing the complete dataset

# COMMAND ----------

df_train.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.train")
df_validate.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.validate")
df_test.write.format("delta").mode("overwrite").saveAsTable(f"{root_table_path}.test")

# COMMAND ----------

display(df_train)
display(df_validate)
display(df_test)

# COMMAND ----------

# MAGIC %md
# MAGIC The next few blocks are looking at the distribution of one of our demographics columns to determine how well our split/stratification worked.
# MAGIC
# MAGIC to summarize, our Gender distribution is (the validate table below was not originally included in teh notebook, but the numbers are provided below)
# MAGIC
# MAGIC ### Gender
# MAGIC | Set | 1 | 2 | -9 |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Overall    | 47.07%   | 52.73%   | 19.3%   |
# MAGIC | Traing    | 45.46%   | 54.36%   | 17.0%   |
# MAGIC | Test    | 49.6%   | 50.1%   | 23%   |
# MAGIC | Validate    | 45.5%   | 54.3%   | 17.2%   |
# MAGIC
# MAGIC

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


from pyspark.sql.functions import col

df_val = spark.sql(f"SELECT * FROM {root_table_path}.validate")
total_count = df_val.count()

# Calculate count for each distinct value
category_counts = df_val.groupBy('GENDER').count()

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

# MAGIC %md
# MAGIC Write out the final gold dataset

# COMMAND ----------

df_gold = spark.sql(f"SELECT * FROM {root_table_path}.mhcld_silver")
df_gold.write.format("delta").mode("overwrite").partitionBy("YEAR","REGION").saveAsTable(f"{root_table_path}.mhcld_gold")
