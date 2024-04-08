from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime

spark = SparkSession.builder \
    .appName("JSON Analysis DataFrame") \
    .getOrCreate()

# question1-Read JSON file with dynamic schema
df = spark.read.option("multiline", "true").json("data.json")

# question2-Flatten the DataFrame
flat_df = df.selectExpr("id", "name", "explode(options) as options", "load_date")

# question3-Find record count when flattened and not flattened
record_count_flattened = flat_df.count()
record_count_original = df.count()

record_count_difference = record_count_flattened - record_count_original

# question4-Differentiate using explode, explode_outer, posexplode functions
exploded_df = flat_df.withColumn("options", col("options").getItem("option").alias("options"))
exploded_outer_df = flat_df.withColumn("options", col("options").getItem("option").alias("options"))
pos_exploded_df = flat_df.withColumn("options", col("options").getItem("option").alias("options"))

# question5-Filter the id equal to 0001
filtered_df = flat_df.filter(col("id") == "0001")

# question6-Convert column names from camel case to snake case
snake_case_df = flat_df.toDF(*(col_name.lower() for col_name in flat_df.columns))

# question7-Add a new column named load_date with the current date
load_date_df = flat_df.withColumn("load_date", to_date(col("load_date"), "yyyy-MM-dd"))

# question8-Create year, month, day columns from load_date
date_columns_df = load_date_df.withColumn("year", year(col("load_date"))) \
                              .withColumn("month", month(col("load_date"))) \
                              .withColumn("day", dayofmonth(col("load_date")))

# question9-Write DataFrame to a table with partition
date_columns_df.write.partitionBy("year", "month", "day") \
                      .mode("overwrite") \
                      .format("json") \
                      .saveAsTable("employee.employee_details")


spark.stop()
