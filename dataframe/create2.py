from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("Credit Card Analysis DataFrame") \
    .getOrCreate()

# question1-Create DataFrame
credit_card_df = spark.createDataFrame([
    ("1234567891234567",), ("5678912345671234",),
    ("9123456712345678",), ("1234567812341122",),
    ("1234567812341342",)
], ["card_number"])

# question2-Print number of partitions
print("Number of partitions before repartitioning:", credit_card_df.rdd.getNumPartitions())

# question3-Increase partition size to 5
credit_card_df = credit_card_df.repartition(5)

print("Number of partitions after repartitioning:", credit_card_df.rdd.getNumPartitions())

# question4-Decrease partition size back to its original partition size
credit_card_df = credit_card_df.coalesce(1)

# question5-Create UDF to mask credit card numbers
def mask_card_number(card_number):
    return "*" * 12 + card_number[-4:]

mask_card_number_udf = udf(mask_card_number, StringType())

# question6-Add masked_card_number column to DataFrame
credit_card_df = credit_card_df.withColumn("masked_card_number", mask_card_number_udf("card_number"))
credit_card_df.show(truncate=False)

spark.stop()
