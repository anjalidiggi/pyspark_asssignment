from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Product Analysis DataFrame") \
    .getOrCreate()

# question1-Create DataFrames
purchase_data_df = spark.createDataFrame([(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"),
                                          (2, "dell i5 core"), (3, "iphone13"), (3, "dell i5 core"),
                                          (1, "dell i3 core"), (1, "hp i5 core"), (1, "iphone14"),
                                          (3, "iphone14"), (4, "iphone13")],
                                         ["customer", "product_model"])

product_data_df = spark.createDataFrame([("iphone13",), ("dell i5 core",),
                                         ("dell i3 core",), ("hp i5 core",), ("iphone14",)],
                                        ["product_model"])

# question2-Find customers who have bought only iphone13
iphone13_customers_df = purchase_data_df.filter(col("product_model") == "iphone13") \
                                        .select("customer")
all_customers_df = purchase_data_df.select("customer").distinct()
only_iphone13_customers_df = all_customers_df.subtract(iphone13_customers_df)
only_iphone13_customers_df.show()

# question3-Find customers who upgraded from product iphone13 to product iphone14
iphone13_customers_df = purchase_data_df.filter(col("product_model") == "iphone13") \
                                        .select("customer")
iphone14_customers_df = purchase_data_df.filter(col("product_model") == "iphone14") \
                                        .select("customer")
upgraded_customers_df = iphone13_customers_df.intersect(iphone14_customers_df)
upgraded_customers_df.show()

# question4-Find customers who have bought all models in the new Product Data
all_products_df = product_data_df.select("product_model")
unique_customers_df = purchase_data_df.select("customer").distinct()
all_models_bought_df = unique_customers_df
for product in all_products_df.collect():
    model = product["product_model"]
    model_customers_df = purchase_data_df.filter(col("product_model") == model) \
                                         .select("customer")
    all_models_bought_df = all_models_bought_df.intersect(model_customers_df)
all_models_bought_df.show()

spark.stop()
