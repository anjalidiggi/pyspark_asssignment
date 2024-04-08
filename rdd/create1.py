from pyspark import SparkContext

sc = SparkContext(appName="Product Analysis RDD")

# question1-Create RDDs
purchase_data_rdd = sc.parallelize([(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"),
                                     (2, "dell i5 core"), (3, "iphone13"), (3, "dell i5 core"),
                                     (1, "dell i3 core"), (1, "hp i5 core"), (1, "iphone14"),
                                     (3, "iphone14"), (4, "iphone13")])
product_data_rdd = sc.parallelize(["iphone13", "dell i5 core", "dell i3 core", "hp i5 core", "iphone14"])

# question2-Find customers who have bought only iphone13
iphone13_customers_rdd = purchase_data_rdd.filter(lambda x: x[1] == "iphone13") \
                                           .map(lambda x: x[0])
all_customers_rdd = purchase_data_rdd.map(lambda x: x[0]).distinct()
only_iphone13_customers_rdd = all_customers_rdd.subtract(iphone13_customers_rdd)
only_iphone13_customers_rdd.collect()

# question3- Find customers who upgraded from product iphone13 to product iphone14
iphone13_customers_rdd = purchase_data_rdd.filter(lambda x: x[1] == "iphone13") \
                                           .map(lambda x: x[0])
iphone14_customers_rdd = purchase_data_rdd.filter(lambda x: x[1] == "iphone14") \
                                           .map(lambda x: x[0])
upgraded_customers_rdd = iphone13_customers_rdd.intersection(iphone14_customers_rdd)
upgraded_customers_rdd.collect()

# question4-Find customers who have bought all models in the new Product Data
all_models_bought_rdd = None
for product in product_data_rdd.collect():
    model_customers_rdd = purchase_data_rdd.filter(lambda x: x[1] == product) \
                                            .map(lambda x: x[0])
    if all_models_bought_rdd is None:
        all_models_bought_rdd = model_customers_rdd
    else:
        all_models_bought_rdd = all_models_bought_rdd.intersection(model_customers_rdd)
all_models_bought_rdd.collect()

sc.stop()
