import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master('local') \
        .appName('test etl') \
        .getOrCreate()
data = [(1, 1, 19), (2, 1, None), (3, 2, 0), (4, 100, 1000), (5, 1, 19), (6, 1, 200), (7, 2, 500), (8, 100, 1000)]
customer = [(1, 'Customer_1'), (2, 'Customer_2'), (3, 'Customer_3'), (1, 'Customer_noooo')]
ordersDataFrame = spark.createDataFrame(data, ["id", "customers_id", "amount"])
customersDataFrame = spark.createDataFrame(customer, ["cid", "login"])
joined = ordersDataFrame.join(customersDataFrame,
                              (ordersDataFrame.customers_id == customersDataFrame.cid),
                              "inner")
# joined.withColumn("amount", f.coalesce("amount", f.lit("xxxx"))).show()
# sortmergejoin
joined.explain()
# broadcast join
joined = ordersDataFrame.join(f.broadcast(customersDataFrame), (ordersDataFrame.customers_id == customersDataFrame.cid), "inner")
joined.explain()