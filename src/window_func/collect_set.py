from datetime import datetime

from pyspark import Row
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

spark = SparkSession.builder\
     .master('local')\
     .appName('test etl')\
     .getOrCreate()

data = [(1, datetime(2019,1,1).date(), datetime(2019,1,31).date(), 'prod1'),
        (1, datetime(2019, 1, 2).date(), datetime(2019, 1, 31).date(), 'prod4'),
        (1, datetime(2019,1,5).date(), datetime(2019,1,15).date(), 'prod2'),
        (2, datetime(2019,1,25).date(), datetime(2019,2,1).date(), 'prod3'),
        (2, datetime(2019,1,26).date(), datetime(2019,2,1).date(), 'prod3'),
        (2, datetime(2019,2,20).date(), datetime(2019,3,1).date(), 'prod1')]

df = spark.createDataFrame(data, ["id", "start", "end", "product"])
df = df.withColumn("collect_to", f.when(f.col("product") == "prod1","prod1").otherwise("prod2"))
df = df.withColumn("collect_to", f.collect_set("product").over(Window.partitionBy("id").orderBy("start")))
df.show(20, False)