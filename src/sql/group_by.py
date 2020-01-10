from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

spark = SparkSession.builder\
     .master('local')\
     .appName('test etl')\
     .getOrCreate()

data = [(1, 1, 'proda'),
        (1, 2, 'prodb'),
        (1, 3, 'prodc'),
        (2, 1, 'proda'),
        (2, 2, 'prodb'),
        (2, 2, 'prodc')]

df = spark.createDataFrame(data, ["id", "sorted_col", "product"])
df.registerTempTable("simpleTable")

result = spark.sql("select id, sum(sorted_col) as sum from simpleTable group by id")
result.show()
