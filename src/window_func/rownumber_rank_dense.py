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

# could adda more partition fields or sort..
window = Window.partitionBy(df['id']).orderBy(df['sorted_col'].desc())
df = df.withColumn("row_number", f.row_number().over(window))\
    .withColumn("rank", f.rank().over(window))\
    .withColumn("dense_rank", f.dense_rank().over(window))

df.show()
