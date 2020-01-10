from datetime import datetime
from pyspark.sql import SparkSession


def gg(x):
    y = sorted(x[1], key=lambda k: k["start"])
    result = list()
    for item in y:
        result.append(item)
    return result


def already_sorted(x):
    result = list()
    for item in x[1]:
        result.append(item)
    return x


spark = SparkSession.builder \
    .master('local') \
    .appName('test etl') \
    .getOrCreate()

data = [(1, datetime(2019, 1, 1).date(), datetime(2019, 1, 31).date(), 'prod1'),
        (1, datetime(2019, 1, 1).date(), datetime(2019, 1, 31).date(), 'prod4'),
        (1, datetime(2019, 1, 5).date(), datetime(2019, 1, 15).date(), 'prod2'),
        (1, datetime(2019, 1, 25).date(), datetime(2019, 2, 1).date(), 'prod3'),
        (2, datetime(2019, 1, 25).date(), datetime(2019, 2, 1).date(), 'prod3'),
        (1, datetime(2019, 2, 20).date(), datetime(2019, 3, 1).date(), 'prod1')]

df = spark.createDataFrame(data, ["id", "start", "end", "product"])

rdd = df.rdd
result_df = rdd.map(lambda x: (x["id"], x))

result2 = result_df \
    .groupByKey() \
    .mapValues(lambda record: sorted(record, key=lambda k: k["start"])) \
    .map(already_sorted)

print(result2.take(10))

result2 = result_df \
    .groupByKey() \
    .flatMap(gg)

print(result2.take(10))

df = spark.createDataFrame(result2, verifySchema=False)
df.show()
