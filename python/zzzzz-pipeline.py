import os

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark-connect-ml/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark-connect-ml/bin/python"

os.environ["SPARK_CONNECT_MODE_ENABLED"] = ""
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

df = spark.createDataFrame(
    [(1.0, 2.0, 1.0), (2.0, -1.0, 1.0), (-3.0, -2.0, 0.0), (-1.0, -2.0, 0.0)],
    ["a", "b", "label"])

vecAssembler = VectorAssembler(outputCol="features")
vecAssembler.setInputCols(["a", "b"])

vecAssembler.transform(df).show()
