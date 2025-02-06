import os

from pyspark.ml.classification import (LogisticRegression,
                                       LogisticRegressionModel)
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator, BinaryClassificationEvaluator, \
    MulticlassClassificationEvaluator, MultilabelClassificationEvaluator, RankingEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

os.environ["PYSPARK_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark/bin/python"
# df = spark.read.format("libsvm").load("/home/bobwang/github/mytools/spark.home/spark-3.5.1-bin-hadoop3/data/mllib/sample_binary_classification_data.txt")

os.environ["SPARK_CONNECT_MODE_ENABLED"] = ""
spark = (SparkSession.builder.remote("sc://localhost")
         .getOrCreate())

@udf(returnType=IntegerType())
def double(x):
    return x * x


df = spark.range(1, 2)
df = df.withColumn("doubled", double(col("id")))

df.show()