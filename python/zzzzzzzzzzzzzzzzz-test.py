import os

from pyspark.ml.linalg import Vector, Vectors

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark-connect-ml/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark-connect-ml/bin/python"

os.environ["SPARK_CONNECT_MODE_ENABLED"] = ""
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

# df = spark.read.format("libsvm").load("/home/bobwang/github/mytools/spark.home/spark-3.5.1-bin-hadoop3/data/mllib/sample_binary_classification_data.txt")

df = spark.createDataFrame([
    (Vectors.dense([1.0, 2.0]), 1),
    (Vectors.dense([2.0, -1.0]), 1),
    (Vectors.dense([-3.0, -2.0]), 0),
    (Vectors.dense([-1.0, -2.0]), 0),
], schema=['features', 'label'])

lr = LogisticRegression()
lr.setMaxIter(30)

model: LogisticRegressionModel = lr.fit(df)
x = model.predictRaw(Vectors.dense([1.0, 2.0]))
print(f"predictRaw {x}")
# TODO make model.evaluate work
# s = model.evaluate(df)
# print(s.weightCol)
assert model.getMaxIter() == 30
print(model.summary.weightedRecall)
print(model.summary.weightedPrecision)
print(model.summary.weightedFalsePositiveRate)
print(model.summary.recallByLabel)
print(model.summary.truePositiveRateByLabel)
print(model.coefficients)
print(model.intercept)

model.transform(df).show()

model.summary.roc.show()

# df.show()
