import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark-connect-ml/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/bobwang/anaconda3/envs/pyspark-connect-ml/bin/python"

os.environ["SPARK_CONNECT_MODE_ENABLED"] = ""
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

df = spark.createDataFrame(
    [(1.0, 2.0, 1.0),
     (2.0, -1.0, 1.0),
     (-3.0, -2.0, 0.0),
     (-1.0, -2.0, 0.0)],
    ["a", "b", "label"])

assembler = VectorAssembler(inputCols=["a", "b"], outputCol="features")

lr = LogisticRegression()
lr.setMaxIter(30)

pipeline = Pipeline(stages=[assembler, lr])

# Fit the pipeline to the data
pipeline_model = pipeline.fit(df)
pipeline_model.transform(df).show()

############# logistic regression model
lr_model = pipeline_model.stages[1]

z = lr_model.summary
x = lr_model.predictRaw(Vectors.dense([1.0, 2.0]))
print(f"predictRaw {x}")
assert lr_model.getMaxIter() == 30
lr_model.summary.roc.show()

print(lr_model.summary.weightedRecall)
print(lr_model.summary.recallByLabel)
print(lr_model.coefficients)
print(lr_model.intercept)
