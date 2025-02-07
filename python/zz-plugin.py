from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("zz-plugin").getOrCreate()

results = spark.sparkContext._jvm.PythonUtils.toSeq(["a", "b"])

# for r in results:
#     print(r)



# data = [
#     ("Alice", 30, "New York"),
#     ("Bob", 25, "London"),
#     ("Charlie", 35, "Paris"),
#     ("David", 28, "Tokyo"),
#     (None, 40, "Sydney")  # Example with a missing value
# ]
#
# # Define column names (schema as a simple list of strings)
# columns = ["name", "age", "city"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# df = df.select(*columns)
# df.show()