from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.read.csv("data/raw/home-credit-default-risk/application_train.csv")
