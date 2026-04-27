import os
from pathlib import Path

from pyspark.sql import SparkSession

from spark.ingestion import DataLoader
from spark.schemas import APPLICATION_SCHEMA, BUREAU_SCHEMA


def main():
    os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@17"
    spark = SparkSession.builder.getOrCreate()

    data_path = Path("data/raw/home-credit-default-risk")

    dl = DataLoader(spark, base_path=data_path)

    data_schemas = [
        ("application", "application_train.csv", APPLICATION_SCHEMA),
        ("bureau", "bureau.csv", BUREAU_SCHEMA),
    ]

    dataframes = {}

    for name, path, schema in data_schemas:
        df = dl.load_table(path=path, schema=schema)
        dataframes[name] = df

    spark.stop()


if __name__ == "__main__":
    main()
