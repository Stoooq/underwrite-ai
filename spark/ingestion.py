from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


class DataLoader:
    def __init__(self, spark_session: SparkSession, base_path: Path):
        self.spark = spark_session
        self.base_path = base_path

    def _load_csv(self, path: str) -> DataFrame:
        # file_path = Path(f"{self.base_path}/{path}")
        return self.spark.read.csv(str(Path(self.base_path) / path), header=True, inferSchema=True)

    def _validate_columns(self, df: DataFrame, required_columns: list[str]):
        required_columns_set = set(required_columns)
        columns_set = set(df.columns)

        columns_difference = required_columns_set - columns_set

        if columns_difference:
            raise ValueError(f"No such columns in data: {columns_difference}")

    def _validate_types(self, df: DataFrame, expected_types: dict[str, str]):
        data_dict = dict(df.dtypes)

        type_conflicts = {
            key: (expected_types[key], data_dict[key])
            for key in expected_types
            if expected_types[key] != data_dict.get(key)
        }

        if type_conflicts:
            raise ValueError(f"Data types conflict in columns: {type_conflicts}")

    def load_table(
        self,
        path: str,
        schema: dict[str, str],
    ) -> DataFrame:
        df = self._load_csv(path)
        self._validate_columns(df, schema.keys())
        self._validate_types(df, schema)
        return df
