from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Extract:
    """
    Input the paths and return dataframes
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _read_standard_csv(self, filepath, delimiter=","):
        """
        Input path in CSV format
        @param filepath: csv file path
        @param delimiter: delimiter
        return: dataframe
        """
        return self.spark.read.format("csv").option("header", "true").option("delimiter", delimiter).load(filepath)

    def read_cities_demographic_raw(self):
        """
        Read demographics dataset
        return: demographics dataset
        """
        return self._read_standard_csv(filepath=self.paths["city_demographics"], delimiter=";")

    def read_temperature_raw(self):
        """
        Read immigration dataset.
        return: inmigration dataset
        """
        return self._read_standard_csv(filepath = self.paths["temperature"], delimiter=",")

    def read_immigration_raw(self):
        """
        Read immigration dataset.
        return: immigration dataset
        """
        return self.spark.read.parquet(self.paths["immigration"])

    def read_airports_raw(self):
        """
        Read airports dataset
        return: airports dataset
        """
        return self._read_standard_csv(self.paths["airports"])


    def read_labels_raw(self):
        """
        Read inmigration dataset.
        return: label dataset
        """
        return self.spark._read_standard_csv.parquet(self.paths["label_data"])