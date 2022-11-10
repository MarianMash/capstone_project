from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType

from pyspark.sql.functions import *


class Loader:
    """
    Loads the preprocessed datasets into a Data Warehouse.
    It's a star schema, consisting of one facts and several dimension tables
    """

    def __init__(self, paths, spark):
        self.paths = paths
        self.spark = spark
        
    def _load_immigration(self, immigration):
        """
        Loads and overwrites the immigration facts table in parquet files which is partitioned by arrival_year, arrival_month and arrival_day
        @param immigration: immigration table
        """
        partition_cols = ["arrival_year", "arrival_month", "arrival_day"]
        immigration.write.partitionBy(partition_cols).mode('overwrite').parquet(self.paths["immigration_load"])
        
    def _load_demographics(self, demographics):
        """
        Loads and overwrites the demographics table into parquet files.
        @param demographics: demographics table
        """
        demographics.write.mode('overwrite').parquet(self.paths["demographics_load"])
        
    def _load_temperature(self, temperature):
        """
        Loads and overwrites the temperature table into parquet files.
        @param temperature: temperature table
        """
        temperature.write.mode('overwrite').parquet(self.paths["temperature_load"])
        
    def _load_airports(self, airports):
        """
        Loads and overwrites the airports table into parquet files.
        @param airports: airports table
        """
        airports.write.mode('overwrite').parquet(self.paths["airports_load"])
        
        
        
    def load(self, immigration,demographics, airports, temperature):
            """
            Load the Data Warwhouse - connection through state_code makes star schema
            @param immigration: facts immigration 
            @param demographics: dimension demographics
            @param airports: dimension airports
            @param temperature: dimension temperature

            """

            self._load_immigration(immigration)
            self._load_demographics(demographics)
            self._load_airports(airports)
            self._load_temperature(temperature)


            
    