from pyspark.sql.functions import *


class Quality_Checker:
    """
    Validate the integrity of the model and the quality of the data
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def get_immigration(self):
        """
        Get immigration table
        return: immigration facts table
        """
        return self.spark.read.parquet(self.paths["immigration_load"])

    def get_demographics(self):
        """
        Get demographics table
        return: demographics dimension table
        """
        return self.spark.read.parquet(self.paths["demographics_load"])

    def get_airports(self):
        """
        Get airports table
        :return: airports dimensiontable
        """
        return self.spark.read.parquet(self.paths["airports_load"])
    
    def get_temperature(self):
        """
        Get temperature table
        :return: temperature dimension table
        """
        return self.spark.read.parquet(self.paths["temperature_load"])



    def empty_check(self, df, path):
        """
        Validates if there is any data in a dataframe
        Raises error if not, print count if count > 0
        @param dataframe: dataframe
        @param path: parquet path
        """
        df_count = df.count()
        df_name = path.replace("./model/","").replace(".parquet","")
        if  not df_count != 0:
            raise ValueError(df_name+": This table is empty!")
        else:
            print("Table: " + df_name + f" is not empty: total {df_count} records.")

    def duplicate_check(self, df, path):
        """
        Validates if there are any dublicated rows in a dataframe.
        Raises error if yes and prints and example
        @param dataframe: dataframe
        @param path: parquet path
        """
        df_count = df.count()
        df_name = path.replace("./model/","").replace(".parquet","")
        
        if df_count > df.dropDuplicates(df.columns).count():
            print("Duplicated rows!!! \nAn example:")
            print(df.exceptAll(df.dropDuplicates(df.columns)).show(6))
            raise ValueError(df_name+": This table has duplicates!")
        else:
            print("Table: " + df_name + " has no duplicates!")
            


    def validate_star(self, immigration, demographics, airports, temperature):
        """
        Validate if all dimension tables can be joined with facts table
        @param immigration: immigration table
        @param demographics: demographics table
        @param airports: airports table
        @param temperature: temperature table

        return: true or false if integrity is correct.
        """

        not_null_demographics = immigration.select(col("state_code")).distinct() \
                     .join(demographics, "state_code", "left")\
                        .filter(col("american_indian_and_alaska_native_ratio").isNotNull())\
                        .count() > 0
        not_null_airports  = immigration.select(col("state_code")).distinct() \
                     .join(airports , "state_code", "left")\
                        .filter(col("airport_name").isNotNull())\
                        .count() > 0
        not_null_temperature = immigration.select(col("state_code"),col("arrival_month"),col("arrival_year")).distinct() \
                     .join(temperature,(immigration.state_code==temperature.state_code) & \
                                       (immigration.arrival_month==temperature.month) & \
                                       (immigration.arrival_year==temperature.year), "left")\
                        .filter(col("city").isNotNull())\
                        .count() > 0
        

        return print(f"Demographics star connection works: {not_null_demographics}!  \nAirport star connection works: {not_null_airports}! \nTemperature star connection works: {not_null_temperature}")
