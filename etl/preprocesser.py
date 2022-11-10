from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType


class Preprocess:
    """
    Preprocess the raw datasets
    """
    
    @staticmethod
    def get_immigration(immigration):
        """
        Preprocess  immigrantion dataset. Rename columns with understandable names. Change to correct formats in dates and select only important columns 
        @param immigrantion: immigrantion dataset
        return: preprocessed immigrantion dataset 
        """
        immigration = immigration \
            .withColumn("cic_id", col("cicid").cast("integer")) \
            .withColumnRenamed("i94addr", "state_code") \
            .withColumnRenamed("i94port", "port_code") \
            .withColumnRenamed("fltno", "flight_nr") \
            .withColumnRenamed("visatype", "visa_type") \
            .withColumn("visa_code", col("i94visa").cast("integer")) \
            .withColumn("mode", col("i94mode").cast("integer")) \
            .withColumn("country_origin_code", col("i94res").cast("integer")) \
            .withColumn("country_cit_code", col("i94cit").cast("integer")) \
            .withColumn("year", col("i94yr").cast("integer")) \
            .withColumn("month", col("i94mon").cast("integer")) \
            .withColumn("birth_year", col("biryear").cast("integer")) \
            .withColumn("age", col("i94bir").cast("integer")) \
            .withColumn("counter", col("count").cast("integer")) \
            .withColumn("data_base_sas", to_date(lit("01/01/1960"), "MM/dd/yyyy")) \
            .withColumn("arrival_date", expr("date_add(data_base_sas, arrdate)")) \
            .withColumn("departure_date", expr("date_add(data_base_sas, depdate)")) \
            .withColumn("arrival_year", month("arrival_date")) \
            .withColumn("arrival_month", year("arrival_date")) \
            .withColumn("arrival_day", dayofmonth("arrival_date")) \
            .drop("data_base_sas", "arrdate", "depdate")\
            .drop("cicid") \
            .drop("i94visa") \
            .drop("i94mode") \
            .drop("i94res") \
            .drop("i94cit") \
            .drop("i94yr") \
            .drop("i94mon") \
            .drop("biryear") \
            .drop("i94bir") \
            .drop("count") 

        return immigration.select(col("cic_id"), col("state_code"), col("port_code"), col("visapost"), col("matflag"),col("dtaddto") \
                                  , col("gender"), col("airline"), col("admnum"), col("flight_nr"), col("visa_type"), col("visa_code"), col("mode") \
                                  , col("country_origin_code"), col("country_cit_code"), col("year"), col("month"), col("birth_year") \
                                  , col("age"), col("counter"), col("arrival_date"), col("departure_date"), col("arrival_year")\
                                  , col("arrival_day"), col("arrival_month"))


    @staticmethod
    def get_cities_demographics(demographics):
        """
        Preprocess city demographics dataset by filling null values with 0 and grouping by city and state and pivot by
        Race into different columns. Then group by state_code and take total sum of each race and then the ratio of each race.
        @param demographics: city demographics dataset
        return: preprocessed city demographics dataset 
        """
        pivot = demographics.fillna(0).groupBy(col("City"), col("State"), col("Median Age"), col("Male Population"),
                                     col("Female Population") \
                                     , col("Total Population"), col("Number of Veterans"), col("Foreign-born"),
                                     col("Average Household Size") \
                                     , col("State Code")).pivot("Race").agg(sum("count").cast("integer")) \
            .fillna(0)
        
        demog = pivot \
            .groupBy(col("State Code").alias("state_code"), col("State").alias("state")).agg(
             round(mean("Median Age"),2).alias("median_age"), sum("Total Population").alias("total_population"),\
             sum("Male Population").alias("male_population"), sum("Female Population").alias("female_population"),\
             round(mean("Average Household Size"),2).alias("average_household_size"),
             sum("American Indian and Alaska Native").alias("american_indian_and_alaska_native"),\
             sum("Asian").alias("asian"), sum("Black or African-American").alias("black_or_african_american"),\
             sum("Hispanic or Latino").alias("hispanic_or_latino"),\
             sum("White").alias("white")) \
            .withColumn("male_population_ratio", round(col("male_population") / col("total_population"), 2))\
            .withColumn("female_population_ratio", round(col("female_population") / col("total_population"), 2))\
            .withColumn("american_indian_and_alaska_native_ratio",
                        round(col("american_indian_and_alaska_native") / col("total_population"), 2))\
            .withColumn("asian_ratio", round(col("asian") / col("total_population"), 2))\
            .withColumn("black_or_african_american_ratio",
                        round(col("black_or_african_american") / col("total_population"), 2))\
            .withColumn("hispanic_or_latino_ratio", round(col("hispanic_or_latino") / col("total_population"), 2))\
            .withColumn("white_ratio", round(col("white") / col("total_population"), 2))

        return demog


    @staticmethod
    def get_airports(airports):
        """
        Preprocess airports dataset filtering for only US airports and ignoring anything that is not an airport.
        Extract iso regions, drop coordinates and change dtype of elevation_ft as float .
        @param airports: airports dataframe
        return: preprocessed airports dataframe 
        """
        airports = airports \
            .withColumnRenamed("ident", "airport_id") \
            .withColumnRenamed("name", "airport_name") \
            .withColumn("state_code",split(col("iso_region"),"-").getItem(1))\
            .drop("iso_region") \
            .where(
            (col("iso_country") == "US") & (col("type").isin("large_airport", "medium_airport", "small_airport"))) \
            .drop(col("coordinates")) \
            .withColumn("elevation_ft", col("elevation_ft").cast("float"))
         
        return airports

   
    def get_temperature(temperature,demographics):
        """
        Preprocess temperature dataset filtering country only for United States.
        Rename columns and bringing columns into right dtypes.
        Group data and take average of avg_temp and avg_temp_uncertainty.

        @param temperature: temperature dataframe
        return: preprocessed temperature dataframe 
        """
        temperature = temperature \
            .withColumnRenamed("AverageTemperature", "avg_temp") \
            .withColumnRenamed("AverageTemperatureUncertainty", "avg_temp_uncertainty") \
            .withColumnRenamed("City", "city") \
            .withColumnRenamed("Country", "country") \
            .withColumn("date", to_date(col("dt"), "yyyy-mm-dd")) \
            .where(
            (col("country") == "United States"))\
            .dropna(subset=["dt","avg_temp","avg_temp_uncertainty"])\
            .withColumn("avg_temp", col("avg_temp").cast('double')) \
            .withColumn("avg_temp_uncertainty", col("avg_temp_uncertainty").cast('double')) \
            .withColumn("month", month("dt")) \
            .withColumn("year", year("dt")) \
            .groupBy("dt","city","country","year","month")\
            .agg(round(mean("avg_temp"),2).alias("avg_temp"),round(mean("avg_temp_uncertainty"),2).alias("avg_temp_uncertainty"))\
            .join(demographics.select(col("City").alias("city"),col("State Code").alias("state_code")), "city","left")\
            .groupBy("dt","state_code","city","country","year","month")\
            .agg(round(mean("avg_temp"),2).alias("avg_temp"),round(mean("avg_temp_uncertainty"),2).alias("avg_temp_uncertainty"))

        return temperature
    
    
    
