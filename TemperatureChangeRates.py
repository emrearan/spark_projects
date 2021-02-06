import findspark
findspark.init("")

import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import Row
import pyspark.sql.functions as F


def Main(start_date):
    """It provides "Monthly Temperature Change Rates" of Germany, France, UK and USA for spesific date interval. 
    Function requires parameter to get starting date. 
    :param start_date: You should specify starting date to get Data Mart.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark = SparkSession \
    .builder \
    .appName("Ornek") \
    .config("spark.eventLog.enabled", True) \
    .enableHiveSupport() \
    .getOrCreate()

    print('TemperatureChangeRates ETL is up-and-running')
    
    # execute ETL pipeline
    Env_Temp_Change, Def_and_Standarts = extract(spark)
    transformed_data = transform(Env_Temp_Change,Def_and_Standarts, start_date)
    load(transformed_data)

    print('TemperatureChangeRates ETL job is finished')
    spark.stop()
    return None
	
	
def extract(spark):
    """Loads data from csv files and returns two Spark dataframes.
    :param spark: Spark session object.
    :return: Env_Temp_Change,Def_and_Standarts
    """
    
    Env_Temp_Change = spark.read.csv('data_store\Env_Temp_Change.csv',header='true')
    Def_and_Standarts = spark.read.csv('data_store\Def_and_Standarts.csv',header='true')
    
    return Env_Temp_Change,Def_and_Standarts
	
	
def transform(Env_Temp_Change,Def_and_Standarts, start_date):
    """Transforms original datasets to Data Mart
    :param Env_Temp_Change: First input DataFrame includes "Temperature Change Rates".
    :param Def_and_Standarts: Second input DataFrame includes "Definitions and Standarts".
    :param start_date: Starting Date Parameter.
    :return: Transformed DataFrame.
    """
    
    def calcForStackFunc(start_date):
        """Creates a string list of dates for stack() function to unpivot dataframe.
        :param start_date: Starting Date Parameter.
        :return: String list of dates.
        """
        stack_string_list = ''
        last_year = int(transformed_data.columns[-1][1:]) # to detect last year
        n = 2020-start_date # Number of desired years

        for i in range (start_date,last_year+1):
            stack_string_list = stack_string_list + ", '"+ str(i) + "', Y" + str(i)
        stack_string_list = stack_string_list[2:]
        stack_string_list = str(n) + ', ' + stack_string_list
        
        return stack_string_list
    
    
    transformed_data = Env_Temp_Change
    transformed_data = transformed_data.where(F.col("Area").isin({"Germany", "France" ,"United Kingdom","United States of America"})) \
                                       .where(F.col("Element Code") == 7271) \
                                       .where(F.col("Months Code").isin({7016, 7017 , 7018, 7019, 7020}) == False) \
                                       .withColumnRenamed('Area Code', 'Area_Code') \
                                       .withColumnRenamed('Months Code', 'Months_Code')

    Def_and_Standarts = Def_and_Standarts.withColumnRenamed('Country Code', 'Country_Code') \
                                         .withColumnRenamed('ISO2 Code', 'ISO2_Code') \
                                         .withColumnRenamed('ISO3 Code', 'ISO3_Code')
            
    
    
    transformed_data = transformed_data.selectExpr("Area_Code","Area","Months_Code","Unit","stack(" + calcForStackFunc(start_date) + ") as (Year, Change_Rate)") \
                                       .withColumn('Date',F.unix_timestamp(F.concat(F.col('Year'),F.col('Months_Code')[3:4]),'yyyyMM').cast("timestamp"))  \
                                       .join(Def_and_Standarts, transformed_data.Area_Code == Def_and_Standarts.Country_Code, 'left_outer') \
                                       .select('Area_Code', \
                                       'Area', \
                                       'ISO2_Code', \
                                       'ISO3_Code', \
                                       'Date', \
                                        F.when(transformed_data.Unit.like('%C%'), 'Centigrade').when(transformed_data.Unit.like('%F%'), 'Fahrenheit ').otherwise('').alias('Unit'), \
                                       'Change_Rate') \
                                       .orderBy('Area','Date')

    return transformed_data
	
	
def load(df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    
    df.coalesce(1).write.csv('output_data\data_temp_changes', mode='overwrite', header=True)
    
    return None



if __name__ == '__main__':
    main(sys.argv[1])
