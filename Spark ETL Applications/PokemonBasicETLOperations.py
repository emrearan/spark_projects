import findspark
findspark.init("")

import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F



def main():
    """It provides three different datasets on Pokemon data. First one brings maximum attack value of every type under specific conditions.
    Second one brings some specific legend pokemons stats. Third one brings some specific pokemons according to filters.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark = SparkSession \
    .builder \
    .appName("PokemonBasicETLOperations") \
    .config("spark.eventLog.enabled", True) \
    .enableHiveSupport() \
    .getOrCreate()

    print('PokemonBasicETLOperations ETL is up-and-running')
    
    # execute ETL pipeline
    pokemon = extract(spark)
    max_attack_per_type,agg_legend_poke,special_criteria_poke = transform(pokemon)
    load(max_attack_per_type,agg_legend_poke,special_criteria_poke)

    print('PokemonBasicETLOperations ETL job is finished')
    spark.stop()
    return None
	
	
def extract(spark):
    """Loads data from csv files and returns Spark dataframe.
    :param spark: Spark session object.
    :return: Env_Temp_Change,Def_and_Standarts
    """
    
    pokemon = spark.read.csv('data_store\Pokemon.csv', header='true', inferSchema =True)
    
    return pokemon
	
def transform(pokemon):
    """Transforms original dataset to Data Marts
    :param pokemon: Input DataFrame includes "Pokemons".
    :return: Transformed DataFrame.
    """
       
    pokemon = pokemon.withColumnRenamed('Type 1', 'Type_1') \
                     .withColumnRenamed('Type 2', 'Type_2') \
                     .withColumnRenamed('Sp. Atk', 'Sp_Atk') \
                     .withColumnRenamed('Sp. Def', 'Sp_Def') 
    
    
    max_attack_per_type = pokemon
    max_attack_per_type = max_attack_per_type.where(F.col("Generation") == 1) \
                                             .filter(pokemon.Name.like('%Mega%') == False) \
                                             .select('Name', \
                                             F.col('Type_1').alias('Type'), \
                                             'Attack', \
                                             'Sp_Atk', \
                                             F.row_number().over(Window.partitionBy("Type_1").orderBy(F.col("Attack").desc(),F.col("Sp_Atk").desc())).alias("rank"))  \
                                             .where(F.col("rank") == 1) \
                                             .drop('rank')
    
    
    agg_legend_poke = pokemon
    agg_legend_poke = agg_legend_poke.where((F.col("Legendary") == True) & (F.col("Type_2") == 'Flying')) \
                      .groupBy("Type_1").agg(F.count('Total').alias('Total_Number'), F.mean('Total').alias('Average_Power')) \
                      .orderBy(F.col('Total_Number').desc())
    
    
    special_criteria_poke = pokemon
    special_criteria_poke = special_criteria_poke.where(F.col("Generation").isin(1,2,4,5)) \
                                                 .where((F.col("HP") > 70) & (F.col("Attack") > 100) & (F.col("Defense") < 80)) \
                                                 .where(F.col("Speed").between(50,100)) \
                                                 .withColumn('Name',F.trim(F.when(special_criteria_poke.Name.like('% %'), F.col("Name").substr(F.lit(1), F.instr(F.col("Name"), ' '))) \
                                                 .otherwise(F.col("Name")))) \
                                                 .orderBy(F.col('Total').desc())

    return max_attack_per_type,agg_legend_poke,special_criteria_poke
	


def load(max_attack_per_type,agg_legend_poke,special_criteria_poke):
    """Collect data locally and write to CSV.
    :param max_attack_per_type: DataFrame 1 to print.
    :param agg_legend_poke: DataFrame 2 to print.
    :param special_criteria_poke: DataFrame 3 to print.
    :return: None
    """
    
    max_attack_per_type.coalesce(1).write.csv('output_data\pokemon\max_attack_per_type', mode='overwrite', header=True)

    agg_legend_poke.coalesce(1).write.csv('output_data\pokemon\legend_poke_agg', mode='overwrite', header=True)

    special_criteria_poke.coalesce(1).write.csv('output_data\pokemon\special_criteria_poke', mode='overwrite', header=True)
    
    return None
	

if __name__ == '__main__':
    main()
