# spark_projects

This project includes ETL processes. It is intended to help those who want to code on Spark.

First of all, Pokemon data is used to show ETL structure. It includes basic ETL operations in both Python and Scala languages. Both of them give the same results. You can compare these languages and code/convert your Spark ETL jobs.

Pokemon data is taken from: https://www.kaggle.com/abcsds/pokemon

Original data is from: pokemon.com,pokemondb,bulbapedia

Second ETL job is about temperature change. In this case, there is an assumption that data analyst, data scientist etc. need some manipulated temperature data from data engineer. In real life examples, some parameters are required to start Spark ETL job, mostly it is current or previous day. Our parameter is start date here. Generally, data is stored in relational database, HDFS environment or taken from api. In this example, data is taken from csv file not make process more complicated. You can try the code without installing anything except spark. The purpose of project is designing ETL job in spark.

Temperature change data is taken from: https://www.kaggle.com/sevgisarac/temperature-change

Original data is from: http://www.fao.org/faostat/en/#data/ET/metadata

You can use the code wherever you want.
