# Spark Streaming Applications

This project includes streaming application. To run application you should have Spark and MongoDB.

- Spark can be downloaded from here https://spark.apache.org/downloads.html

- Free version of MongoDB can be downloaded from here https://www.mongodb.com/


The purpose of project is to get data from csv files and write it to MongoDB. To try application:

1. Open streaming.ipynb and run all cells
2. Open readingData.ipynb and run __get_jokes()__
3. See the results from MongoDB using __print(json_data)__

__Note:__ You should run necessary cells to be able to run __get_jokes()__ and __print(json_data)__

First off all, when you run streaming.ipynb Jupyter Notebook file, Spark starts listening __/data/input/__ folder. When you run first, spark will find __'jokes_20210522_204032.csv'__ data and get an action. Data will be written to MongoDB. After this, flow goes with retrieving data from Web API. When __get_jokes()__ run in another Notebook, data is being written to __/data/input/__ folder. Again, spark will realize new data and get and action. To sum up, Spark will get data from folder and write to MongoDB whenever data comes.


Here is a [Link](https://spark.apache.org/downloads.html "here").