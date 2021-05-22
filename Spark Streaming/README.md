# Spark Streaming Applications

This project includes streaming application. To run application you should have Spark and MongoDB.

- Spark can be downloaded from [__here__](https://spark.apache.org/downloads.html "here")

- Free version of MongoDB can be downloaded from [__here__](https://www.mongodb.com/ "here")

Reading Data From Web API and Create CSV File : [__streaming.ipynb__](https://github.com/emrearan/spark_projects/blob/main/Spark%20Streaming/streaming.ipynb "__streaming.ipynb__")

Spark Streaming From File to MongoDB:  [__readingData.ipynb__](https://github.com/emrearan/spark_projects/blob/main/Spark%20Streaming/readingData.ipynb "__readingData.ipynb__")

The purpose of project is to get data from csv files and write it to MongoDB. To try application:

1. Open __streaming.ipynb__ and run all cells
2. Open __readingData.ipynb__ and run __get_jokes()__
3. See the results from MongoDB using __print(json_data)__

__Note:__ You should run necessary cells to be able to run __get_jokes()__ and __print(json_data)__


Flow goes like followings:

1. When you run streaming.ipynb Jupyter Notebook file, Spark starts listening __/data/input/__ folder.

2. If it is your first run, spark will find __'jokes_20210522_204032.csv'__ data and get an action.

3. This data will be written to MongoDB, you can see the log on streaming Notebook.

4. After this, flow continues with retrieving data from Web API.

5. When you run  __get_jokes()__ in another Notebook, data will be written to __/data/input/__ folder.

6. Again, spark will realize new data and get and action.

7. To sum up, Spark will get data from folder __/data/input/__ and write to __MongoDB__ whenever data comes.
