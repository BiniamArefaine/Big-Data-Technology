



=========================Installing Kafka and Scala IDE=============================


1.Download kafka application from https://kafka.apache.org/downloads and un-zip it
  Open the command line and run “gedit ~/.bashrc” , a bash file will open and paste the path of the unzipped, in our case it is, “export KAFKA_HOME=/home/cloudera/Desktop/kafka_2.13-2.6.0
  export PATH=$PATH:$KAFKA_HOME/bin”
  Zookeeper should run first, and  assuming the zookeeper is running perfectly then start the kafka by typing the command line “cd $KAFKA_HOME” and then “kafka-server-start.sh ./config/server.properties” …… now kafka is running perfectly 

2.Download scala IDE and install scala
  Download scala IDE for linux at  https://www.scala-lang.org/download/ and select the appropriate version.

========================Running the Project=========================================

1.Open the Scala IDE
2.Properly Import the producer and the consumer Projects
3.Properly Handle all the dependencies conflicts, if there are any, between the scala library and the pom dependencies.
4.With the correct and not expired "API key, API secret Key, Access token, Access token secret", set the producer Application

5.Now everything is set and there are no errors, then first run the Producer Application with Scala Application and
  this time, the datas are being published to the stores of the kafka
6.Then at the same time, Run the Consumer Application as Scala Application and this time, the data will be consumed by the consumer
  and saved in the hdfs as parquet file formats. And at the same time creating a hive table.
7.you can switch consoles between the producers and consumers
8.Connect your choice of data visualization tool with the hive table you created through the inet address which you can find by
  typing this command " $ifconfig " on the command line of your VM machine, in our case we used Tableau



Enjoy the visualization..........

