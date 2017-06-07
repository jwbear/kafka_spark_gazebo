# kafka_spark_gazebo
By request, simple Java source code for running Apache Kafka on top of a Gazebo/ROS implementation and consuming the streaming data with Spark Streaming. Reads laser and odometry data. There are no analytics in this batch, but the full framework is soon to be released.

This code is not package level. The main class is not included and some of the data objects referenced are part of the full EKF SLAM framework.

ProducerSimConnect Class needs to be added to connect to your simulator bot.
