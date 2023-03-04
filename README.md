# final-project
#!/usr/bin python3

##########
Main file used for Toronto Bus System Streaming Data pipline project

##########
Zekun Peng
3/5/2023
##########
![Screenshot](final-project-0.png)

# 01-setup-ec-vm

Network: vpc-3f2f7b57
Security Group: launch-wizard-1


## ssh into ec vm

ssh -i 01-setup-ec-vm/nifi-ec-vm.pem ec2-user@###

## Install software

- sudo yum update
- docker


## Issues

**Permissions 0644 for '01-setup-ec-vm/nifi-ec-vm.pem' are too open.**

chmod 400 01-setup-ec-vm/nifi-ec-vm.pem


## Debezium

docker run -dit --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=## -e MYSQL_USER=## -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.6

### Questions

What if we want to setup debezium without example-mysql? and from scratch which works with mysql

## Nifi

docker run --name nifi -p 8080:8080 -p 8443:8443 --link mysql:mysql -d apache/nifi:1.12.0

Nifi is not running detatched. We have to ssh into the vm to start nifi whenever necessary

## Zookeeper

docker run -dit --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper:1.6

## Kafka

docker run -dit --name kafka -p 9092:9092 --link zookeeper:zookeeper debezium/kafka:1.6

## Rebooting
docker start mysql nifi zookeeper kafka connect spark-master spark-worker



# MSK

Client Information -> Plaintext Private Endpoint
Install telnet in virtual machine

```bash
sudo yum install telnet -y 
telnet b-1.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092

```

```bash
# Be sure to only stop the docker containers if you want to start them again in the future
# docker stop kafka zookeeper connect spark-master spark-worker
# WARNING: running docker rm will remove the data and you will have to redo several steps

# Set up client.properties as per the guide in kafka_2.12-2.6.2/bin

# Set up environment variables for your connection strings
ZOOKEEPER_CONNECTION_STRING=z-3.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:2181,z-2.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:2181,z-1.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:2181
BOOTSTRAP_SERVERS=b-1.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092,b-3.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092,b-2.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092

# Check that you are able to retrieve the topics on MSK
./kafka-topics.sh --list --zookeeper $ZOOKEEPER_CONNECTION_STRING

# Start the connect-msk docker so that the nifi and mysql containers are connected and ready to talk to MSK
docker run -dit --name connect-msk -p 8083:8083 -e GROUP_ID=1 -e CONFIG_STORAGE_TOPIC=my-connect-configs -e OFFSET_STORAGE_TOPIC=my-connect-offsets -e STATUS_STORAGE_TOPIC=my_connect_statuses -e BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS -e KAFKA_VERSION=2.6.2 -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=2 -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=2 -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=2 --link mysql:mysql debezium/connect:1.8.0.Final

# Read the messages in the connection topic: my-connect-configs
./kafka-console-consumer.sh  --bootstrap-server $BOOTSTRAP_SERVERS --topic my-connect-configs --from-beginning

# Create a connector so that nifi mysql are sending messages to kafka in MSK
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "bus-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "root", "database.password": "debezium", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "demo", "database.history.kafka.bootstrap.servers": "'"$BOOTSTRAP_SERVERS"'", "database.history.kafka.topic": "dbhistory.demo" } }'

 ./kafka-topics.sh --list --zookeeper $ZOOKEEPER_CONNECTION_STRING

# read incoming messages on dbserver1.demo.bus_status
./kafka-console-consumer.sh  --bootstrap-server $BOOTSTRAP_SERVERS  --topic dbserver1.demo.bus_status  --from-beginning

```

Let's set up the EMR Cluster: https://ca-central-1.console.aws.amazon.com/elasticmapreduce/home?region=ca-central-1#
Use screenshots - For the initial setup.
Once we are setup, navigate the EMR IAM Role. eg: EMR_EC2_DefaultRole and make sure it has AmazonS3FullAccess permission.



We need to prep the python code, see the Notes in the pyspark_job.py file for the changes.
1. val kafkaReaderConfig = KafkaReaderConfig("*BootstrapConnectString*", "dbserver1.demo.bus_status")
2. val schemas  = get_schema("s3://'<your-s3-bucket>'/bus_status.json")
- From the vm, copy the file over
  - `aws s3 cp bus_status_schema.json s3://bus-service-wcd-eu-west-1/msk/bus_status_schema.json`
3. new StreamingJobExecutor(spark, kafkaReaderConfig, "s3://'<your-s3-bucket>'/checkpoint/job", schemas).execute()
4. .save("s3://'<your-s3-bucket>'/hudi/bus_status_bootcamp3")

5. copy the python file over to s3 bucket: s3://'<your-s3-bucket>'/jars/pyspark_job.py
`AWS_PROFILE=indrani aws s3 cp EMR-setup/pyspark_job.py s3://bus-service-wcd-eu-west-1/msk/jars/pyspark_job.py `

```bash
# SSH into the emr-master

# Make sure that you add ssh access for your IP to the security group associated with ElasticMapReduce-master
# To find this easily, you can look in the Summary tab of your EMR, then in the Security and Access, you can find the link to the security group associated with ElasticMapReduce-master
ssh -i 01-setup-ec-vm/nifi-ec-vm.pem hadoop@ec2-3-99-238-134.ca-central-1.compute.amazonaws.com

#  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1
# --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.1

pyspark --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --conf 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'

spark-submit --master yarn --deploy-mode client --name wcd-streaming-app --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" s3://bus-service-wcd-eu-west-1/msk/jars/pyspark_job.py

spark-submit --master yarn --deploy-mode cluster --name wcd-stremaing-app --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" s3://bus-service-wcd-eu-west-1/msk/jars/pyspark_job.py

```


## Verify links

[ec2-user@ip-172-31-7-14 ~]$ docker inspect -f "{{ .HostConfig.Links }}"  kafka
[/zookeeper:/kafka/zookeeper]
[ec2-user@ip-172-31-7-14 ~]$ docker inspect -f "{{ .HostConfig.Links }}"  connect
[/zookeeper:/connect/zookeeper /kafka:/connect/kafka /mysql:/connect/mysql]
[ec2-user@ip-172-31-7-14 ~]$ docker inspect -f "{{ .HostConfig.Links }}"  nifi
[/mysql:/nifi/mysql]
[ec2-user@ip-172-31-7-14 ~]$ docker inspect -f "{{ .HostConfig.Links }}"  mysql
[]
[ec2-user@ip-172-31-7-14 ~]$ docker inspect -f "{{ .HostConfig.Links }}"  spark-master
[/kafka:/spark-master/kafka]
[ec2-user@ip-172-31-7-14 ~]$ docker inspect -f "{{ .HostConfig.Links }}"  spark-worker
[/kafka:/spark-worker/kafka]

- We are telling docker that the three images need to connect to each other using --link
- debezium/connect helps with the configuration so that the three docker instances can communicate with each other
- since --link is an older way of doing things, we should be using a docker network that the three docker instances are part of. ?????
- GROUP_ID is env variable required from debezium connect.

`curl -H "Accept:application/json" http://localhost:8083`
{
"message":"Not Found",
"url":"/",
"status":"404"
`curl -H "Accept:application/json" localhost:8083/connectors/`

We will have no connectors at the start. We need to set up the mysql connector. With the config below:



```bash

#This command creates teh bus connector so that it is looking at the right db , uses the mysql.
#Whenever data arrives in demo tables, its pushing to kafka
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "bus-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "root", "database.password": "debezium", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "demo", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.demo" } }'

curl -H "Accept:application/json" localhost:8083/connectors/ This will give ["bus-connector"]
curl -H "Accept:application/json" localhost:8083/connectors/bus-connector

`docker exec -it CONTAINER_ID bash`
# run from inside the kafka container id.
bin/kafka-topics.sh --list --zookeeper zookeeper:2181 # Check which topics are available. We should see dbserver1, dbhistory.demo and dbserver1.demo.bus_status

bin/kafka-console-consumer.sh --topic dbserver1.demo.bus_status --bootstrap-server CONTAINER_ID:9092 # Let's check that we are receiving data. CONTAINER_ID corresponds to the kafka container id (You can get this from docker ps)

docker run -dit -p 8555:8080 -v /home/ec2-user:/jars -p 7077:7077 -e INIT_DEAMON_STEP=setup_spark --link kafka:kafka --name spark-master bde2020/spark-master:3.2.0-hadoop3.2 # Master

docker run -dit -p 8081:8081 -v /home/ec2-user:/jars  -e  SPARK_MASTER=spark://spark-master:7077 --link kafka:kafka --name spark-worker bde2020/spark-master:3.2.0-hadoop3.2 # Worker
```

## pyspark
# The following commands are to be run from spark master
```bash
spark/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:2.8.1

```

Referencing: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

```scala

val kafkaReaderConfig = KafkaReaderConfig("kafka:9092","dbserver1.demo.bus_status")
val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaReaderConfig.kafkaBootstrapServers)
      .option("subscribe", kafkaReaderConfig.topics)
      .option("startingOffsets", kafkaReaderConfig.startingOffsets)
      .load()
```

```python
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "dbserver1.demo.bus_status") \
  .load()

df.printSchema()

e
```

```bash
# from the ec-vm
touch bus_status_schema.json
vi bus_status_schema.json
docker cp bus_status_schema.json spark-master:/jar
```

```python
# docker exec -it SPARK_MASTER_CONTAINER_ID bash
# spark/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:2.8.1




from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = spark.read.json('file:///jars/bus_status_schema.json').schema

df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "dbserver1.demo.bus_status") \
      .option("startingOffsets", "latest") \
      .load()


# we cast the original `value` from Kafka stream, which is in binary format into a string.
# we then select the string and convert it into our schema
# we store it back in the df (transform_df) as the jsonData column
# then we select the path: jsonData.payload.after.*

transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

# Let's stream the data
transform_df.writeStream.option("checkpointLocation", "file:///checkpoint/sparkjob").format("console").option("truncate", "false").start().awaitTermination()

```



## IAM Role for EC2 -> S3

EC2 Actions -> Security -> Modify IAM Role -> Create new IAM Role
Go to Roles ( left side panel)
Create New Role -> AWS Service -> Common Use Cases EC2
From the Policies -> Select S3FullAccess
Name the role and create it. (eg. AmazonS3FullAccessEC2)

Attaching a role will not be enough if you want the docker instances to talk to the s3. Let's create a new user with `Access Key - Programmatic access`. Attach an existing policy - S3FullAccess. 
Once the account is created, you will be shown the AccessKey and Secret.

https://hudi.apache.org/docs/s3_hoodie/
https://spark.apache.org/docs/latest/configuration.html#inheriting-hadoop-cluster-configuration

Go to Spark container:

Craete the folder /etc/hadoop/conf if it does not exist
We will need to set s3 credentials in `core-site.xml`. This should be located in `/etc/hadoop/conf` and `spark-env.sh` needs to know that HADOOP_CONF_DIR=/etc/hadoop/conf
Be sure to add the AWS Credentials in core-site.xml

`spark-env.sh.template` file can be found in `/spark/conf`. You must copy this and rename the file to `spark-env.sh` and in the file, make sure to add this line HADOOP_CONF_DIR=/etc/hadoop/conf



## With Hudi

https://hudi.apache.org/docs/s3_hoodie/
https://aws.amazon.com/blogs/aws/new-insert-update-delete-data-on-s3-with-amazon-emr-and-apache-hudi/
https://stackoverflow.com/questions/71375432/how-to-connect-s3-to-pyspark-on-local-org-apache-hadoop-fs-unsupportedfilesyste

```python
# docker exec -it SPARK_MASTER_CONTAINER_ID bash

# spark/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:2.8.1,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.1,com.amazonaws:aws-java-sdk:1.12.334,org.apache.hadoop:hadoop-aws:3.3.1 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'

# Be sure to use the correct version of hadoop:hadoop-aws. You have to see the startup output to identify which hadoop-common jar we are using. Both versions have to be the same.

# To find this information: 
# run `spark/bin/pyspark`
# then -> look for the following

# Look for the lines:
#         found org.apache.hadoop#hadoop-client-runtime;3.3.1 in central
#         found org.apache.hadoop#hadoop-client-api;3.3.1 in central

# make sure your package versions are matching hadoop-client-api and hadoop-client-runtime, in this case 3.3.1

# Be sure to use eu-west-1; Other regions may work, but some regions like ca-central-1 will not work.
# Be sure to pass --conf 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' so that spark knows how to put files in to s3.

from pyspark.sql.types import *
from pyspark.sql.functions import *

# specifically mentioning file here because otherwise it will think s3 is where it will read from. Other examples are file, http, https, s3, wss
schema = spark.read.json('file:///jars/bus_status_schema.json').schema 
df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "dbserver1.demo.bus_status") \
      .option("startingOffsets", "latest") \
      .load()

# we cast the original `value` from Kafka stream, which is in binary format into a string.
# we then select the string and convert it into our schema
# we store it back in the df (transform_df) as the jsonData column
# then we select the path: jsonData.payload.after.*

transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

# Let's stream the data
checkpoint_location = "file:///checkpoint/sparkjob"
table_name = 'bus_status'
hudi_options = {
    'hoodie.table.name': table_name,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    'hoodie.datasource.write.recordkey.field': 'record_id',
    'hoodie.datasource.write.partitionpath.field': 'routeId',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'event_time',
    'hoodie.upsert.shuffle.parallelism': 100,
    'hoodie.insert.shuffle.parallelism': 100
}
s3_path = "s3a://bus-service-wcd-eu-west-1/routes"



# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html
def write_batch(batch_df, batch_id):
      batch_df.write.format("org.apache.hudi") \
      .options(**hudi_options) \
      .mode("append") \
      .save(s3_path)

transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("wcd-bus-streaming").foreachBatch(write_batch).start().awaitTermination()





# 22/11/04 18:20:53 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
# 22/11/04 18:20:54 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
# 22/11/04 18:20:59 WARN DFSPropertiesConfiguration: Cannot find HUDI_CONF_DIR, please set it as the dir of hudi-defaults.conf
# 22/11/04 18:20:59 WARN DFSPropertiesConfiguration: Properties file file:/etc/hudi/conf/hudi-defaults.conf not found. Ignoring to load props file
# 22/11/04 18:21:12 WARN HoodieBackedTableMetadata: Metadata table was not found at path s3a://bus-service-wcd-eu-west-1/test/.hoodie/metadata
# 22/11/04 18:21:24 WARN S3ABlockOutputStream: Application invoked the Syncable API against stream writing to test/.hoodie/metadata/files/.files-0000_00000000000000.log.1_0-0-0. This is unsupported
# # WARNING: Unable to get Instrumentation. Dynamic Attach failed. You may add this JAR as -javaagent manually, or supply -Djdk.attach.allowAttachSelf
# # WARNING: Unable to attach Serviceability Agent. Unable to attach even with module exceptions: [org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed., org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed., org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed.]
# 22/11/04 18:21:38 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-hbase.properties,hadoop-metrics2.properties




```

## Athena

Query Editor -> Create Table -> Create Database if necessary
Bulk add columns for your table: 
`record_id int, id int, routeId int, directionId string, kph int, predictable int, secsSinceReport int, heading int, lat double, lon double, leadingVehicleId int, event_time date`

You will get the error:
No output location provided. An output location is required either through the Workgroup result configuration setting or as an API input.

To fix it: 
You have to go to Athena settings and specify an output path.


`select * from "bus-service-wcd-eu-west-1"."routes" limit 100`

```sql
record_id INT NOT NULL AUTO_INCREMENT,
id INT NOT NULL,
routeId INT NOT NULL,
directionId VARCHAR(40),
kph INT NOT NULL,
predictable BOOLEAN, ====> int
secsSinceReport INT NOT NULL,
heading INT,
lat REAL NOT NULL ====>  double
lon REAL NOT NULL ====> double
leadingVehicleId INT,
event_time DATETIME DEFAULT NOW(),
```

## Spark-Submit

- In order to submit a python file as a job. We need to create a file with the code below in the EC2 VM instance. Then copy it over using `docker cp` command into the `spark-master` docker instance.


```py
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
      from pyspark.sql.types import *
      from pyspark.sql.functions import *
      spark = SparkSession.builder.master("local[1]") \
                    .appName('bus-service') \
                    .getOrCreate()


      schema = spark.read.json('file:///jars/bus_status_schema.json').schema 
      df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "dbserver1.demo.bus_status") \
            .option("startingOffsets", "latest") \
            .load()

      transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

      checkpoint_location = "file:///checkpoint/sparkjob"
      table_name = 'bus_status'
      hudi_options = {
      'hoodie.table.name': table_name,
      "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
      'hoodie.datasource.write.recordkey.field': 'record_id',
      'hoodie.datasource.write.partitionpath.field': 'routeId',
      'hoodie.datasource.write.table.name': table_name,
      'hoodie.datasource.write.operation': 'upsert',
      'hoodie.datasource.write.precombine.field': 'event_time',
      'hoodie.upsert.shuffle.parallelism': 100,
      'hoodie.insert.shuffle.parallelism': 100
      }
      s3_path = "s3a://bus-service-wcd-eu-west-1/routes"

      def write_batch(batch_df, batch_id):
            batch_df.write.format("org.apache.hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(s3_path)

      transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("wcd-bus-streaming").foreachBatch(write_batch).start().awaitTermination()

```


```bash
# in the VM, save the above code into pyspark_job.py and copy it into spark-master docker
docker cp pyspark_job.py spark-master:/home

# docker exec into the spark-master and submit the job
# https://spark.apache.org/docs/latest/submitting-applications.html
spark/bin/spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.1,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.12.334 /home/pyspark_job.py
```

## Iceberg

https://iceberg.apache.org/docs/latest/aws/#spark

```py
# spark/bin/pyspark --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:2.8.1,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.1,com.amazonaws:aws-java-sdk:1.12.334,org.apache.hadoop:hadoop-aws:3.3.1 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.my_catalog.warehouse=s3a://bus-service-wcd-eu-west-1/iceberg --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO


from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = spark.read.json('file:///jars/bus_status_schema.json').schema 
df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "dbserver1.demo.bus_status") \
      .option("startingOffsets", "latest") \
      .load()

transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

checkpoint_location = "file:///checkpoint/sparkjob"
table_name = 'bus_status'
s3_path = "s3a://bus-service-wcd-eu-west-1/iceberg"

transform_df.writeStream.format("iceberg").outputMode("append") \
    .option("path", 's3_path') # issue is here somewhere \
    .option("checkpointLocation", checkpoint_location) \
    .start()

```
