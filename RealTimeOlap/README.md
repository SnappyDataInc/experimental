## Build the project
Use the following command to build this application: 
 
$./gradlew clean build 

To test this application:

#### Start a cassandra server
```
bin/cassandra 
```
#### If you have csv data, you can skip this step. If you have parquet data, convert it to csv using spark-shell: 
```
./bin/spark-shell 
val df = spark.read.parquet("/nfs/users/hemant/snappydata/build-artifacts/scala-2.11/snappy/quickstart/data/airlineParquetData/")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mydata.csv")
```
#### Create a key space in cassandra using the following code
```
CREATE KEYSPACE initkey WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```
#### Create a table in cassandra
```
bin/cqlsh> use initkey;
bin/cqlsh:initkey>CREATE TABLE airline (Year int,Month int,DayOfMonth int,DayOfWeek int,DepTime int,CRSDepTime int, ArrTime int,CRSArrTime int, UniqueCarrier text ,FlightNum int,TailNum text,ActualElapsedTime int, CRSElapsedTime int,AirTime int,ArrDelay int,DepDelay int,Origin text,Dest text,Distance int ,TaxiIn int, TaxiOut  int,Cancelled int ,CancellationCode  text,Diverted int ,CarrierDelay int , WeatherDelay int,NASDelay int,SecurityDelay int,LateAircraftDelay int, ArrDelaySlot int, Primary Key(Year,Month,DayOfMonth,DepTime,UniqueCarrier)) WITH cdc=true;
```
#### Import the csv data into Cassandra's table using Cassandra's copy utility
```
bin/cqlsh:initkey> copy airline (Year ,Month ,DayOfMonth ,DayOfWeek ,DepTime ,CRSDepTime , ArrTime ,CRSArrTime , UniqueCarrier ,FlightNum ,TailNum ,ActualElapsedTime , CRSElapsedTime ,AirTime ,ArrDelay ,DepDelay ,Origin ,Dest ,Distance ,TaxiIn , TaxiOut  ,Cancelled ,CancellationCode  ,Diverted ,CarrierDelay , WeatherDelay ,NASDelay ,SecurityDelay ,LateAircraftDelay , ArrDelaySlot)  from '/nfs/users/hemant/data/airline.csv' with HEADER=TRUE 
```
#### Create the conf/leads and conf/servers with the following jars as the classpath 
conf/leads - Specify the cassandra host and the jars 
```
localhost -spark.cassandra.connection.host=localhost -classpath=/home/hemant/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.0.1/20f57d120023d21b802429dc58131090e5d199ee/spark-cassandra-connector_2.11-2.0.1.jar:/home/hemant/.gradle/caches/modules-2/files-2.1/org.apache.cassandra/cassandra-all/3.10/78dcef53ddff978613b0f864c3a73ddd61a34535/cassandra-all-3.10.jar:/home/hemant/.gradle/caches/modules-2/files-2.1/com.datastax.cassandra/cassandra-driver-core/3.2.0/65ffab45202c0830a37140dbc22c021b8a269c0/cassandra-driver-core-3.2.0.jar:/home/hemant/.gradle/caches/modules-2/files-2.1/org.apache.cassandra/cassandra-thrift/3.10/8ebd08d5a479420aefe1e17754cc2fb64484c39b/cassandra-thrift-3.10.jar
```
conf/servers - Specify the cassandra jars 
```
localhost -classpath=/home/hemant/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.0.1/20f57d120023d21b802429dc58131090e5d199ee/spark-cassandra-connector_2.11-2.0.1.jar:/home/hemant/.gradle/caches/modules-2/files-2.1/org.apache.cassandra/cassandra-all/3.10/78dcef53ddff978613b0f864c3a73ddd61a34535/cassandra-all-3.10.jar:/home/hemant/.gradle/caches/modules-2/files-2.1/com.datastax.cassandra/cassandra-driver-core/3.2.0/65ffab45202c0830a37140dbc22c021b8a269c0/cassandra-driver-core-3.2.0.jar:/home/hemant/.gradle/caches/modules-2/files-2.1/org.apache.cassandra/cassandra-thrift/3.10/8ebd08d5a479420aefe1e17754cc2fb64484c39b/cassandra-thrift-3.10.jar
{code }
Start the snappy leads and servers 
#### Create Cassandra's table as an external table
```
bin/snappy-shell> create external table staging_airline using org.apache.spark.sql.cassandra options (table 'airline', keyspace 'initkey') 
```
#### Import Cassandra's table in Snappy 
```
create table snappy_airline using column options () AS (select * from staging_airline) 
```
