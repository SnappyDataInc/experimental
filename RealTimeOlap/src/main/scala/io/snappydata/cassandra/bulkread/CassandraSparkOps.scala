package io.snappydata.cassandra.bulkread

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._


/**
  * Created by hemant on 5/5/17.
  */
object CassandraSparkOps {
  def main(args: Array[String]) {

    if (args.length != 5) {
      println("CassandraSparkOps cassandrahost snappydataclusterurl keyspace tableName partitionnByColumn")
      println("CassandraSparkOps localhost localhost:1527 mykeyspace mytable colName")
      return
    }
    val spark: SparkSession = SparkSession
      .builder
      .appName("CassandraSparkOps")
      .master("local[*]")
      .config("spark.cassandra.connection.host", args(0))
      .config("snappydata.Cluster.URL", args(1))
      // sys-disk-dir attribute specifies the directory where persistent data is saved
      .getOrCreate
    val snSession = new SnappySession(spark.sparkContext)
    cassandraOps(snSession, args(2), args(3), args(4))
  }

  def cassandraOps(snSession: SnappySession, keySpace: String, table: String, partitionByCol: String) {

    val df1 = snSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keySpace))
      .load()
    df1.show()
    df1.printSchema()
    val snappyTbl = s"snappy_${table}"
    snSession.dropTable(snappyTbl, true);

    snSession.createTable(snappyTbl, "column", df1.schema, Map("PARTITION_BY" -> partitionByCol), true)
    // Thread.sleep(100000)
    df1.write.insertInto(snappyTbl)
    snSession.sql(s"select * from $snappyTbl").show
    Thread.sleep(1000000)
  }

}
