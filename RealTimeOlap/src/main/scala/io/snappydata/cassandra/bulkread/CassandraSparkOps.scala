package io.snappydata.cassandra.bulkread

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{SnappySession, SparkSession}


/**
  * Created by hemant on 5/5/17.
  */
object CassandraSparkOps {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession
      .builder
      .appName("CollocatedJoinExample")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("snappydata.store.locators", "localhost:10334")
      // sys-disk-dir attribute specifies the directory where persistent data is saved
      .getOrCreate
    val snSession = new SnappySession(spark.sparkContext)
    val sc = spark.sparkContext
    import com.datastax.spark.connector._ //Loads implicit functions
    val df = sc.cassandraTable("initkey", "foo1")
    val df1 = snSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "foowithint", "keyspace" -> "initkey" ))
      .load()
    df1.show()

    snSession.dropTable("foo1_snappy", true);

    snSession.createTable("foo1_snappy", "column", df1.schema, Map("PARTITION_BY" -> "a"), true)
    // Thread.sleep(100000)
    df1.write.insertInto("foo1_snappy")
    snSession.sql("select a from foo1_snappy ").show

    Thread.sleep(1000000)


  }

}
