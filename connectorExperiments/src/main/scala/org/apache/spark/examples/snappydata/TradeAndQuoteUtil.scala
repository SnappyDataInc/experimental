/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.examples.snappydata

import java.io.PrintWriter

import org.apache.spark.sql._


object TradeAndQuoteUtil {

  val tradeTable = "TRADES"
  val quoteTable = "QUOTES"
  val stagingTradeTable = "STAGING_TRADES"
  val stagingQuoteTable = "STAGING_QUOTES"
  val symbolsTable = "SYMBOLS"
  val stagingSymbolsTable = "STAGING_SYMBOLS"

  def createTables(snc: SnappySession,
                   provider: String,
                   path: String,
                   pw: Option[PrintWriter] = None): Any = {

    // Drop tables if already exists
    snc.dropTable(tradeTable, ifExists = true)
    snc.dropTable(quoteTable, ifExists = true)
    snc.dropTable(stagingTradeTable, ifExists = true)
    snc.dropTable(stagingQuoteTable, ifExists = true)

    // Create a DF from the parquet data file and make it a table
    val tradeDF = snc.catalog.createExternalTable(stagingTradeTable, "parquet",
      Map("path" -> s"$path/trades"))
    val quoteDF = snc.catalog.createExternalTable(stagingQuoteTable, "parquet",
      Map("path" -> s"$path/quotes"))
    val symbolDF = snc.catalog.createExternalTable(stagingSymbolsTable, "parquet",
      Map("path" -> s"$path/symbols"))

    // Create tables in Snappy
    snc.createTable(tradeTable, provider,
      tradeDF.schema, Map("PARTITION_BY" -> "sym", "persistent" -> "SYNCHRONOUS"))
    snc.createTable(quoteTable, provider,
      quoteDF.schema, Map("PARTITION_BY" -> "sym", "persistent" -> "SYNCHRONOUS"))

    // symbols will be a replicated table, always
    snc.createTable(symbolsTable, "row",
      symbolDF.schema, Map("persistent" -> "SYNCHRONOUS"))

    // Load the tables and note the time taken
    val t1 = System.currentTimeMillis()
    tradeDF.write.mode(SaveMode.Append).saveAsTable(tradeTable)
    val t2 = System.currentTimeMillis()
    quoteDF.write.mode(SaveMode.Append).saveAsTable(quoteTable)
    val t3 = System.currentTimeMillis()

    val printString = s"Load time- Trades: ${(t2 - t1)} millis. Quotes: ${t3 - t2} millis."

    pw match {
      case Some(w) => w.println(printString)
      case _ => System.out.println(printString)
    }
  }
  private val dateString = "2016-06-06"
  def queryTables(snc: SnappySession,
                  pw: Option[PrintWriter] = None): Unit = {
    // scala example: Is it needed?
    //    val tradeDF: DataFrame = snc.table(tradeTable)
    //    val quoteDF: DataFrame = snc.table(quoteTable)
    //    val actualResult = tradeDF.join(quoteDF,
    //      tradeDF.col("sym").equalTo(quoteDF("sym"))).
    //      groupBy(tradeDF("sym")).
    //      agg("bid" -> "max")

    val queries = Array(
      "select quotes.sym, last(bid) from quotes join S " +
        s"on (quotes.sym = S.sym) where date='$dateString' group by quotes.sym",
      "select trades.sym, ex, last(price) from trades join S " +
        s"on (trades.sym = S.sym) where date='$dateString' group by trades.sym, ex",
      "select trades.sym, hour(time), avg(size) from trades join S " +
        s"on (trades.sym = S.sym) where date='$dateString' group by trades.sym, hour(time)"
    )

    val printString = queries.zipWithIndex.map {
      case (q: String, index: Int) =>
        val t1 = System.currentTimeMillis()
        snc.sql(q).collect()
        val t2 = System.currentTimeMillis()
        s"Query $index- Time taken: ${t2-t1}"
    }.mkString("\n")

    pw match {
      case Some(w) => w.println(printString)
      case _ => System.out.println(printString)
    }
  }
}
