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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{Decimal, StringType, StructField, StructType}

object TPCETradeDataGenerator {

  def main (args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("DataGenerator")
      .getOrCreate

    val datasize =  args(0).toLong
    // calculate number of rows from data sizes.
    val quoteSize:Long = datasize * 16320000
    val tradeSize:Long = datasize * 2400000

    val path = args(1)

    val provider = if (args.size == 2) "parquet" else args(2)

    val EXCHANGES: Array[String] = Array("NYSE", "NASDAQ", "AMEX", "TSE",
      "LON", "BSE", "BER", "EPA", "TYO")

    val ALL_SYMBOLS: Array[String] = {
      val syms = new Array[String](400)
      for (i <- 0 until 10) {
        syms(i) = s"SY0$i"
      }
      for (i <- 10 until 100) {
        syms(i) = s"SY$i"
      }
      for (i <- 100 until 400) {
        syms(i) = s"S$i"
      }
      syms
    }
    val SYMBOLS: Array[String] = ALL_SYMBOLS.take(100)
    val numDays = 1
    import spark.implicits._
    val sDF = spark.createDataset(SYMBOLS)

    val quoteDF = spark.range(0,quoteSize,1,200).mapPartitions { itr =>
      val rnd = new java.util.Random()
      val syms = ALL_SYMBOLS
      val numSyms = syms.length
      val exs = EXCHANGES
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(rnd.nextInt(numSyms))
        val ex = exs(rnd.nextInt(numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            day = (day + 1) % numDays
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        cal.set(Calendar.HOUR, rnd.nextInt(8))
        cal.set(Calendar.MINUTE, rnd.nextInt(60))
        cal.set(Calendar.SECOND, rnd.nextInt(60))
        cal.set(Calendar.MILLISECOND, rnd.nextInt(1000))
        val time = new Timestamp(cal.getTimeInMillis)
        val bid=rnd.nextDouble() * 100000
        Quote(sym, ex, bid, new SimpleDateFormat("HH:mm:ss.SSS").format(time).toString, date.toString)
      }
    }
    quoteDF.write.format(s"$provider").save(s"$path/quotes")

    val tradeDF = spark.range(0,tradeSize,1,200).mapPartitions { itr =>
      val rnd = new java.util.Random()
      val syms = ALL_SYMBOLS
      val numSyms = syms.length
      val exs = EXCHANGES
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(rnd.nextInt(numSyms))
        val ex = exs(rnd.nextInt(numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            // change date
            day = (day + 1) % numDays
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        cal.set(Calendar.HOUR, rnd.nextInt(8))
        cal.set(Calendar.MINUTE, rnd.nextInt(60))
        cal.set(Calendar.SECOND, rnd.nextInt(60))
        cal.set(Calendar.MILLISECOND, rnd.nextInt(1000))
        val time = new Timestamp(cal.getTimeInMillis)
        val dec = Decimal(rnd.nextInt(100000000), 10, 4).toString
        val size=rnd.nextDouble() * 1000
        Trade(sym, ex, dec, new SimpleDateFormat("HH:mm:ss.SSS").format(time).toString, date.toString, size)
      }
    }
    tradeDF.write.format(s"$provider").save(s"$path/trades")
    sDF.write.format(s"$provider").save(s"$path/symbols")
  }

  case class Quote(sym: String, ex: String, bid: Double, time: String,
                   date: String)

  case class Trade(sym: String, ex: String, price: String, time: String,
                   date: String, size: Double)
}
