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

import java.io.{PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

object QueryTradeAndQuoteJob extends SnappySQLJob {
  var dateString: String = _

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw = new PrintWriter("QueryTradeAndQuoteJob.out")

    Try {
      TradeAndQuoteUtil.queryTables(snc, Some(pw))
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/QueryTradeAndQuoteJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(snc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}
