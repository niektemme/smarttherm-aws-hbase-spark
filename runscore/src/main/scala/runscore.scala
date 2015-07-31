/*
 * SMART THERMOSTAT - Cloud part
 * Niek Temme
 * Documentation: http://niektemme.com/2015/07/31/smart-thermostat/ @@to do
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import scala.sys.process._
import collection.mutable.ArrayBuffer
import org.slf4j.Logger;
import org.slf4j.LoggerFactory

trait Logging {
  def logger: Logger
  def debug(message: String) { logger.debug(message) }
  def warn(message: String) { logger.warn(message) }
}


object runscoretemp  {
	val logger =  LoggerFactory.getLogger(classOf[runscoretemp])
	
	//triggers each spark job through the command line
	def sourceFilesAt(urowVal: String): Stream[String] = {
		val cmd = Seq("/usr/local/spark/bin/spark-submit","--driver-class-path","/usr/local/hbase/lib/hbase-server-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-protocol-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-hadoop2-compat-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-client-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-common-0.98.7-hadoop2.jar:/usr/local/hbase/lib/htrace-core-2.04.jar:/usr/local/hbase/lib/guava-12.0.1.jar","--class","scoretemp","--master","spark://ip-172-31-7-254:7077","/usr/local/spark/tempniek/score/target/score-1.0-SNAPSHOT.jar",urowVal)
		cmd.lines
	}

	def main(args: Array[String]) {
		
		logger.info("runscore")

		//scans used scenario hbase index table for scnearios to be scored
		val bvepoch: Long = 9999999999L
		val vepoch: Long = System.currentTimeMillis / 1000
		val startmepoch : Long = bvepoch - vepoch + (60L*60L)
		logger.info(startmepoch.toString)
		val rowkeys = ArrayBuffer[String]()

		val vstartrow: String = "40b5af01_"+ startmepoch.toString + "_40b5af01_9999999999_9999999"

		val confmrscen = HBaseConfiguration.create()
		val tablemrscen = new HTable(confmrscen,"husedscenariotbsc")
		val scanmrscen = new Scan()

		val scfilter=new SingleColumnValueFilter(Bytes.toBytes("fd"),Bytes.toBytes("cd"),CompareFilter.CompareOp.EQUAL,new SubstringComparator("tbc")) //tbc is default value tb scored
		scanmrscen.setFilter(scfilter)
		scanmrscen.setStartRow(vstartrow.getBytes())
		scanmrscen.setStopRow("40b5af01_9999999999_40b5af01_9999999999_9999999".getBytes())
		scanmrscen.setCaching(100) //caching set for fast results
		val res = tablemrscen.getScanner(scanmrscen)


		var result = res.next()

		//add row key to array
		while (result != null) {
			var rowval = result.getRow()
			rowkeys += Bytes.toString(rowval)
			result = res.next()
		}

		//quickly sets cd cell to running to prevent used scenario to be scanned twice
		for (i <- 0 until rowkeys.size) {
			val putrun = new Put(rowkeys(i).getBytes())
			putrun.add("fd".getBytes(), "cd".getBytes(),"running".getBytes())
			putrun.add("fd".getBytes(), "vepoch".getBytes(),vepoch.toString.getBytes())
			tablemrscen.put(putrun)
		}

		tablemrscen.close()

		//loop trough array for a second time to trigger spark job for row key
		for (i <- 0 until rowkeys.size) {
			logger.info(rowkeys(i))
			sourceFilesAt(rowkeys(i))
		}

	}

}

//class used for logging purposses
class runscoretemp  extends Logging {
  final def logger = runscoretemp.logger
}