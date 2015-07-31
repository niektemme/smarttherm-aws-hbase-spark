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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._

object scoretemp {
	
	//used for HBase scan opperation from spark hadoop api
	def convertScanToString(scan: Scan) = {
		val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
		Base64.encodeBytes(proto.toByteArray)
	}


	def main(args: Array[String]) {
		//spark settings
		val sparkconf = new SparkConf().setAppName("tempscore") 
		val sc = new SparkContext(sparkconf)

		//get scenario information from husedscenario table
		val confmrscen = HBaseConfiguration.create()
		val rowval = args(0).getBytes() //used scenario key is input
	
		val getscenvals = new Get(rowval)
		val tablescenvals = new HTable(confmrscen,"husedscenario")
		val resgetscenvals = tablescenvals.get(getscenvals)
		val valscenariokey = resgetscenvals.getValue("fd".getBytes(), "scenariokey".getBytes())
		val valsettemp : Int = Bytes.toString(resgetscenvals.getValue("fd".getBytes(), "settemp".getBytes())).toInt
		val valrevepoch : Long = Bytes.toString(rowval).substring(9,19).toLong 
		val valmaxrevepoch : Long =  valrevepoch - (60L*60L) //limit to one hour after scenario started

		tablescenvals.close()

		
		//scan hsensval table to retreive information to score scenario part 1: temperature information
		//this scan collects the actual inside temperature sensor data
		val valstartrow : String = "40b5af01_rx000A04_" + valmaxrevepoch.toString + "_9999999"
		val valstoprow : String = "40b5af01_rx000A04_" + valrevepoch.toString + "_9999999"

		val conf = HBaseConfiguration.create()
		val scan = new Scan()
		conf.set(TableInputFormat.INPUT_TABLE, "hsensvals")
		scan.setStartRow(valstartrow.getBytes())
		scan.setStopRow(valstoprow.getBytes())
		conf.set(TableInputFormat.SCAN, convertScanToString(scan))

		val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
		val valRows = hBaseRDD.map(tuple => tuple._2).map(result => (Bytes.toString(result.getRow()).substring(18,28) , Bytes.toString(result.getValue("fd".getBytes(), "cd".getBytes())).toInt ) )
		valRows.cache()

		val testval = (valsettemp*10) //temperature to be reached
		
		var tempReached : Long = 70L*60L
		val valFirstTempR = valRows.filter(r => r._2 >= testval)
		valFirstTempR.cache()
		val valFirstTempRhap : Long = valFirstTempR.count() 
		println("numonovermax")
		println(valFirstTempRhap)
		
		if (valFirstTempRhap > 0L) {
			val valFirstTempRval : Long = (valFirstTempR.sortByKey().take(1))(0)._1.toLong
			println("reached max")
			println(valFirstTempRval)
			tempReached = valrevepoch - valFirstTempRval //after how many seconds set temperature was reached
			}
		
		val mresultmax : Int = ((valRows.map(item => item.swap).sortByKey(false).take(1))(0))._1.toInt
		
		println(valrevepoch)
		println(tempReached)
		println(mresultmax)


		//scan hsensval table to retreive information to score scenario part 2: boiler information
		//this scan collects when the boiler was turned on or off
		val valstartrowonoff : String = "40b5af01_rx000B02_" + valmaxrevepoch.toString + "_9999999"
		val valstoprowonoff : String = "40b5af01_rx000B02_" + valrevepoch.toString + "_9999999"

		val confonoff = HBaseConfiguration.create()
		val scanonoff = new Scan()
		confonoff.set(TableInputFormat.INPUT_TABLE, "hsensvals")
		scanonoff.setStartRow(valstartrowonoff.getBytes())
		scanonoff.setStopRow(valstoprowonoff.getBytes())
		confonoff.set(TableInputFormat.SCAN, convertScanToString(scanonoff))

		val RDDonOff = sc.newAPIHadoopRDD(confonoff, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
		val valRowsOnOff = RDDonOff.map(tuple => tuple._2).map(result => (Bytes.toString(result.getRow()).substring(18,28) , Bytes.toString(result.getValue("fd".getBytes(), "cd".getBytes())).toInt ) )
		valRowsOnOff.cache()

		val rddons = valRowsOnOff.filter(r => r._2 == 1)
		rddons.cache()
		val rddoffs = valRowsOnOff.filter(r => r._2 != 1).map(result => ((result._1.toLong - 1L).toString,result._2))

		//rddoffs.collect().foreach(println)
		//rddons.collect().foreach(println)
		val onoffscomb = rddons.join(rddoffs)
		onoffscomb.cache()

		val numonoffs = onoffscomb.count() //how often the boiler was turned on 
		val numoverfl = onoffscomb.filter(r => r._2._2 == 2).count() //overflow had occurred
		val ontime = rddons.count() //how many seconds boiler was on
		println(numonoffs)
		println(numoverfl)
		println(ontime)

		
		var overflsocre = 0
		if (numoverfl>0) {
			overflsocre = 3600
		}

		//calculate final score
		val fscore = (numonoffs*10*60)+(ontime*2)+((math.abs(mresultmax-testval))*150)+(overflsocre)+(tempReached)

		//put score in used scenario
		val confputscore = HBaseConfiguration.create()
		val tableputscore = new HTable(confputscore,"husedscenario")
		val putscore = new Put(rowval)
		putscore.add("fd".getBytes(), "score".getBytes(),fscore.toString.getBytes())
		tableputscore.put(putscore)
		
		
		//delete used scenario key from index table
		val tabledelrow = new HTable(confputscore,"husedscenariotbsc")
		val useddelete = new Delete(rowval)
		tabledelrow.delete(useddelete)



		tabledelrow.close()
		tableputscore.close()

		sc.stop()

	}
}
