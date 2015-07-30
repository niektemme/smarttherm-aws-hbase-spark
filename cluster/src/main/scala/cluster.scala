/*
 * SMART THERMOSTAT - Cloud part
 * Niek Temme
 * Documentation: http://niektemme.com/@@
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

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


object clustertemp {

	//used for HBase scan opperation from spark hadoop api
	def convertScanToString(scan: Scan) = {
		val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
		Base64.encodeBytes(proto.toByteArray)
	}

	def main(args: Array[String]) {
		//spark settings
		val sparkconf = new SparkConf().setAppName("tempcluster")
		val sc = new SparkContext(sparkconf)

		//scan hbase used scenario table to collect used scenarios that have been scored
		val confonoff = HBaseConfiguration.create()
		val scanonoff = new Scan()
		scanonoff.addColumn("fd".getBytes(), "outtempdif".getBytes())
		scanonoff.addColumn("fd".getBytes(), "tempdif".getBytes())
		scanonoff.addColumn("fd".getBytes(), "score".getBytes())
		scanonoff.addColumn("fd".getBytes(), "scenariokey".getBytes())
		scanonoff.setStopRow("40b5af01_9999999999_40b5af01_9999999999_9999999".getBytes())
		confonoff.set(TableInputFormat.INPUT_TABLE, "husedscenario")
		confonoff.set(TableInputFormat.SCAN, convertScanToString(scanonoff))

		//create RDD with used scenarios
		val RDDonOff = sc.newAPIHadoopRDD(confonoff, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
		val valRowsOnOff = RDDonOff.map(tuple => tuple._2
			).filter(result =>  Bytes.toString(result.getValue("fd".getBytes(), "score".getBytes())) != null //only select scored scenarios
			).map(result =>  ( Bytes.toString(result.getValue("fd".getBytes(), "scenariokey".getBytes())), (  Bytes.toString(result.getValue("fd".getBytes(), "outtempdif".getBytes())).toInt , Bytes.toString(result.getValue("fd".getBytes(), "tempdif".getBytes())).toInt    , Bytes.toString(result.getValue("fd".getBytes(), "score".getBytes())).toLong  )  )
			)
			valRowsOnOff.cache()
	
			val RDDscore = valRowsOnOff.map(result => result._2).map(result => Vectors.dense(result._1,result._2))
			RDDscore.cache()

			//use mllib k-means clustering to create clusters (temperature groups)
			val numClusters = 10
			val numIterations = 200
			val clusters = KMeans.train(RDDscore, numClusters, numIterations)

			//select best scenario for each temperature cluster
			val RDDtotalClustered = valRowsOnOff.map(result => ( (result._1, clusters.predict(Vectors.dense(result._2._1,result._2._2)) ), (result._2._3, 1L) )  
			).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)
			).map(result => (result._1._2, ( result._2._1/result._2._2, result._1._1  ) ) 
			).reduceByKey((x, y) => if (x._1 < y._1) {(x._1, x._2)} else {(y._1, y._2)} 
			).mapValues(result => (math.round(result._1), result._2  )) 
			RDDtotalClustered.cache()

			//alternative scnearios get highest score
			val maxscore : Int = ((RDDtotalClustered.map(result=> (result._2._1,1)).sortByKey(false).take(1))(0))._1 + 10
		
			//determine cluster centers (centers are new tempdif and outtempdif)
			val RDDcenters = sc.parallelize(clusters.clusterCenters) 
			val RDDcentersCalc = RDDcenters.map(result => (clusters.predict(result) , result ) )
			RDDcentersCalc.cache()
			
			//join best scenarios for temperature groups with cluster centers
			val RDDClusterWithCenters = RDDtotalClustered.join(RDDcentersCalc
				).map(result => (result._2._1._2, (result._1, result._2._1._1, math.round(result._2._2(0)), math.round(result._2._2(1)) ) ))
			RDDClusterWithCenters.cache()
			
		
			//scan scenario hbase table te retreive scenario information
			val bvepoch: Long = 9999999999L
			val vepoch: Long = System.currentTimeMillis / 1000
			val startmepoch : Long = bvepoch - vepoch
			val vstartrow: String = "40b5af01_"+ startmepoch.toString
						
			val confscenvals = HBaseConfiguration.create()
			val scanscenvals = new Scan()
	
			confscenvals.set(TableInputFormat.INPUT_TABLE, "hactscenario")
			confscenvals.set(TableInputFormat.SCAN, convertScanToString(scanscenvals))
			
			val RDDconfscenvals = sc.newAPIHadoopRDD(confscenvals, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
			val RDDscenvals = RDDconfscenvals.map(tuple => tuple._2
				).map(result => ( Bytes.toString(result.getRow()) ,  
						(
							(Bytes.toString(result.getValue("fd".getBytes(), "run0".getBytes())) , 
							Bytes.toString(result.getValue("fd".getBytes(), "run1".getBytes())), 
							Bytes.toString(result.getValue("fd".getBytes(), "run2".getBytes())), 
							Bytes.toString(result.getValue("fd".getBytes(), "run3".getBytes())) , 
							Bytes.toString(result.getValue("fd".getBytes(), "run4".getBytes())), 
							Bytes.toString(result.getValue("fd".getBytes(), "run5".getBytes())) 
							), 
							(Bytes.toString(result.getValue("fd".getBytes(), "group".getBytes())),
							maxscore,
							Bytes.toString(result.getValue("fd".getBytes(), "outtempdif".getBytes())),	
							Bytes.toString(result.getValue("fd".getBytes(), "tempdif".getBytes()))	
							)
						)  
					) 
				)
			RDDscenvals.cache()
		
			//join newly created scenarios with all other scenarios to create complet list of scenarios (best and alternative)
			//collect all to a new array
			val ClusterResSet = RDDscenvals.leftOuterJoin(RDDClusterWithCenters
				).mapValues(result => if (result._2 == None) { (result._1._1,result._1._2) } else {(result._1._1,(result._2).get)} 
				).map(result => (vstartrow+result._1.substring(19,27), result._2 ) ).collect()
		
			
			//insert new scenarios with new version (ipeoch) to hactscenario table
			val confputscen = HBaseConfiguration.create()
			val tableputscen = new HTable(confputscen,"hactscenario")
			
			for (i <- 0 until ClusterResSet.size) {
				val putrun = new Put((ClusterResSet(i)._1).getBytes())
				putrun.add("fd".getBytes(), "group".getBytes(), (ClusterResSet(i)._2._2._1).toString.getBytes() ) 
				putrun.add("fd".getBytes(), "score".getBytes(), (ClusterResSet(i)._2._2._2).toString.getBytes() )	
				putrun.add("fd".getBytes(), "outtempdif".getBytes(), (ClusterResSet(i)._2._2._3).toString.getBytes() )	
				putrun.add("fd".getBytes(), "tempdif".getBytes(), (ClusterResSet(i)._2._2._4).toString.getBytes() )	
				putrun.add("fd".getBytes(), "iepoch".getBytes(), vepoch.toString.getBytes() )	
				putrun.add("fd".getBytes(), "run0".getBytes(), (ClusterResSet(i)._2._1._1).toString.getBytes() ) 
				putrun.add("fd".getBytes(), "run1".getBytes(), (ClusterResSet(i)._2._1._2).toString.getBytes() ) 
				putrun.add("fd".getBytes(), "run2".getBytes(), (ClusterResSet(i)._2._1._3).toString.getBytes() ) 
				putrun.add("fd".getBytes(), "run3".getBytes(), (ClusterResSet(i)._2._1._4).toString.getBytes() ) 
				putrun.add("fd".getBytes(), "run4".getBytes(), (ClusterResSet(i)._2._1._5).toString.getBytes() ) 
				putrun.add("fd".getBytes(), "run5".getBytes(), (ClusterResSet(i)._2._1._6).toString.getBytes() ) 
				
				tableputscen.put(putrun)
			}
			
			println("done")
			
			tableputscen.close()

		}

}

