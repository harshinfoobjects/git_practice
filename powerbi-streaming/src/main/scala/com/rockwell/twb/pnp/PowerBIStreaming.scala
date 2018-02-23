package com.rockwell.twb.pnp

import java.io.FileInputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.text.SimpleDateFormat
import java.util.Properties

import scala.collection.mutable.MutableList

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.sql.functions.lit

import com.google.gson.Gson
import com.google.gson.JsonParser
import com.rockwell.twb.stores.MySqlOffsetsStore
import com.rockwell.twb.util.Gateway
import com.rockwell.twb.util.SparkUtils
import org.apache.spark.sql.functions._

import kafka.serializer.StringDecoder
import org.apache.spark.sql.DataFrame

case class machineDetail(machine_name: String, equipment_make: String)

object PowerBIStreaming {
  //  val properties = new Properties()
  val properties = new Properties()
  var lastRefreshTime = -1L;
  var machineDetailBroadcastedDf: Broadcast[DataFrame] = null

  def main(args: Array[String]): Unit = {
    properties.load(new FileInputStream(args(0)))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
      "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))

    val changer = udf(
      (location: String) => {
        val locationArr = location.split(",")
        locationArr(1)
      })

    val hole = udf(
      (location: String) => {
        val locationArr = location.split(",")
        locationArr(2)
      })

    val su = new SparkUtils(properties)
    var ssc: StreamingContext = null
    ssc = su.getSparkStreamingContext()
    val topic = properties.getProperty("inletKafkaTopic")
    val debug = true
    val offsetStore = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, properties.getProperty("jdbcDatabase"))
    val dataStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, topic, "")
    val powerBIApi = dataStream.context.sparkContext.broadcast(properties.getProperty("powerBIApi"))
    val mappedDataStream = dataStream.map(_._2);
    mappedDataStream.foreachRDD { rdd =>
      // Refresh logic
      try {
      val currentTime = java.lang.System.currentTimeMillis()
      val lookupRefreshIntervalMiliSec = properties.getProperty("lookupRefreshIntervalMiliSec").toLong;
      var sdf = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss a z");
      if ((currentTime - lastRefreshTime) > lookupRefreshIntervalMiliSec) {
        if (debug) {
          println("currentTime:" + currentTime + " lastRefreshTime:" + lastRefreshTime + " lookupRefreshIntervalmill:" + lookupRefreshIntervalMiliSec)
          println("(currentTime - lastRefreshTime) :" + (currentTime - lastRefreshTime) + " lookupRefreshIntervalmill :" + lookupRefreshIntervalMiliSec)
        }
        machineDetailBroadcastedDf = dataStream.context.sparkContext.broadcast(refreshMaterialCostTables(properties, dataStream.context.sparkContext))

        val refreshTime = sdf.format(currentTime)
        println("Lookup tables refreshed at --------> " + refreshTime);

        lastRefreshTime = currentTime

      }

      if (!rdd.isEmpty()) {
        println("rdd is not empty")
        val sqlContext = SQLContext.getOrCreate(rdd.context)
        import sqlContext.implicits._
        val rawDf = sqlContext.read.json(rdd)
        val pnpDf = rawDf.filter(rawDf("xml_tag_name").contains(properties.getProperty("ea_pap_nozzle_header")))

        if ((!pnpDf.take(1).isEmpty) && machineDetailBroadcastedDf.value != null) {
          val joineddf = pnpDf.join(machineDetailBroadcastedDf.value, machineDetailBroadcastedDf.value("equipment_make") <=> pnpDf("user_name"), "left")
          val finalDf = joineddf.withColumn("ch_hole", concat(changer(joineddf("location")), lit("-"), hole(joineddf("location"))))
          val newDf = finalDf.withColumn("mach_ch_hole", concat(finalDf("machine_name"), lit("-"), finalDf("ch_hole")))
            .select("ch_hole", "Date_Time", "mach_ch_hole", "machine_name", "ulcompmissing", "ulpicks", "ulplacements", "ulPossibleMissing", "ulRejects")
          if (debug) {
            println("########################### Data to be posted to PowerBI #######################")
            newDf.show()
          }
          dumpToPowerBI(newDf, powerBIApi, debug)

        } else {
          println("####################### No data to Post to PowerBI as no nozzle data recieved #######################")
        }

      }
    } catch {
          case all: Exception =>
          if (all.getCause !=  null && all.getCause.toString().equalsIgnoreCase("kafka.common.OffsetOutOfRangeException")) {
              offsetStore.deleteOffsets(topic)
          } else{
            all.printStackTrace()
          }
        }
    }

    //Saving offsets
    offsetStore.saveOffsets(dataStream, "")

    ssc.start()
    ssc.awaitTermination()
  }

  @throws(classOf[Exception])
  def refreshMaterialCostTables(properties: Properties, sparkContext: SparkContext) = {

    val driverName: String = "org.apache.hive.jdbc.HiveDriver";
    val debug = properties.getProperty("debug").toBoolean;

    try {
      Class.forName(driverName);
    } catch {
      case e: ClassNotFoundException => e.printStackTrace() // TODO:
    }
    val jdbcUrl: String = properties.getProperty("driverPath");
    if (debug) {
      println("Connecting to " + jdbcUrl);
    }

    if (properties.getProperty("secure").toBoolean) {
      System.setProperty("java.security.auth.login.config", properties.getProperty("jaasFile"));
      System.setProperty("sun.security.jgss.debug", "true");
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      System.setProperty("java.security.krb5.conf", properties.getProperty("krbCfg"));
    }

    val con: Connection = DriverManager.getConnection(jdbcUrl);
    if (debug) {
      System.out.println("Connected!");
    }
    val stmt: Statement = con.createStatement();

    var sql = properties.getProperty("query")

    System.out.println("Running: " + sql);

    var res: ResultSet = stmt.executeQuery(sql);
    var machineDetails = MutableList[machineDetail]()
    while (res.next()) {
      val machine_name = res.getString("machine_name")
      val equipment_make = res.getString("equipment_make")
      machineDetails += machineDetail(machine_name, equipment_make)
    }

    val machineDetailsRdd = sparkContext.parallelize(machineDetails)
    var result: DataFrame = null
    val sqlContext = SQLContext.getOrCreate(machineDetailsRdd.context)
    import sqlContext.implicits._

    if (!machineDetailsRdd.isEmpty()) {
      result = machineDetailsRdd.toDF()
    }
    result
  }

  /**
   * calling REST API endpoints for dumping data to PowerBI
   */

  def dumpToPowerBI(dataframe: DataFrame, powerBIApi: Broadcast[String], debug: Boolean) {
    if (debug) {
      println("####################### Posting data to PowerBI #####################")
    }
    if (dataframe != null) {

      val rddArr = dataframe.map {
        x =>
          val machine = if (x.getAs("machine_name") != null) x.getAs("machine_name").toString() else null
          val mach_ch_hole = if (x.getAs("mach_ch_hole") != null) x.getAs("mach_ch_hole").toString() else null
          val jsonBody = """{""" +
            """"ch_hole" : """" + x.getAs("ch_hole").toString() +
            """","date_time" :"""" + x.getAs("Date_Time").toString() +
            """","mach_ch_hole" :"""" + mach_ch_hole +
            """","machine_name" :"""" + machine +
            """","ulcompmissing" :""" + x.getAs("ulcompmissing").toString().toDouble +
            ""","ulpicks" :""" + x.getAs("ulpicks").toString().toDouble +
            ""","ulplacements" :""" + x.getAs("ulplacements").toString().toDouble +
            ""","ulpossiblemissing" :""" + x.getAs("ulPossibleMissing").toString().toDouble +
            ""","ulrejects" :""" + x.getAs("ulRejects").toString().toDouble +
            """}"""
          jsonBody
      }.toArray().mkString(",")

      val postString = """[""" + rddArr + """]"""

      if (debug) {
        println("===================================== Post Body ==================================")
        println(postString)
      }

      val post = new HttpPost(powerBIApi.value.toString())
      post.addHeader("Content-Type", "application/json")
      post.setEntity(new StringEntity(postString))
      val Response = Gateway.getPostContent(post)
      if (!Response.isEmpty() && Response.contains("error")) {
        println("####################### PowerBI API is down! #############################")
      } else {
        println("####################### data posted to powerBI ############################")
      }

    }

  }

}