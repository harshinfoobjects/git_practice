package com.rockwell.twb

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import com.rockwell.twb.stores.MySqlOffsetsStore
import com.rockwell.twb.util.SparkUtils

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.MutableList
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.ShortType
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DateType
import scala.collection.mutable.MutableList
import org.apache.spark.sql.DataFrame
import scala.xml.XML
import java.util.ArrayList
import scala.xml.Document
import scala.xml.Document
import org.apache.spark.sql.DataFrame
import org.apache.kafka.clients.producer.ProducerRecord
import com.rockwell.twb.kafka.KafkaSink
import org.apache.spark.broadcast.Broadcast
import com.google.gson.Gson
import com.rockwell.twb.util.SparkUtils
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedExceptionAction
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.conf.Configuration
import com.rockwell.twb.kafka.KafkaSink
import com.rockwell.twb.util.HiveUtil
import org.apache.spark.sql.hive.HiveContext
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.Date

object Parser {

  val properties = new Properties()

  @transient private var instance: HiveContext = _

 /*
	* processing kafka direct steam - parsing xml to json objects and send the parsed object on kafka and also save into hive
	* 
	* @param dataStream direct kafka stream
	* 
	* @param ssc spark streaming context
	*  
	*/
  def processStream(dataStream: InputDStream[(String, String)], topic: String, outputTopic: String, kafkaSink: Broadcast[KafkaSink], su: SparkUtils, offsetStore: MySqlOffsetsStore, propertiesBroadcast: Broadcast[Properties]) {
    val mappedDataStream = dataStream.map(_._2);
    mappedDataStream.foreachRDD { rdd =>
      try {
        if (!rdd.isEmpty()) {
          val sparkContext = rdd.sparkContext
          var headerNotFound = true
          var fileName = propertiesBroadcast.value.getProperty("hdfs_file_prefix") + "_" + System.currentTimeMillis();
          val parsedDf = loadXML(rdd, fileName, topic)

          if (propertiesBroadcast.value.getProperty("inletKafkaTopic").equalsIgnoreCase("cgp_kafka_spi_ea_ra")) {
            if (!parsedDf.isEmpty()) {

              val rdd_spi_header = parsedDf.filter {
                ele => ele.contains("spiTest-header")
              }
              if (!rdd_spi_header.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_spi_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_spi_header"), kafkaSink)
                HiveUtil.insertInto_ea_spi_header(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_spi_header), propertiesBroadcast);
              }

              val rdd_spi_detail = parsedDf.filter {
                ele => ele.contains("spiTest-detail")
              }
              if (!rdd_spi_detail.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_spi_detail, propertiesBroadcast.value.getProperty("outletKafkaTopic_spi_detail"), kafkaSink)
                HiveUtil.insertInto_ea_spi_detail(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_spi_detail), propertiesBroadcast);
              }
            }

          } else if (propertiesBroadcast.value.getProperty("inletKafkaTopic").equalsIgnoreCase("cgp_kafka_pap_ea_ra")) {
            if (!parsedDf.isEmpty()) {

              val rdd_pap_timers = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_timers_header"))
              }
              if (!rdd_pap_timers.isEmpty()) {
                headerNotFound = false
                su.produceToKafka(rdd_pap_timers, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_timers_header"), kafkaSink)
                HiveUtil.insertInto_ea_pap_timers(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_pap_timers), propertiesBroadcast);
              }

              val rdd_pap_slot = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_slot_header"))
              }
              if (!rdd_pap_slot.isEmpty()) {
                headerNotFound = false
                su.produceToKafka(rdd_pap_slot, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_slot_header"), kafkaSink)
                HiveUtil.insertInto_ea_pap_slot(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_pap_slot), propertiesBroadcast);
              }

              val rdd_pap_info = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_info_header"))
              }
              if (!rdd_pap_info.isEmpty()) {
                headerNotFound = false
                su.produceToKafka(rdd_pap_info, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"), kafkaSink)
                HiveUtil.insertInto_ea_pap_info(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_pap_info), propertiesBroadcast);
              }

              val rdd_pap_gen = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_gen_header"))
              }
              if (!rdd_pap_gen.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_pap_gen, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_gen_header"), kafkaSink)
                HiveUtil.insertInto_ea_pap_gen(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_pap_gen), propertiesBroadcast);
              }

              val rdd_pap_nozzle = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_nozzle_header"))
              }
              if (!rdd_pap_nozzle.isEmpty()) {
                headerNotFound = false
                su.produceToKafka(rdd_pap_nozzle, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_nozzle_header"), kafkaSink)
                HiveUtil.insertInto_ea_pap_nozzle(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_pap_nozzle), propertiesBroadcast);
              }

            }

          } else if (propertiesBroadcast.value.getProperty("inletKafkaTopic").equalsIgnoreCase("cgp_kafka_masterdata_xyz_ra")) {
            if (!parsedDf.isEmpty()) {
              var outputTopics: List[String] = null
              val rdd_progorderlist_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_progorderlist_header"))
              }
              if (!rdd_progorderlist_header.isEmpty()) {
                headerNotFound = false
                //outputTopics = List(propertiesBroadcast.value.getProperty("outletKafkaTopic_progorderlist_header"), propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"))
                su.produceToKafka(rdd_progorderlist_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"), kafkaSink)
                HiveUtil.insertInto_progorderlist(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_progorderlist_header), propertiesBroadcast);
              }

              val rdd_ftdefects_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_ftdefects_header"))
              }
              if (!rdd_ftdefects_header.isEmpty()) {
                headerNotFound = false
                su.produceToKafka(rdd_ftdefects_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_ftdefects_header"), kafkaSink)
                HiveUtil.insertInto_ftdefects(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_ftdefects_header), propertiesBroadcast);
              }

              val rdd_assydetail_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_assydetail_header"))
              }
              if (!rdd_assydetail_header.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_assydetail_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_assydetail_header"), kafkaSink)
                HiveUtil.insertInto_assydetail(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_assydetail_header), propertiesBroadcast);
              }

              val rdd_units_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_units_header"))
              }
              if (!rdd_units_header.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_units_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_units_header"), kafkaSink)
                HiveUtil.insertInto_units(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_units_header), propertiesBroadcast);
              }

              val rdd_wodetail_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_wodetail_header"))
              }
              if (!rdd_wodetail_header.isEmpty()) {
                headerNotFound = false
                //outputTopics = List(propertiesBroadcast.value.getProperty("outletKafkaTopic_wodetail_header"), propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"))
                su.produceToKafka(rdd_wodetail_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"), kafkaSink)
                HiveUtil.insertInto_wodetail(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_wodetail_header), propertiesBroadcast);
              }

              val rdd_assycircuitsilk_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_assycircuitsilk_header"))
              }
              if (!rdd_assycircuitsilk_header.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_assycircuitsilk_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_assycircuitsilk_header"), kafkaSink)
                HiveUtil.insertInto_assycircuitsilk(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_assycircuitsilk_header), propertiesBroadcast);
              }

              val rdd_workorder_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_workorder_header"))
              }
              if (!rdd_workorder_header.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_workorder_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_workorder_header"), kafkaSink)
                HiveUtil.insertInto_workorder(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_workorder_header), propertiesBroadcast);
              }

              val rdd_parentchild_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_parentchild_header"))
              }
              if (!rdd_parentchild_header.isEmpty()) {
                headerNotFound = false
                //su.produceToKafka(rdd_parentchild_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_parentchild_header"), kafkaSink)
                HiveUtil.insertInto_parentchild(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_parentchild_header), propertiesBroadcast);
              }

              val rdd_comptype_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_comptype_header"))
              }
              if (!rdd_comptype_header.isEmpty()) {
                headerNotFound = false
                //outputTopics = List(propertiesBroadcast.value.getProperty("outletKafkaTopic_comptype_header"), propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"))
                su.produceToKafka(rdd_comptype_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"), kafkaSink)
                HiveUtil.insertInto_comptype(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_comptype_header), propertiesBroadcast);
              }

              val rdd_equipmentoutline_header = parsedDf.filter {
                ele => ele.contains(propertiesBroadcast.value.getProperty("ea_equipmentoutline_header"))
              }
              if (!rdd_equipmentoutline_header.isEmpty()) {
                headerNotFound = false
                //outputTopics = List(propertiesBroadcast.value.getProperty("outletKafkaTopic_equipmentoutline_header"), propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"))
                su.produceToKafka(rdd_equipmentoutline_header, propertiesBroadcast.value.getProperty("outletKafkaTopic_pap_masterdata"), kafkaSink)
                HiveUtil.insertInto_equipmentoutline(getInstance(sparkContext, propertiesBroadcast).read.json(rdd_equipmentoutline_header), propertiesBroadcast);
              }
            }
          }
          // incase if no xml tag matched with existing given headers 
          if (headerNotFound) {
            println("***********************************************************************************")
            println("*****  Header not found for incoming kafka topic " + propertiesBroadcast.value.getProperty("inletKafkaTopic") + "******")
            println("*****************   File name : " + fileName + " *******************")
            println("***********************************************************************************")
          }
          // file into hdfs
          val configuration = new Configuration
          configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
          UserGroupInformation.setConfiguration(configuration)

          UserGroupInformation.getCurrentUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS)

          // Perform work within the context of the login user object

          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            propertiesBroadcast.value.getProperty("hadoop.kerberos.principal"), propertiesBroadcast.value.getProperty("hadoop.kerberos.keytab"))
            .doAs(new PrivilegedExceptionAction[Unit]() {
              @Override
              def run(): Unit = {
                try {
                  // saving rdd into hdfs
                  val hdfsPath = propertiesBroadcast.value.getProperty("hdfsPath").replace("{env}",  properties.getProperty("env"))
                  rdd.saveAsTextFile(hdfsPath + "/" + fileName)
                  println("saved: " + hdfsPath + "/" + fileName)
                  parsedDf.unpersist(true)
                  rdd.unpersist(true)

                } catch {
                  case e: Exception => println(" ************** unable to save data in hdfs ********* , " + e)
                }
              }
            })

        }else{
          loginUsingKeytab(propertiesBroadcast)
        }
      } catch {
        case all: Exception =>
          if (all.getCause != null && all.getCause.toString().equalsIgnoreCase("kafka.common.OffsetOutOfRangeException")) {
            // delete the offset value from MySql
            offsetStore.deleteOffsets(topic)
          } else {
            all.printStackTrace()
          }
      }

    }
  }

    def loginUsingKeytab(propertiesBroadcast: Broadcast[Properties]): Unit = {

    println("login using keytab if rdd empty")
    val configuration = new Configuration
    configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.getCurrentUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS)

    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      properties.getProperty("hadoop.kerberos.principal"), propertiesBroadcast.value.getProperty("hadoop.kerberos.keytab"))

  }
    
  /*
	 * main function
	 */
  def main(args: Array[String]): Unit = {

    properties.load(new FileInputStream(args(0)))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
      "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))
    val su = new SparkUtils(properties)

    val conf = su.getSparkConf();
    conf.set("spark.sql.parquet.mergeSchema", "false")
    conf.set("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false")
    conf.set("parquet.enable.summary-metadata", "false")
    conf.set("spark.shuffle.encryption.enabled", "true")

    import org.apache.log4j.{ Level, Logger }
    Logger.getRootLogger().setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    val sc = su.getSparkContext(conf)
    sc.setLogLevel("WARN");

    val secure = properties.getProperty("kerberosSecurity").toBoolean

    if (secure) {
      //This is not the same as the keytab filename you pass it through spark-submit. Spark added a random string at the end of the filename to avoid conflict.
      var keytab = conf.get("spark.yarn.keytab")
      if (keytab != null) {
        println("keytab from spark-submit / yarn " + keytab)
        //Use the below code to distribute the keytab to executor after step 1
        sc.addFile(keytab)
        properties.put("hadoop.kerberos.keytab", keytab)
      } else {
        keytab = properties.getProperty("hadoop.kerberos.keytab");
      }
    }
    val topic = properties.getProperty("inletKafkaTopic")

    val props = new Properties()
    props.put("bootstrap.servers", properties.getProperty("metadataBrokerList"))
    props.put("auto.offset.reset", properties.getProperty("autoOffsetReset"))
    props.put("client.id", properties.getProperty("group.id"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("request.required.acks", "-1")
    props.put("acks", "all")
    props.put("retries", "3")
    props.put("message.send.max.retries", "3")

    //props.put("producer.type", "sync")
    props.put("batch.size", "49152");
    props.put("linger.ms", "2");
    props.put("batch.num.messages", "100");

    val ssc = su.createSparkStreamingContext(sc)
    Logger.getRootLogger().setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    // environment variable
    val env = properties.getProperty("env")
    val databaseName = env + properties.getProperty("jdbcDatabase")
    val offsetStore = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, databaseName)
    val dataStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, topic, "")
    val kafkaSink = dataStream.context.sparkContext.broadcast(KafkaSink(props))
    val propertiesBroadcast = dataStream.context.sparkContext.broadcast(properties)
    processStream(dataStream, topic, properties.getProperty("outletKafkaTopic"), kafkaSink, su, offsetStore, propertiesBroadcast)

    // save the offsets
    offsetStore.saveOffsets(dataStream, "")

    ssc.start()
    ssc.awaitTermination()
  }

 /*
	* processing parsing xml to json objects
	* 
	* @param jsonRDD
	* 
	* @param hdfsFileName
	*  
	* @param topic
	*/
  def loadXML(jsonRDD: RDD[String], hdfsFileName: String, topic: String): RDD[String] = {
    val mapRdd = jsonRDD.map {
      ele =>
        val currentDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(new Date())
        var valuesHeader: String = ""
        var valuesDetails: Array[String] = Array[String]()
        var fields: Array[String] = Array[String]()
        var fieldtypes: Array[String] = Array[String]()
        var separator: String = ""
        var filename: String = ""
        val x = XML.loadString(ele)
        val fileTag = (x \\ "ArrayOfLiveTextData")
        for (tValue <- fileTag) {
          if (tValue.attributes("filename") != null)
            filename = tValue.attributes("filename").toString()
        }
        val nodes = (x \\ "LiveTextData")
        val gson = new Gson
        var finalRow = MutableList[String]()
        for (i <- 0 until nodes.length) {
          val tagValue = (nodes(i) \\ "TagValue")
          for (tValue <- tagValue) {
            fields = tValue.attributes("fieldnames").toString().split(",")
            fieldtypes = tValue.attributes("fieldtypes").toString().split(",")
            separator = tValue.attributes("separator").toString()
          }
          val xmlTagName = (nodes(i) \\ "TagName").text.toString()
          val xmlStatus = (nodes(i) \\ "Status").text.toString()
          val tagId = (nodes(i) \\ "TagId").text.toString()
          val timeStamp = (nodes(i) \\ "TimeStamp").text.toString()
          val jmapHeader = new java.util.LinkedHashMap[String, Any]()

          if (i == 0 && (nodes.length > 1)) {
            valuesHeader = tagValue.text.toString()

            for (i <- 0 until fields.size) {

              jmapHeader.put("xml_tag_name", xmlTagName)
              var splitedData = valuesHeader.split(separator, -1);
              if (splitedData.size < fields.size) {
                println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " " + xmlTagName)
                println("FATAL ERROR: " + hdfsFileName + ": column mismatch in data and header in section 0 " + valuesHeader);

              }
              if (i < splitedData.size) {
                val value = splitedData(i);
                if (value == null || value.trim.isEmpty) {
                  jmapHeader.put(fields(i), value)
                } else {
                  try {
                    jmapHeader.put(fields(i), convertTypes(value.trim(), fieldtypes(i)))
                  } catch {
                    case e: Exception =>
                      println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " " + xmlTagName)
                      println("FATAL ERROR: error in add xml rows in map, " + value.trim() + ", " + fieldtypes(i) + " " + e)
                      jmapHeader.put(fields(i), "")
                  }
                }
              } else {
                println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " " + xmlTagName)
                println("FATAL ERROR: this index does not exist " + fields(i))
                jmapHeader.put(fields(i), "")
              }
              jmapHeader.put("xml_status", xmlStatus)
              jmapHeader.put("xml_tag_id", tagId)
              jmapHeader.put("bicoe_load_dttm", currentDateTime)
              jmapHeader.put("file_timestamp", timeStamp)
              jmapHeader.put("hdfs_file_name", hdfsFileName)
              jmapHeader.put("file_name", filename)
              jmapHeader.put("kafka_topic", topic)

            }
            finalRow += gson.toJson(jmapHeader)
          } else {
            valuesDetails = tagValue.text.split("\\r?\\n")
            for (x <- valuesDetails if (x != null && !x.trim.isEmpty)) {

              val jmapDetail = new java.util.LinkedHashMap[String, Any]()

              for (i <- 0 until fields.size) {
                try {
                  jmapDetail.put("xml_tag_name", xmlTagName)
                  var splitedData = x.split(separator, -1);
                  if (splitedData.size < fields.size) {
                    println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " total rows: " + xmlTagName + ": " + valuesDetails.length)

                    println("FATAL ERROR: " + hdfsFileName + ": column mismatch in data and header " + x + ",data: " + splitedData.size + ",header: " + fields.size);

                  }
                  if (i < splitedData.size) {
                    var value = splitedData(i);
                    if (value == null || value.trim.isEmpty) {
                      jmapDetail.put(fields(i), value)
                    } else {
                      try {
                        jmapDetail.put(fields(i), convertTypes(value.trim(), fieldtypes(i)))
                      } catch {
                        case e: Exception =>
                          println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " total rows: " + xmlTagName + ": " + valuesDetails.length)

                          println("FATAL ERROR: error in add xml rows in map, " + value.trim() + ", " + fieldtypes(i) + " " + e)
                          jmapDetail.put(fields(i), "")
                      }
                    }
                  } else {
                    println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " total rows: " + xmlTagName + ": " + valuesDetails.length)

                    println("FATAL ERROR: this index does not exist " + fields(i))
                    jmapDetail.put(fields(i), "")
                  }
                  jmapDetail.put("xml_status", xmlStatus)
                  jmapDetail.put("xml_tag_id", tagId)
                  jmapDetail.put("bicoe_load_dttm", currentDateTime)
                  jmapDetail.put("file_timestamp", timeStamp)
                  jmapDetail.put("hdfs_file_name", hdfsFileName)
                  jmapDetail.put("file_name", filename)
                  jmapDetail.put("kafka_topic", topic)

                } catch {
                  case e: Exception =>
                    println("hdfs_file_name: " + hdfsFileName + " file_name: " + filename + " fields: " + fields.size + " total rows: " + xmlTagName + ": " + valuesDetails.length)

                    println("FATAL ERROR: error in add xml rows in map, rows till now" + finalRow.size + " " + e)
                }
              }
              finalRow += gson.toJson(jmapDetail)

            }
          }
        }
        finalRow.toList
    }
    mapRdd.flatMap(x => x)
  }

  /*
	 * type conversion function
	 * 
	 * @param value
	 * 
	 * @param type
	 */
  def convertTypes(value: String, ftype: String): Any = ftype match {
    case "Int" | "int"         => value.toInt
    case "String" | "string"   => value.toString
    case "Double" | "double"   => value.toDouble
    case "Float" | "float"     => value.toFloat
    case "Int32" | "int32"     => value.toInt
    case "Int64" | "int64"     => value.toLong
    case "Long" | "long"       => value.toLong
    case "Decimal" | "decimal" => value.toDouble
    case "Boolean" | "boolean" => value.toBoolean
    case _                     => value
  }

  /*
	 * creating singleton hive context
	 * 
	 * @param SparkContext
	 * 
	 * @param Properties
	 */
  def getInstance(sparkContext: SparkContext, propertiesBroadcast: Broadcast[Properties]): HiveContext = {
    synchronized {
      if (instance == null) {
        System.setProperty("hive.metastore.uris", propertiesBroadcast.value.getProperty("hive.metastore.uris"));
        System.setProperty("hive.metastore.sasl.enabled", "true")
        System.setProperty("hive.metastore.kerberos.keytab.file", sparkContext.getConf.get("spark.yarn.keytab"))
        System.setProperty("hive.security.authorization.enabled", "false")
        System.setProperty("hive.metastore.kerberos.principal", propertiesBroadcast.value.getProperty("hive.metastore.kerberos.principal"))
        System.setProperty("hive.metastore.execute.setugi", "true")
        instance = new HiveContext(sparkContext)
        instance.setConf("spark.sql.parquet.writeLegacyFormat", "true")
        instance.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
        instance.setConf("hive.exec.dynamic.partition", "true")
        instance.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        sys.addShutdownHook {
          instance = null
        }

      }
      instance
    }
  }

}