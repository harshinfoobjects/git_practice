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
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.DateType
import scala.collection.mutable.MutableList
import org.apache.spark.sql.DataFrame
import java.util.ArrayList
import org.apache.spark.sql.DataFrame
import org.apache.kafka.clients.producer.ProducerRecord
import com.rockwell.twb.kafka.KafkaSink
import org.apache.spark.broadcast.Broadcast
import com.google.gson.Gson
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions._

object HiveDump {

  val properties = new Properties()

  def processStream(dataStream: InputDStream[(String, String)], hiveContext: HiveContext) {
    val mappedDataStream = dataStream.map(_._2);
    mappedDataStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val rdd_ffr = rdd.filter {
          ele => ele.contains("ffr_result")
        }
        if (!rdd_ffr.isEmpty()) {
          insertInto_ffr(hiveContext.read.json(rdd_ffr))
        }

        val rdd_fpy = rdd.filter {
          ele => ele.contains("fpy_result")
        }
        if (!rdd_fpy.isEmpty()) {
          insertInto_fpy(hiveContext.read.json(rdd_fpy))
        }

        val rdd_volume = rdd.filter {
          ele => ele.contains("volume_result")
        }
        if (!rdd_volume.isEmpty()) {
          insertInto_volume(hiveContext.read.json(rdd_volume))
        }
      }
    }
  }

  def insertInto_volume(df: DataFrame): Unit = {
    var tableName = properties.getProperty("ea_spi_volume_table")
    val renameDf = df.withColumnRenamed("volume_result", "result")
    val readyToSaveDf = renameDf.select("generated_key", "interval_id", "pcb_name", "result", "min_inspection_end_time", "max_inspection_end_time", "machineid_side", "bicoe_load_dttm", "machine_id", "pcb_side")
    readyToSaveDf.write.partitionBy("machine_id", "pcb_side").mode(SaveMode.Append).format("parquet").insertInto(tableName)
  }

  def insertInto_fpy(df: DataFrame): Unit = {

    val tableName = properties.getProperty("ea_spi_fpy_table")
    val renameDf = df.withColumnRenamed("fpy_result", "result")
    val readyToSaveDf = renameDf.select("generated_key", "interval_id", "pcb_name", "result", "min_inspection_end_time", "max_inspection_end_time", "machineid_side", "bicoe_load_dttm", "machine_id", "pcb_side")
    readyToSaveDf.write.partitionBy("machine_id", "pcb_side").mode(SaveMode.Append).format("parquet").insertInto(tableName)
  }

  def insertInto_ffr(df: DataFrame): Unit = {
    var tableName = properties.getProperty("ea_spi_ffr_table")
    val renameDf = df.withColumnRenamed("ffr_result", "result")
    val readyToSaveDf = renameDf.select("generated_key", "interval_id", "pcb_name", "result", "min_inspection_end_time", "max_inspection_end_time", "machineid_side", "bicoe_load_dttm", "machine_id", "pcb_side")
    readyToSaveDf.write.partitionBy("machine_id", "pcb_side").mode(SaveMode.Append).format("parquet").insertInto(tableName)

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
    val sc = su.getSparkContext(conf)
    val secure = properties.getProperty("kerberosSecurity").toBoolean

    if (secure) {
      var keytab = conf.get("spark.yarn.keytab")
      if (keytab == null)
        keytab = properties.getProperty("hadoop.kerberos.keytab");
      sc.addFile(keytab)
    }

    System.setProperty("hive.metastore.uris", properties.getProperty("hive.metastore.uris"));
    //System.setProperty("hive.metastore.sasl.enabled", "true")
    //System.setProperty("hive.security.authorization.enabled", "false")
    //System.setProperty("hive.metastore.kerberos.principal", properties.getProperty("hadoop.kerberos.principal"))
    //System.setProperty("hive.metastore.execute.setugi", "true")
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

    val ssc = su.createSparkStreamingContext(sc)
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
    props.put("producer.type", "sync")

    val offsetStore = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, properties.getProperty("jdbcDatabase"))
    val dataStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, topic, "_hive")
    processStream(dataStream, hiveContext)

    // save the offsets
    offsetStore.saveOffsets(dataStream, "_hive")

    ssc.start()
    ssc.awaitTermination()
  }
}
