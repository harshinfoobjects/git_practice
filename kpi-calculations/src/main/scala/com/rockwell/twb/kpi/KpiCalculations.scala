package com.rockwell.twb.kpi

import java.io.FileInputStream
import java.util.Calendar
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import com.rockwell.twb.kafka.KafkaSink
import com.rockwell.twb.stores.MySqlOffsetsStore
import com.rockwell.twb.util.Gateway
import com.rockwell.twb.util.SparkUtils

import kafka.serializer.StringDecoder
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame

object KpiCalculations {

  val properties = new Properties()
  var broadcastedHeader: Broadcast[DataFrame] = null

  var to = ""
  var from = ""
  var subject = ""
  var key = ""

  val getintervalId = udf { (dateTime: Integer) =>
    println("inside udf............." + dateTime)
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(dateTime.toLong * 1000)
    var min = 0
    val origMin = calendar.get(Calendar.MINUTE)
    if (origMin <= 15)
      min = 0
    if (origMin > 15 && origMin <= 30)
      min = 15
    else if (origMin > 30 && origMin <= 45)
      min = 30
    else
      min = 45

    calendar.set(Calendar.MINUTE, min);
    calendar.getTimeInMillis
  }

  /*
	 * main function
	 */
  def main(args: Array[String]): Unit = {
    properties.load(new FileInputStream(args(0)))

    to = properties.getProperty("to_List");
    from = properties.getProperty("from");
    subject = properties.getProperty("subject");
    key = properties.getProperty("sendGridKey");

    val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
      "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))
    val su = new SparkUtils(properties)
    var ssc: StreamingContext = null
    try {
      ssc = su.getSparkStreamingContext()
      val headerTopicName = properties.getProperty("inletHeaderKafkaTopic")
      val detailTopicName = properties.getProperty("inletDetailKafkaTopic")
      val debug = properties.getProperty("debug").toBoolean

      val props = new Properties()
      props.put("bootstrap.servers", properties.getProperty("metadataBrokerList"))
      props.put("auto.offset.reset", properties.getProperty("autoOffsetReset"))
      props.put("client.id", properties.getProperty("group.id"))
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("outTopic", properties.getProperty("outletKafkaTopic"))
      props.put("request.required.acks", "-1")
      props.put("acks", "all")
      props.put("retries", "3")
      props.put("message.send.max.retries", "3")
      props.put("producer.type", "sync")

      val offsetStore = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, properties.getProperty("jdbcDatabase"))
      val headerStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, headerTopicName, "")
      val detailStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, detailTopicName, "")
      val kafkaSink = headerStream.context.sparkContext.broadcast(KafkaSink(props))
      val numberOfPartitions = headerStream.context.sparkContext.broadcast(properties.getProperty("noOfPartitions"))
      performCalculation(headerStream, ssc, "spi_header_data", debug, properties.getProperty("outletKafkaTopic"), kafkaSink, su, numberOfPartitions)
      performCalculation(detailStream, ssc, "spi_detail_data", debug, properties.getProperty("outletKafkaTopic"), kafkaSink, su, numberOfPartitions)

      // save the offsets
      offsetStore.saveOffsets(headerStream, "")
      offsetStore.saveOffsets(detailStream, "")
    } catch {
      case e1: Exception =>
        Gateway.sendEmail(to, from, subject, e1, key)
        e1.printStackTrace()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  @throws(classOf[Exception])
  def performCalculation(dataStream: InputDStream[(String, String)], ssc: StreamingContext, tableName: String, debug: Boolean, outputTopic: String, kafkaSink: Broadcast[KafkaSink], su: SparkUtils, numberOfPartitions: Broadcast[String]) {
    val mappedDataStream = dataStream.map(_._2);
    mappedDataStream.foreachRDD { rdd =>
      val timestamp = unix_timestamp().cast("Double").cast("timestamp")
      if (!rdd.isEmpty()) {
        val sqlContext = SQLContext.getOrCreate(rdd.context)
        import sqlContext.implicits._

        val rawDf = sqlContext.read.json(rdd)
        var fpy = sqlContext.emptyDataFrame
        var ffr = sqlContext.emptyDataFrame
        var volume = sqlContext.emptyDataFrame
        if (tableName.equalsIgnoreCase("spi_header_data")) {
          broadcastedHeader = rdd.context.broadcast(rawDf)
          broadcastedHeader.value.registerTempTable(tableName)
          fpy = calculateFPYFFR(sqlContext, "fpy", "'GOOD','PASS'", numberOfPartitions).withColumn("bicoe_load_dttm", lit(timestamp))
          ffr = calculateFPYFFR(sqlContext, "ffr", "'PASS'", numberOfPartitions).withColumn("bicoe_load_dttm", lit(timestamp))
          if (debug) {
            println("======================= Printing SPI data =============================")
            println("======================= FPY per Machine ===============================")
            fpy.show()
            println("======================= FFR per Machine ===============================")
            ffr.show()
          }
          su.produceToKafka(fpy.toJSON, outputTopic, kafkaSink)
          su.produceToKafka(ffr.toJSON, outputTopic, kafkaSink)
        } else {
          rawDf.registerTempTable(tableName)
          volume = calculateVolume(rawDf: DataFrame, sqlContext, broadcastedHeader, numberOfPartitions).withColumn("bicoe_load_dttm", lit(timestamp))
          if (debug) {
            println("======================= Volume % per Machine ==========================")
            volume.show()
          }
          su.produceToKafka(volume.toJSON, outputTopic, kafkaSink)
        }

        //        rawDf.unpersist()
      }
    }
  }

  @throws(classOf[Exception])
  def calculateFPYFFR(sqlcontext: SQLContext, datapoint: String, values: String, numberOfPartitions: Broadcast[String]): DataFrame = {
    val arg1 = sqlcontext.sql("select count(*) as arg1,MachineID as arg1_machine from spi_header_data where PCBResult in ('GOOD','PASS','FAIL') group by MachineID,Side");
    val arg2 = sqlcontext.sql("select count(*) as arg2,MachineID as arg2_machine from spi_header_data where PCBResult in (" + values + ") group by MachineID,Side");
    val machines = sqlcontext.sql("select concat(MachineID,'-',Side) as machineId_side,GeneratedKey,Side,PCBName,MachineID,min(UNIX_TIMESTAMP(InspectionEndTime, 'MM/dd/yyyy hh:mm:ss a')) AS MinInspectionEndTimestamp," +
      "max(UNIX_TIMESTAMP(InspectionEndTime, 'MM/dd/yyyy hh:mm:ss a')) AS MaxInspectionEndTimestamp " +
      "from spi_header_data group by MachineID,PCBName,Side,GeneratedKey");
    val dataframe = arg1.join(arg2, arg1("arg1_machine") <=> arg2("arg2_machine"), "inner").coalesce(numberOfPartitions.value.toInt)
    val fpyPerMachine = dataframe.withColumn("result", dataframe("arg2") / dataframe("arg1"))
    val newdf = fpyPerMachine.join(machines, machines("MachineID") <=> fpyPerMachine("arg1_machine"), "right").coalesce(numberOfPartitions.value.toInt)
      .withColumn("interval_id", getintervalId(machines("MaxInspectionEndTimestamp")))
      .withColumn("MinInspectionEndTimestamp", machines("MinInspectionEndTimestamp").cast("Double").cast("timestamp")).withColumn("MaxInspectionEndTimestamp", machines("MaxInspectionEndTimestamp").cast("Double").cast("timestamp"))
    val finaldf = newdf.na.fill(0).select("GeneratedKey", "MinInspectionEndTimestamp", "MaxInspectionEndTimestamp", "MachineID", "PCBName", "Side", "result", "machineId_side", "interval_id")
    val newNames = Seq("generated_key", "min_inspection_end_time", "max_inspection_end_time", "machine_id", "pcb_name", "pcb_side", datapoint + "_result", "machineId_side", "interval_id")
    val dfRenamed = finaldf.toDF(newNames: _*)
    dfRenamed
  }

  @throws(classOf[Exception])
  def calculateVolume(detaildf: DataFrame, sqlcontext: SQLContext, broadcastedHeader: Broadcast[DataFrame], numberOfPartitions: Broadcast[String]): DataFrame = {
    val sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")

    val parserDateTime = udf { (time: String) =>
      sdf.parse(time).getTime / 1000
    }

    val volumeWithDetails = detaildf.join(broadcastedHeader.value, Seq("GeneratedKey"), "inner").coalesce(numberOfPartitions.value.toInt)
    val formattedDate = volumeWithDetails.withColumn("InspectionEndTime", parserDateTime(volumeWithDetails("InspectionEndTime")))

    val volumeCal = formattedDate.groupBy("MachineID", "Side")
      .agg(avg("Volume").alias("volume_result"),
        min("InspectionEndTime").cast("Double").cast("timestamp").alias("min_inspection_end_time"),
        max("InspectionEndTime").alias("max_inspection_end_time"))

    val finalDf = volumeCal.join(formattedDate.select("MachineID", "Side", "GeneratedKey", "PCBName").distinct(),
      Seq("MachineID", "Side"), "inner").coalesce(numberOfPartitions.value.toInt).na.fill(0)
    val volumeDf = finalDf.withColumn("interval_id", getintervalId(finalDf("max_inspection_end_time")))
      .withColumn("machineId_side", concat(finalDf("MachineID"), lit("-"), finalDf("Side")))
      .withColumn("max_inspection_end_time", finalDf("max_inspection_end_time").cast("Double").cast("timestamp"))

    val selectedDf = volumeDf.select("MachineID", "Side", "GeneratedKey", "PCBName", "volume_result", "min_inspection_end_time", "max_inspection_end_time", "interval_id", "machineId_side")
    val newNames = Seq("machine_id", "pcb_side", "generated_key", "pcb_name", "volume_result", "min_inspection_end_time", "max_inspection_end_time", "interval_id", "machineId_side")
    val dfRenamed = selectedDf.toDF(newNames: _*)
    dfRenamed
  }

}