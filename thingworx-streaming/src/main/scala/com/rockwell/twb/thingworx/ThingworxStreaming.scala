package com.rockwell.twb.thingworx

import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

import scala.collection.mutable.MutableList
import scala.xml.XML

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
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
import com.google.gson.Gson
import com.rockwell.twb.stores.MySqlOffsetsStore
import com.rockwell.twb.util.Gateway
import com.rockwell.twb.util.SparkUtils
import org.apache.spark.sql.functions.unix_timestamp
import kafka.serializer.StringDecoder

object ThingworxStreaming {

  val properties = new Properties()
  val dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  @transient private var instance: HiveContext = _
  var broadcastedEquipmentOnline: Broadcast[DataFrame] = null
  var broadcastedInfo: Broadcast[DataFrame] = null
  var broadcastedWoDetail: Broadcast[DataFrame] = null
  var broadcastedProgorderList: Broadcast[DataFrame] = null
  var broadcastedCompType: Broadcast[DataFrame] = null
  var lastRefreshTime = -1L;
  var lookupRefreshInterval = 0L;

  /*
	* main function
	*/
  def main(args: Array[String]): Unit = {
    import java.io.FileInputStream
    properties.load(new FileInputStream(args(0)))
    val to = properties.getProperty("to_List");
    val from = properties.getProperty("from");
    val subject = properties.getProperty("subject");
    val key = properties.getProperty("sendGridKey");
    var ssc: StreamingContext = null

    try {
      val kafkaParams = Map[String, String](
        "metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
        "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))

      val split = udf(
        (first: String) => {
          val firstArr = first.split("\\.")
          firstArr(0)
        })

      val concat = udf((first: String, second: String) => first + "-" + second)

      val concatenated = udf((first: String, second: String) => { "http://usmkeweb071.na.home.ra-int.com/IncuityPortal/MyEnterprise/Reports/Dashboards/wo.asp?wo=" + first + "&Location=" + second + "&Section=" })

      // Kafka properties
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
      props.put("batch.size", "49152"); //48 KB
      props.put("linger.ms", "2");
      props.put("batch.num.messages", "100");

      val su = new SparkUtils(properties)
      val conf = su.getSparkConf();
      conf.set("spark.kryoserializer.buffer.max", "512")
      conf.set("spark.sql.broadcastTimeout", "36000")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "94371840")

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

      ssc = su.createSparkStreamingContext(sc)

      Logger.getRootLogger().setLevel(Level.WARN)
      Logger.getLogger("org").setLevel(Level.WARN);
      Logger.getLogger("akka").setLevel(Level.WARN);

      val secure = properties.getProperty("kerberosSecurity").toBoolean

      if (secure) {
        var keytab = conf.get("spark.yarn.keytab")
        if (keytab == null){
          keytab = properties.getProperty("hadoop.kerberos.keytab");
        }else{
          properties.setProperty("spark.yarn.keytab", keytab)
        }
        ssc.sparkContext.addFile(keytab)
      }

      // environment variable
      val env = properties.getProperty("env")
      val hiveContext = getHiveContext(ssc.sparkContext, properties)
      val topic = properties.getProperty("inletKafkaTopic")
      var lookupRefreshIntervalMiliSec = properties.getProperty("lookupRefreshInterval").toLong * 1000
      val offsetStore = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, env + properties.getProperty("jdbcDatabase"))
      val dataStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, topic, "")

      val propertiesBroadcast = dataStream.context.sparkContext.broadcast(properties)
      val debug = properties.getProperty("debug").toBoolean
      println("loading common tables first time")

      // load latest records from raw layer elec_assy.ea_equipmentoutline table

      val equipDfHive = hiveContext.sql("select tmp.machine_name, tmp.equipment_make, tmp.equipment_model,"
        + " tmp.equipment_type, tmp.head_1, tmp.head_2, tmp.plant from (select machine_name, equipment_make, equipment_model,equipment_type, head_1,"
        + " head_2, plant, row_number() over (partition by machine_name , plant order by hdfs_file_name desc) row_num from " + env + properties.getProperty("ea_equipmentoutline_table") + " ) tmp"
        + " where tmp.row_num = 1")

      broadcastedEquipmentOnline = ssc.sparkContext.broadcast(equipDfHive.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis())))
      if (broadcastedEquipmentOnline != null && broadcastedEquipmentOnline.value != null)
        broadcastedEquipmentOnline.value.registerTempTable("ea_equipmentoutline")
      if (debug) {
        println("###  ea_equipmentoutline ###")
        hiveContext.sql("select * from ea_equipmentoutline").show()

      }

      // load latest records from raw layer elec_assy.ea_wodetail table
      val infoDfHive = hiveContext.sql("select tmp.crc_id, tmp.name, tmp.description, tmp.plant_code, tmp.machine from (select crc_id, name,"
        + " description, plant_code, machine, row_number() over (partition by plant_code, machine, crc_id order by hdfs_file_name desc) row_num from " + env + properties.getProperty("ea_pap_info_table") + " )"
        + " tmp where tmp.row_num = 1")

      broadcastedInfo = ssc.sparkContext.broadcast(infoDfHive.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis())))
      if (broadcastedInfo != null && broadcastedInfo.value != null)
        broadcastedInfo.value.registerTempTable("ea_pap_info")
      if (debug) {
        println("###  ea_pap_info ###")
        hiveContext.sql("select * from ea_pap_info").show()
      }

      val wodetailDfHive = hiveContext.sql("select tmp.optel_schedule_wo, tmp.sap_wo, tmp.side, tmp.assembly, tmp.plant, tmp.setup, tmp.wo_detail from (select optel_schedule_wo,sap_wo,"
        + " side, assembly, plant, setup, concat(\"http://usmkeweb071.na.home.ra-int.com/IncuityPortal/MyEnterprise/Reports/Dashboards/wo.asp?wo=\",sap_wo,\"&Location=\", plant,\"&Section=\") as wo_detail,"
        + " row_number() over (partition by plant , optel_schedule_wo  order by hdfs_file_name) row_num  from " + env + properties.getProperty("ea_wodetail_table") + ") tmp"
        + " where tmp.row_num = 1")

      broadcastedWoDetail = ssc.sparkContext.broadcast(wodetailDfHive.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis())))
      if (broadcastedWoDetail != null && broadcastedWoDetail.value != null)
        broadcastedWoDetail.value.registerTempTable("ea_wodetail")
      if (debug) {
        println("### ea_wodetail ###")
        hiveContext.sql("select * from ea_wodetail").show()
      }

      val progorderListDfHive = hiveContext.sql("select tmp.machine, tmp.plant, tmp.work_order, tmp.ref_id, tmp.silkscreen, tmp.circuit, tmp.head, tmp.spindle, tmp.head_spindle, tmp.ref_des,"
        + " tmp.Pcircuit from ( select machine, plant, work_order, ref_id, silkscreen, circuit, head, spindle, concat(head,\"-\",spindle) as head_spindle, substr(ref_id,1,length(circuit)+1) as Pcircuit,"
        + " substr(ref_id,length(circuit)+2) as ref_des, row_number() over (partition by machine,plant,work_order,substr(ref_id,length(circuit)+2),silkscreen order by hdfs_file_name desc) row_num from " + env + properties.getProperty("ea_progorderlist_table") + " ) tmp"
        + " where tmp.row_num = 1")

      broadcastedProgorderList = ssc.sparkContext.broadcast(progorderListDfHive.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis())))
      if (broadcastedProgorderList != null && broadcastedProgorderList.value != null)
        broadcastedProgorderList.value.registerTempTable("ea_progorderlist")
      if (debug) {
        println("### ea_progorderlist ###")
        hiveContext.sql("select * from ea_progorderlist").show()

      }

      val compTypeDfHive = hiveContext.sql("select tmp.plant, tmp.machine, tmp.part_number, tmp.head_type, tmp.pins, tmp.nozzle1, tmp.package from ("
        + " select plant, machine, part_number, head_type, pins, nozzle1, package, row_number() over (partition by plant , machine , part_number, head_type order by hdfs_file_name) row_num from " + env + properties.getProperty("ea_comptype_table") + " ) tmp"
        + " where tmp.row_num = 1")

      broadcastedCompType = ssc.sparkContext.broadcast(compTypeDfHive.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis())))
      if (broadcastedCompType != null && broadcastedCompType.value != null)
        broadcastedCompType.value.registerTempTable("ea_comptype")
      if (debug) {
        println("###  ea_comptype ###")
        hiveContext.sql("select * from ea_comptype").show()

      }

      refreshMaterialCostTable(hiveContext, propertiesBroadcast)

      val mappedDataStream = dataStream.map(_._2);
      mappedDataStream.foreachRDD { rdd =>
        try {

          if (!rdd.isEmpty()) {

            val sparkContext = rdd.sparkContext
            import org.apache.spark.sql.functions.col
            var fileName = propertiesBroadcast.value.getProperty("hdfs_file_prefix") + "_" + System.currentTimeMillis();
            val parsedRdd = loadXML(rdd, fileName, topic)
            // val parser = new JsonParser
            val hiveContext = getHiveContext(sparkContext, propertiesBroadcast.value)
            parsedRdd.cache()
            val infoDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_info_header"))))
            val equipmentonlineDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_equipmentoutline_header"))))
            val wodetailDF = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_wodetail_header"))))
            val progorderListDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_progorderlist_header"))))
            val compTypeDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_comptype_header"))))
            val nozzleDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_nozzle_header"))))
            val timerseDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_timers_header"))))
            val slotDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_slot_header"))))
            val defectDf = hiveContext.read.json(parsedRdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_ftdefects_header"))))

            val debug = properties.getProperty("debug").toBoolean
            if (debug) {

              if (!equipmentonlineDf.rdd.isEmpty()) {
                println("### incoming equipment Online Data ###")
                equipmentonlineDf.show()
              } else {
                println("### No data for Equipment Online ###")
              }

              if (!infoDf.rdd.isEmpty()) {
                println("###  incoming  info Data ###")
                infoDf.show()
              } else {
                println("### No data for Info ###")
              }

              if (!wodetailDF.rdd.isEmpty()) {
                println("###  incoming wodetail Data ###")
                wodetailDF.show()
              } else {
                println("### No data for woDetail ###")
              }

              if (!progorderListDf.rdd.isEmpty()) {
                println("###  incoming  progorderList Data ###")
                progorderListDf.show()
              } else {
                println("### No data for progorderList ###")
              }

              if (!nozzleDf.rdd.isEmpty()) {
                println("###  incoming nozzle Data ###")
                nozzleDf.show()
              } else {
                println("### No data for nozzle ###")
              }
              if (!timerseDf.rdd.isEmpty()) {
                println("###  incoming timers Data ###")
                timerseDf.show()
              } else {
                println("### No data for timers ###")
              }

              if (!slotDf.rdd.isEmpty()) {
                println("###  incoming slot Data ###")
                slotDf.show()
              } else {
                println("### No data for slot ###")
              }

              if (!defectDf.rdd.isEmpty()) {
                println("###  incoming defect Data ###")
                defectDf.show()
              } else {
                println("### No data for defect ###")
              }
            }

            if (!infoDf.rdd.isEmpty()) {
              val infoDf1 = infoDf.select("CRCId", "Name", "Description", "PlantCode", "Machine").withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis()))
                .withColumnRenamed("Name", "name")
                .withColumnRenamed("CRCId", "crc_id")
                .withColumnRenamed("Description", "description")
                .withColumnRenamed("PlantCode", "plant_code")
                .withColumnRenamed("Machine", "machine")

              broadcastedInfo.value.unionAll(infoDf1).registerTempTable("infoTmp")

              val infoLatestRecordDF = hiveContext.sql("select tmp.crc_id, tmp.name, tmp.description, tmp.plant_code, tmp.machine, tmp.updateTableTimestamp from (select crc_id, name,"
                + " description, plant_code, machine, updateTableTimestamp, row_number() over (partition by plant_code, machine, crc_id order by updateTableTimestamp desc) row_num from  infoTmp )"
                + " tmp where tmp.row_num = 1")

              if (!infoLatestRecordDF.rdd.isEmpty()) {

                broadcastedInfo = rdd.context.broadcast(infoLatestRecordDF)

                broadcastedInfo.value.registerTempTable("ea_pap_info")
                if (debug) {
                  println("### broadcasted ea_info ###")
                  hiveContext.sql("select updateTableTimestamp, count(*) from ea_pap_info group by updateTableTimestamp").show()
                }
              } else {
                println("### after join ea_info its empty ###")
                infoDf.show();
              }
            }

            if (!equipmentonlineDf.rdd.isEmpty()) {
              val equipmentonlineDf1 = equipmentonlineDf.select("Machine Name", "Equipment Make", "Equipment Model", "Equipment Type", "Head 1", "Head 2", "PLANT").withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis()))

              val renameEquipmentonlineDf = equipmentonlineDf1.withColumnRenamed("Machine Name", "machine_name")
                .withColumnRenamed("Equipment Make", "equipment_make")
                .withColumnRenamed("Equipment Model", "equipment_model")
                .withColumnRenamed("Equipment Type", "equipment_type")
                .withColumnRenamed("Head 1", "head_1")
                .withColumnRenamed("Head 2", "head_2")
                .withColumnRenamed("PLANT", "plant")

              broadcastedEquipmentOnline.value.unionAll(renameEquipmentonlineDf).registerTempTable("equipmentonlineTmp")

              val equipmentonlineLatestDf = hiveContext.sql("select tmp.machine_name, tmp.equipment_make, tmp.equipment_model,"
                + " tmp.equipment_type, tmp.head_1, tmp.head_2, tmp.plant , tmp.updateTableTimestamp from (select machine_name, equipment_make, equipment_model,equipment_type, head_1,"
                + " head_2, plant, updateTableTimestamp, row_number() over (partition by machine_name , plant order by updateTableTimestamp desc) row_num from equipmentonlineTmp ) tmp"
                + " where tmp.row_num = 1")

              if (!equipmentonlineLatestDf.rdd.isEmpty()) {
                broadcastedEquipmentOnline = rdd.context.broadcast(equipmentonlineLatestDf)
                broadcastedEquipmentOnline.value.registerTempTable("ea_equipmentoutline")
                if (debug) {
                  println("### broadcasted ea_equipmentoutline ###")
                  hiveContext.sql("select updateTableTimestamp, count(*) from ea_equipmentoutline group by updateTableTimestamp").show()
                }
              } else {
                println("### after join ea_equipmentoutline its empty ###")
                equipmentonlineDf.show();
              }
            }

            if (!wodetailDF.rdd.isEmpty()) {
              val wodetailDF1 = wodetailDF.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis()))
                .withColumnRenamed("Optel Schedule WO", "optel_schedule_wo")
                .withColumnRenamed("SAP WO", "sap_wo")
                .withColumnRenamed("Side", "side")
                .withColumnRenamed("Assembly", "assembly")
                .withColumn("wo_detail", concatenated(wodetailDF("sap_wo"), wodetailDF("PLANT")))
                .withColumnRenamed("PLANT", "plant")
                .withColumnRenamed("Setup#", "setup").select("optel_schedule_wo", "sap_wo", "side", "assembly", "plant", "setup", "wo_detail", "updateTableTimestamp")

              broadcastedWoDetail.value.unionAll(wodetailDF1).registerTempTable("wodetailTmp")

              val wodetailLatestDf = hiveContext.sql("select tmp.optel_schedule_wo, tmp.sap_wo, tmp.side, tmp.assembly, tmp.plant, tmp.setup, tmp.wo_detail, tmp.updateTableTimestamp from (select optel_schedule_wo,sap_wo,"
                + " side, assembly, plant, setup, concat(\"http://usmkeweb071.na.home.ra-int.com/IncuityPortal/MyEnterprise/Reports/Dashboards/wo.asp?wo=\",sap_wo,\"&Location=\", plant,\"&Section=\") as wo_detail,"
                + " updateTableTimestamp, row_number() over (partition by plant , optel_schedule_wo  order by updateTableTimestamp desc) row_num  from wodetailTmp) tmp"
                + " where tmp.row_num = 1")

              if (!wodetailLatestDf.rdd.isEmpty()) {

                broadcastedWoDetail = rdd.context.broadcast(wodetailLatestDf)
                broadcastedWoDetail.value.registerTempTable("ea_wodetail")
                if (debug) {
                  println("### broadcasted ea_wodetail ###")
                  hiveContext.sql("select updateTableTimestamp, count(*) from ea_wodetail group by updateTableTimestamp").show()
                }
              } else {
                println("### after join ea_wodetail its empty ###")
                wodetailDF.show();
              }
            }

            if (debug) {
              println("currentTime:" + java.lang.System.currentTimeMillis() + " lastRefreshTime:" + lastRefreshTime + " lookupRefreshIntervalmill:" + lookupRefreshIntervalMiliSec)
              println("(currentTime - lastRefreshTime) :" + (java.lang.System.currentTimeMillis() - lastRefreshTime) + " lookupRefreshIntervalmill :" + lookupRefreshIntervalMiliSec)
            }

            if (!progorderListDf.rdd.isEmpty()) {
              progorderListDf.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis()))
                .withColumnRenamed("PLANT", "plant")
                .withColumnRenamed("Machine", "machine")
                .withColumnRenamed("Work Order", "work_order")
                .withColumnRenamed("Assembly", "assembly")
                .withColumnRenamed("Revision", "revision")
                .withColumnRenamed("Side", "side")
                .withColumnRenamed("Part Number", "part_number")
                .withColumnRenamed("Slot", "slot")
                .withColumnRenamed("Head #", "head")
                .withColumnRenamed("Spindle", "spindle")
                .withColumnRenamed("RefID", "ref_id")
                .withColumnRenamed("Time Stamp", "time_stamp")
                .withColumnRenamed("Circuit #", "circuit")
                .withColumnRenamed("Silkscreen", "silkscreen").registerTempTable("progorderListInnerTmp")

              val progorderListDf1 = hiveContext.sql("select machine, plant, work_order, ref_id, silkscreen, circuit, head, spindle, concat(head,\"-\",spindle) as head_spindle, substr(ref_id,length(circuit)+2) as ref_des,"
                + " substr(ref_id,1,length(circuit)+1) as Pcircuit, updateTableTimestamp from progorderListInnerTmp")

              broadcastedProgorderList.value.unionAll(progorderListDf1).registerTempTable("progorderListTmp")

              val progorderListLatsetDf = hiveContext.sql("select tmp.machine, tmp.plant, tmp.work_order, tmp.ref_id, tmp.silkscreen, tmp.circuit, tmp.head, tmp.spindle, tmp.head_spindle, tmp.ref_des,"
                + " tmp.Pcircuit, tmp.updateTableTimestamp from ( select machine, plant, work_order, ref_id, silkscreen, circuit, head, spindle, head_spindle, Pcircuit,"
                + " ref_des, updateTableTimestamp, row_number() over (partition by machine,plant,work_order,ref_des,silkscreen order by updateTableTimestamp desc) row_num from progorderListTmp ) tmp"
                + " where tmp.row_num = 1")

              if (!progorderListLatsetDf.rdd.isEmpty()) {

                broadcastedProgorderList = rdd.context.broadcast(progorderListLatsetDf)
                broadcastedProgorderList.value.registerTempTable("ea_progorderlist")
                if (debug) {
                  println("### broadcasted ea_progorderlist ###")
                  hiveContext.sql("select updateTableTimestamp, count(*) from ea_progorderlist group by updateTableTimestamp").show()
                }
              } else {
                println("### after join ea_progorderlist its empty ###")
                progorderListDf.show();
              }
            }

            if (!compTypeDf.rdd.isEmpty()) {
              val compTypeDf1 = compTypeDf.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis()))
                .withColumnRenamed("PLANT", "plant")
                .withColumnRenamed("Machine", "machine")
                .withColumnRenamed("Part Number", "part_number")
                .withColumnRenamed("Package", "package")
                .withColumnRenamed("Head Type", "head_type")
                .withColumnRenamed("Nozzle1", "nozzle1")
                .withColumnRenamed("# Pins", "pins")
                .select("plant", "machine", "part_number", "head_type", "pins", "nozzle1", "package", "updateTableTimestamp")

              broadcastedCompType.value.unionAll(compTypeDf1).registerTempTable("compTypeTmp")

              val compTypeLatestDf = hiveContext.sql("select tmp.plant, tmp.machine, tmp.part_number, tmp.head_type, tmp.pins, tmp.nozzle1, tmp.package, tmp.updateTableTimestamp from ("
                + " select plant, machine, part_number, head_type, pins, nozzle1, package, updateTableTimestamp, row_number() over (partition by plant , machine , part_number, head_type order by updateTableTimestamp) row_num from compTypeTmp ) tmp"
                + " where tmp.row_num = 1")

              if (!compTypeLatestDf.rdd.isEmpty()) {
                broadcastedCompType = rdd.context.broadcast(compTypeLatestDf)
                broadcastedCompType.value.registerTempTable("ea_comptype")
                if (debug) {
                  println("### broadcasted ea_comptype ###")
                  hiveContext.sql("select updateTableTimestamp, count(*) from ea_comptype group by updateTableTimestamp").show()
                }
              } else {
                println("### after join ea_info its empty ###")
                compTypeDf.show();
              }
            }
            val dformat = new SimpleDateFormat(dateTimeFormat);
            if (!nozzleDf.rdd.isEmpty()) {
              println("new nozzle data");
              nozzleDf.withColumnRenamed("Product_ID", "product_id")
                .withColumn("Date_Time", unix_timestamp(nozzleDf("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time").withColumn("date_time", when(col("date_time").isNull, dformat.format(new Date())).otherwise(col("date_time")))
                .withColumnRenamed("User_name", "user_name")
                .withColumnRenamed("location", "location")
                .withColumnRenamed("ulpicks", "ul_picks")
                .withColumnRenamed("ulplacements", "ul_placements")
                .withColumnRenamed("ulcompmissing", "ul_comp_missing")
                .withColumnRenamed("ulPossibleMissing", "ul_possible_missing")
                .withColumnRenamed("ulRejects", "ul_rejects")
                .withColumn("split_plant_id", split(col("product_id")))
                .select("product_id", "date_time", "user_name", "location", "tooltype", "ul_picks", "ul_placements", "ul_comp_missing", "ul_possible_missing", "ul_rejects", "split_plant_id")
                .registerTempTable("ea_pap_nozzle")

              if (debug) {
                println("### nozzle table ###")
                hiveContext.sql("select * from ea_pap_nozzle").show()
              }

              val query = new StringBuilder
              query.append("SELECT A.*, B.machine_name, B.plant, C.name as tooltypenozzle, D.optel_schedule_wo, D.sap_wo, D.side, D.assembly, D.wo_detail, ")
              query.append("CONCAT (CAST(CAST(split(A.location, ',')[1] AS INT) + 1 AS string),\"-\",CAST(CAST(split(A.location, ',')[2] AS INT) + 1 AS string)) nozzle_ch_hole, ")
              query.append("CONCAT (B.machine_name,\" - \",CAST(CAST(split(A.location, ',')[1] AS INT) + 1 AS string),\"-\",CAST(CAST(split(A.location, ',')[2] AS INT) + 1 AS string)) nozzle_mach_ch_hole ")
              query.append("FROM ea_pap_nozzle A ")
              query.append("JOIN ea_equipmentoutline B ON A.user_name = B.equipment_make ")
              query.append("JOIN ea_pap_info C ON A.tooltype = C.crc_id AND C.machine = B.machine_name and C.plant_code = B.plant ")
              query.append("JOIN ea_wodetail D ON A.split_plant_id = D.optel_schedule_wo ")
              query.append("LIMIT " + propertiesBroadcast.value.getProperty("maxLimit"))

              val resultDF = hiveContext.sql(query.toString())

              if (debug) {
                println("### result nozzle data ###")
                resultDF.show()
              }
              if (!resultDF.rdd.isEmpty) {
                dumpNozzleResultToThingworx(resultDF, propertiesBroadcast, to, from, subject, key, su)
                resultDF.unpersist();
              } else {
                println("### No common matched data for nozzle ###")
                nozzleDf.show()
              }
            }

            if (!timerseDf.rdd.isEmpty()) {
              println("new timer data");

              timerseDf.withColumnRenamed("ulProductionTime", "ul_production_time")
                .withColumnRenamed("Product_ID", "product_id")
                .withColumn("Date_Time", unix_timestamp(timerseDf("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time").withColumn("date_time", when(col("date_time").isNull, dformat.format(new Date())).otherwise(col("date_time")))
                .withColumnRenamed("User_name", "user_name")
                .withColumnRenamed("ulSetupTime", "ul_setup_time")
                .withColumnRenamed("ulIdleTime", "ul_idle_time")
                .withColumnRenamed("ulDiagnosticTime", "ul_diagnostic_time")
                .withColumnRenamed("ulWaitingForOperator", "ul_waiting_for_operator")
                .withColumnRenamed("ulWaitingForInterruptRecovery", "ul_waiting_for_interrupt_recovery")
                .withColumnRenamed("ulWaitingForBoardInWorkArea", "ul_waiting_for_board_in_workarea")
                .withColumn("split_plant_id", split(col("product_id")))
                .select("product_id", "date_time", "user_name", "ul_production_time", "ul_setup_time", "ul_idle_time", "ul_diagnostic_time", "ul_waiting_for_operator", "ul_waiting_for_interrupt_recovery", "ul_waiting_for_board_in_workarea", "split_plant_id")
                .registerTempTable("ea_pap_timers")

              if (debug) {
                println("### timers table ###")
                hiveContext.sql("select * from ea_pap_timers").show()
              }

              val query = new StringBuilder
              query.append("SELECT A.*, B.machine_name, B.plant, A.product_id, A.date_time, D.optel_schedule_wo, D.setup, ")
              query.append("D.sap_wo, D.side, D.assembly, D.wo_detail ")
              query.append("FROM ea_pap_timers A ")
              query.append("JOIN ea_equipmentoutline B ON A.user_name = B.equipment_make ")
              query.append("JOIN ea_wodetail D ON A.split_plant_id = D.optel_schedule_wo ")
              query.append("LIMIT " + propertiesBroadcast.value.getProperty("maxLimit"))
              val resultDF = hiveContext.sql(query.toString())

              if (debug) {
                println("### result timers data ###")
                resultDF.show()
              }

              if (!resultDF.rdd.isEmpty) {
                dumpTimersResultToThingworx(resultDF, propertiesBroadcast, to, from, subject, key, su)
                resultDF.unpersist();
              } else {
                println("### No common matched data for timers ###")
                timerseDf.show()
              }
            }

            if (!defectDf.rdd.isEmpty()) {
              println("new defects data");
              defectDf
                .withColumnRenamed("PLACING_PLANT_CODE", "placing_plant_code")
                .withColumnRenamed("ORDER_NO", "order_no")
                .withColumnRenamed("FAULT_BOARD_NO", "fault_board_no")
                .withColumnRenamed("ASSEM_SERIAL_NO", "assem_serial_no")
                .withColumnRenamed("REF_DESIGNATOR", "ref_designator")
                .withColumnRenamed("DEFECT_COUNT", "defect_count")
                .withColumnRenamed("CMPNT_PART_NO", "cmpnt_part_no")
                .withColumnRenamed("DEFECT_CODE", "defect_code")
                .withColumn("DEFECT_DATE", unix_timestamp(defectDf("DEFECT_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("DEFECT_DATE", "defect_date").withColumn("defect_date", when(col("defect_date").isNull, dformat.format(new Date())).otherwise(col("defect_date")))
                .withColumnRenamed("FAIL_TYPE", "fail_type").registerTempTable("ea_ftdefects")

              if (debug) {
                println("### defects table ###")
                hiveContext.sql("select * from ea_ftdefects").show()
              }

              val query = new StringBuilder
              query.append("select A.placing_plant_code, A.machine_name, A.order_no, A.side, A.fault_board_no, A.ref_designator, B.optel_schedule_wo, ")
              query.append("A.cmpnt_part_no, A.defect_date, A.defect_code, A.fail_type, A.assem_serial_no, A.defect_count, B.wo_detail, C.ref_id, C.head_spindle, C.head, ")
              query.append("C.spindle, concat(\"http://usmkeweb071.na.home.ra-int.com/IncuityPortal/MyEnterprise/Reports/Dashboards/Unit.ASP?Location=\",A.placing_plant_code,\"&SN=\",A.assem_serial_no) as sn_detail ")
              query.append("from ( select X.* , case ")
              query.append("case length(regexp_replace(placing_wc_code , '[^_]' , '')) ")
              query.append("when 0 then '' ")
              query.append("else split(X.placing_wc_code , '_')[length(regexp_replace(placing_wc_code , '[^_]' , '')) -1] ")
              query.append("end ")
              query.append("when 'TOP' then 'Top' ")
              query.append("when 'BOT' then 'Bottom' ")
              query.append("else '' end side, ")
              query.append("split(placing_wc_code , '_')[length(regexp_replace(placing_wc_code , '[^_]' , ''))] machine_name ")
              query.append("from ea_ftdefects X ) A ")
              query.append("left join ea_wodetail B on A.placing_plant_code = B.plant and A.order_no = B.sap_wo and A.side = B.side ")
              query.append("left join ea_progorderlist C on  A.placing_plant_code = C.plant and A.machine_name = C.machine ")
              query.append("and B.optel_schedule_wo = C.work_order and A.FAULT_BOARD_NO = C.silkscreen and A.REF_DESIGNATOR = C.ref_des ")
              query.append("LIMIT " + propertiesBroadcast.value.getProperty("maxLimit"))

              val resultDF = hiveContext.sql(query.toString())

              if (debug) {
                println("### result defects data ###")
                resultDF.show()
              }
              if (!resultDF.rdd.isEmpty) {

                dumpDefectsResultToThingworx(resultDF, propertiesBroadcast, to, from, subject, key, su)
                resultDF.unpersist();
              } else {
                println("### No common matched data for defects ###")
                defectDf.show()
              }
            }

            if (!slotDf.rdd.isEmpty()) {
              println("new slot data");

              //  updateTableTimestamp is the current time
              if ((java.lang.System.currentTimeMillis() - lastRefreshTime) > lookupRefreshIntervalMiliSec) {
                refreshMaterialCostTable(hiveContext, propertiesBroadcast)
                lastRefreshTime = java.lang.System.currentTimeMillis()
              }

              slotDf
                .withColumnRenamed("Product_ID", "product_id")
                .withColumn("split_plant_id", split(col("product_id")))
                .withColumnRenamed("User_Name", "user_name")
                .withColumn("Date_Time", unix_timestamp(slotDf("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time").withColumn("date_time", when(col("date_time").isNull, dformat.format(new Date())).otherwise(col("date_time")))
                .withColumnRenamed("ComponentName", "component_name")
                .withColumnRenamed("SlotNumber", "slot_number")
                .withColumnRenamed("ulCompMissing", "ul_comp_missing")
                .withColumnRenamed("ulSlotPicks", "ul_slot_picks")
                .withColumnRenamed("ulSlotPlacements", "ul_slot_placements")
                .withColumnRenamed("ulSlotVisionFailures", "ul_slot_vision_failures")
                .withColumnRenamed("ulPossibleMissing", "ul_possible_missing")
                .withColumnRenamed("ulSlotRejects", "ul_slot_rejects").registerTempTable("ea_pap_slot")

              if (debug) {
                println("### slot table ###")
                hiveContext.sql("select * from ea_pap_slot").show()
              }

              val query = new StringBuilder
              query.append("SELECT B.plant, B.machine_name, D.sap_wo, D.side, A.component_name, D.optel_schedule_wo, D.assembly, D.wo_detail, A.product_id, ")
              query.append("A.date_time,C.pins, C.nozzle1, C.package, A.slot_number, CAST(CAST(split(A.location, ',')[1] AS INT) + 1 AS string) track, ")
              query.append("A.ul_possible_missing, A.ul_slot_picks, A.ul_slot_placements, A.ul_slot_vision_failures, IF(E.unit_cost IS NOT NULL, E.unit_cost, 0.20) as unit_cost, ")
              query.append("CONCAT ( CAST(A.slot_number AS STRING) ,\".\" ,CAST(CAST(split(A.location, ',')[2] AS INT) + 1 AS string)) slot ")
              query.append("FROM ea_pap_slot A ")
              query.append("JOIN ea_equipmentoutline B ON A.user_name = B.equipment_make ")
              query.append("JOIN ea_comptype C ON B.machine_name = C.machine AND B.plant = C.plant AND B.head_1 = C.head_type AND A.component_name = C.part_number ")
              query.append("JOIN ea_wodetail D ON A.split_plant_id = D.optel_schedule_wo ")
              query.append("LEFT JOIN material_cost E ON B.plant = E.plant AND A.component_name = E.material ")
              query.append("LIMIT " + propertiesBroadcast.value.getProperty("maxLimit"))

              val resultDF = hiveContext.sql(query.toString())

              if (debug) {
                println("### result slot data ###")
                resultDF.show()
              }
              if (!resultDF.rdd.isEmpty) {
                dumpSlotResultToThingworx(resultDF, propertiesBroadcast, to, from, subject, key, su)
                resultDF.unpersist();
              } else {
                println("### No common matched data for slot ###")
                slotDf.show()
              }
            }
            parsedRdd.unpersist(true)
          }else{
            loginUsingKeytab(properties)
          }
        } catch {
          case all: Exception =>
            if (all.getCause != null && all.getCause.toString().equalsIgnoreCase("kafka.common.OffsetOutOfRangeException")) {
              offsetStore.deleteOffsets(topic)
            } else {
              all.printStackTrace()
            }
        }
      }

      //Saving offsets
      offsetStore.saveOffsets(dataStream, "")
    } catch {
      case e1: Exception =>
        e1.printStackTrace()
      // Gateway.sendEmail(to, from, subject, e1, key)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * calling REST API endpoints for dumping nozzle data to Thingworx
   */

  @throws(classOf[Exception])
  def dumpNozzleResultToThingworx(dataframe: DataFrame, propertiesBroadcast: Broadcast[Properties], to: String, from: String, subject: String, key: String, su: SparkUtils) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting nozzle data to Thingworx : " + dateFormat.format(date) + " ###")

    try {
      if (dataframe != null) {
        val resultRdd = dataframe.map {
          x =>

            val sb = new StringBuilder
            sb.append("""{""")
            sb.append(""""table_name" : """" + """nozzle""")
            sb.append("""","machine_name" : """ + getStingValues(x, "machine_name"))
            sb.append(""","product_id" : """ + getStingValues(x, "product_id"))
            sb.append(""","optel_schedule_wo": """ + getStingValues(x, "optel_schedule_wo"))
            sb.append(""","date_time" : """ + getStingValues(x, "date_time"))
            sb.append(""","plant" : """ + getStingValues(x, "plant"))
            sb.append(""","sap_wo" : """ + getStingValues(x, "sap_wo"))
            sb.append(""","side" : """ + getStingValues(x, "side"))
            sb.append(""","assembly" : """ + getStingValues(x, "assembly"))
            sb.append(""","wo_detail" : """ + getStingValues(x, "wo_detail"))
//            sb.append(""","sn_detail" : """ + null)
//            sb.append(""","defect_refid" : """ + null)
//            sb.append(""","defect_head_spindle" : """ + null)
//            sb.append(""","defect_head" : """ + null)
//            sb.append(""","defect_spindle" : """ + null)
            sb.append(""","nozzle_ch_hole" : """ + getStingValues(x, "nozzle_ch_hole"))
            sb.append(""","nozzle_mach_ch_hole" : """ + getStingValues(x, "nozzle_mach_ch_hole"))
            sb.append(""","tooltypenozzle" : """ + getStingValues(x, "tooltypenozzle"))
//            sb.append(""","component_name" : """ + null)
//            sb.append(""","slot" : """ + null)
//            sb.append(""","slot_nozzle1" : """ + null)
//            sb.append(""","slot_package" : """ + null)
//            sb.append(""","slot_number" : """ + null)
//            sb.append(""","slot_track" : """ + null)
//            sb.append(""","defect_code" : """ + null)
//            sb.append(""","defect_fail_type" : """ + null)
//            sb.append(""","defect_count" : """ + null)
//            sb.append(""","setup" : """ + null)
//            sb.append(""","timer_diagnostic_time" : """ + null)
//            sb.append(""","timer_setup_time" : """ + null)
//            sb.append(""","timer_waiting_for_operator" : """ + null)
//            sb.append(""","timer_waiting_for_board_workarea" : """ + null)
//            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + null)
//            sb.append(""","timer_idle_time" : """ + null)
//            sb.append(""","timer_production_time" : """ + null)
//            sb.append(""","slot_vision_failures" : """ + null)
            sb.append(""","picks" : """ + x.getAs[Double]("ul_picks"))
            sb.append(""","placements" : """ + x.getAs[Double]("ul_placements"))
            sb.append(""","possible_missing" : """ + x.getAs[Double]("ul_possible_missing"))
            sb.append(""","rejects" : """ + x.getAs[String]("ul_rejects"))
            sb.append(""","comp_missing" : """ + x.getAs[Double]("ul_comp_missing"))
//            sb.append(""","slot_pins" : """ + null)
//            sb.append(""","unit_cost" : """ + null)
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        // su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the Thingworx
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitThingworx").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToThingworx(postString, propertiesBroadcast)
          }
        }
      }
    } catch {
      case e1: Exception =>
      // Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  /**
   * calling REST API endpoints for dumping timers data to Thingworx
   */

  @throws(classOf[Exception])
  def dumpTimersResultToThingworx(dataframe: DataFrame, propertiesBroadcast: Broadcast[Properties], to: String, from: String, subject: String, key: String, su: SparkUtils) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting timers data to Thingworx : " + dateFormat.format(date) + " ###")

    try {
      if (dataframe != null) {
        val resultRdd = dataframe.map {
          x =>

            val sb = new StringBuilder
            sb.append("""{""")
            sb.append(""""table_name" : """" + """timers""")
            sb.append("""","machine_name" : """ + getStingValues(x, "machine_name"))
            sb.append(""","product_id" : """ + getStingValues(x, "product_id"))
            sb.append(""","optel_schedule_wo": """ + getStingValues(x, "optel_schedule_wo"))
            sb.append(""","date_time" : """ + getStingValues(x, "date_time"))
            sb.append(""","plant" : """ + getStingValues(x, "plant"))
            sb.append(""","sap_wo" : """ + getStingValues(x, "sap_wo"))
            sb.append(""","side" : """ + getStingValues(x, "side"))
            sb.append(""","assembly" : """ + getStingValues(x, "assembly"))
            sb.append(""","wo_detail" : """ + getStingValues(x, "wo_detail"))
//            sb.append(""","sn_detail" : """ + null)
//            sb.append(""","defect_refid" : """ + null)
//            sb.append(""","defect_head_spindle" : """ + null)
//            sb.append(""","defect_head" : """ + null)
//            sb.append(""","defect_spindle" : """ + null)
//            sb.append(""","nozzle_ch_hole" : """ + null)
//            sb.append(""","nozzle_mach_ch_hole" : """ + null)
//            sb.append(""","tooltypenozzle" : """ + null)
//            sb.append(""","component_name" : """ + null)
//            sb.append(""","slot" : """ + null)
//            sb.append(""","slot_nozzle1" : """ + null)
//            sb.append(""","slot_package" : """ + null)
//            sb.append(""","slot_number" : """ + null)
//            sb.append(""","slot_track" : """ + null)
//            sb.append(""","defect_code" : """ + null)
//            sb.append(""","defect_fail_type" : """ + null)
//            sb.append(""","defect_count" : """ + null)
            sb.append(""","setup" : """ + x.getAs[Double]("setup"))
            sb.append(""","timer_diagnostic_time" : """ + x.getAs[Double]("ul_diagnostic_time"))
            sb.append(""","timer_setup_time" : """ + x.getAs[Double]("ul_setup_time"))
            sb.append(""","timer_waiting_for_operator" : """ + x.getAs[Double]("ul_waiting_for_operator"))
            sb.append(""","timer_waiting_for_board_workarea" : """ + x.getAs[Double]("ul_waiting_for_board_in_workarea"))
            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + x.getAs[Double]("ul_waiting_for_interrupt_recovery"))
            sb.append(""","timer_idle_time" : """ + x.getAs[Double]("ul_idle_time"))
            sb.append(""","timer_production_time" : """ + x.getAs[Double]("ul_production_time"))
//            sb.append(""","slot_vision_failures" : """ + null)
//            sb.append(""","picks" : """ + null)
//            sb.append(""","placements" : """ + null)
//            sb.append(""","possible_missing" : """ + null)
//            sb.append(""","rejects" : """ + null)
//            sb.append(""","comp_missing" : """ + null)
//            sb.append(""","slot_pins" : """ + null)
//            sb.append(""","unit_cost" : """ + null)
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        // su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the Thingworx
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitThingworx").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToThingworx(postString, propertiesBroadcast)
          }
        }

      }
    } catch {
      case e1: Exception =>
      //  Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  /**
   * calling REST API endpoints for dumping defects data to Thingworx
   */

  @throws(classOf[Exception])
  def dumpDefectsResultToThingworx(dataframe: DataFrame, propertiesBroadcast: Broadcast[Properties], to: String, from: String, subject: String, key: String, su: SparkUtils) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting defects data to Thingworx : " + dateFormat.format(date) + " ###")

    try {
      if (dataframe != null) {
        val resultRdd = dataframe.map {
          x =>
            val sb = new StringBuilder
            sb.append("""{""")
            sb.append(""""table_name" : """" + """defects""")
            sb.append("""","machine_name" : """ + getStingValues(x, "machine_name"))
//            sb.append(""","product_id" : """ + null)
            sb.append(""","optel_schedule_wo": """ + getStingValues(x, "optel_schedule_wo"))
            sb.append(""","date_time" : """ + getStingValues(x, "defect_date"))
            sb.append(""","plant" : """ + getStingValues(x, "placing_plant_code"))
            sb.append(""","sap_wo" : """ + getStingValues(x, "order_no"))
            sb.append(""","side" : """ + getStingValues(x, "side"))
            sb.append(""","assembly" : """ + getStingValues(x, "assem_serial_no"))
            sb.append(""","wo_detail" : """ + getStingValues(x, "wo_detail"))
            sb.append(""","sn_detail" : """ + getStingValues(x, "sn_detail"))
            sb.append(""","defect_refid" : """ + getStingValues(x, "ref_id"))
            sb.append(""","defect_head_spindle" : """ + getStingValues(x, "head_spindle"))
            sb.append(""","defect_head" : """ + getStingValues(x, "head"))
            sb.append(""","defect_spindle" : """ + getStingValues(x, "spindle"))
//            sb.append(""","nozzle_ch_hole" : """ + null)
//            sb.append(""","nozzle_mach_ch_hole" : """ + null)
//            sb.append(""","tooltypenozzle" : """ + null)
            sb.append(""","component_name" : """ + getStingValues(x, "cmpnt_part_no"))
//            sb.append(""","slot" : """ + null)
//            sb.append(""","slot_nozzle1" : """ + null)
//            sb.append(""","slot_package" : """ + null)
//            sb.append(""","slot_number" : """ + null)
//            sb.append(""","slot_track" : """ + null)
            sb.append(""","defect_code" : """ + getStingValues(x, "defect_code"))
            sb.append(""","defect_fail_type" : """ + getStingValues(x, "fail_type"))
            sb.append(""","defect_count" : """ + x.getAs[Double]("defect_count"))
//            sb.append(""","setup" : """ + null)
//            sb.append(""","timer_diagnostic_time" : """ + null)
//            sb.append(""","timer_setup_time" : """ + null)
//            sb.append(""","timer_waiting_for_operator" : """ + null)
//            sb.append(""","timer_waiting_for_board_workarea" : """ + null)
//            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + null)
//            sb.append(""","timer_idle_time" : """ + null)
//            sb.append(""","timer_production_time" : """ + null)
//            sb.append(""","slot_vision_failures" : """ + null)
//            sb.append(""","picks" : """ + null)
//            sb.append(""","placements" : """ + null)
//            sb.append(""","possible_missing" : """ + null)
//            sb.append(""","rejects" : """ + null)
//            sb.append(""","comp_missing" : """ + null)
//            sb.append(""","slot_pins" : """ + null)
//            sb.append(""","unit_cost" : """ + null)
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        //su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the Thingworx
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitThingworx").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToThingworx(postString, propertiesBroadcast)
          }
        }

      }
    } catch {
      case e1: Exception =>
      //Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  /**
   * calling REST API endpoints for dumping slot data to Thingworx
   */

  @throws(classOf[Exception])
  def dumpSlotResultToThingworx(dataframe: DataFrame, propertiesBroadcast: Broadcast[Properties], to: String, from: String, subject: String, key: String, su: SparkUtils) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting slot data to Thingworx : " + dateFormat.format(date) + " ###")

    try {
      if (dataframe != null) {
        val resultRdd = dataframe.map {
          x =>

            val sb = new StringBuilder
            sb.append("""{""")
            sb.append(""""table_name" : """" + """slot""")
            sb.append("""","machine_name" : """ + getStingValues(x, "machine_name"))
            sb.append(""","product_id" : """ + getStingValues(x, "product_id"))
            sb.append(""","optel_schedule_wo": """ + getStingValues(x, "optel_schedule_wo"))
            sb.append(""","date_time" : """ + getStingValues(x, "date_time"))
            sb.append(""","plant" : """ + getStingValues(x, "plant"))
            sb.append(""","sap_wo" : """ + getStingValues(x, "sap_wo"))
            sb.append(""","side" : """ + getStingValues(x, "side"))
            sb.append(""","assembly" : """ + getStingValues(x, "assembly"))
            sb.append(""","wo_detail" : """ + getStingValues(x, "wo_detail"))
//            sb.append(""","sn_detail" : """ + null)
//            sb.append(""","defect_refid" : """ + null)
//            sb.append(""","defect_head_spindle" : """ + null)
//            sb.append(""","defect_head" : """ + null)
//            sb.append(""","defect_spindle" : """ + null)
//            sb.append(""","nozzle_ch_hole" : """ + null)
//            sb.append(""","nozzle_mach_ch_hole" : """ + null)
//            sb.append(""","tooltypenozzle" : """ + null)
            sb.append(""","component_name" : """ + getStingValues(x, "component_name"))
            sb.append(""","slot" : """ + getStingValues(x, "slot"))
            sb.append(""","slot_nozzle1" : """ + getStingValues(x, "nozzle1"))
            sb.append(""","slot_package" : """ + getStingValues(x, "package"))
            sb.append(""","slot_number" : """ + getStingValues(x, "slot_number"))
            sb.append(""","slot_track" : """ + getStingValues(x, "track"))
//            sb.append(""","defect_code" : """ + null)
//            sb.append(""","defect_fail_type" : """ + null)
//            sb.append(""","defect_count" : """ + null)
//            sb.append(""","setup" : """ + null)
//            sb.append(""","timer_diagnostic_time" : """ + null)
//            sb.append(""","timer_setup_time" : """ + null)
//            sb.append(""","timer_waiting_for_operator" : """ + null)
//            sb.append(""","timer_waiting_for_board_workarea" : """ + null)
//            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + null)
//            sb.append(""","timer_idle_time" : """ + null)
//            sb.append(""","timer_production_time" : """ + null)
            sb.append(""","slot_vision_failures" : """ + x.getAs[Double]("ul_slot_vision_failures"))
            sb.append(""","picks" : """ + x.getAs[Double]("ul_slot_picks"))
            sb.append(""","placements" : """ + x.getAs[Double]("ul_slot_placements"))
            sb.append(""","possible_missing" : """ + x.getAs[Double]("ul_possible_missing"))
//            sb.append(""","rejects" : """ + null)
//            sb.append(""","comp_missing" : """ + null)
            sb.append(""","slot_pins" : """ + x.getAs[Double]("pins"))
            sb.append(""","unit_cost" : """ + x.getAs[Double]("unit_cost"))
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        //su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the Thingworx
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitThingworx").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToThingworx(postString, propertiesBroadcast)
          }
        }

      }
    } catch {
      case e1: Exception =>
      // Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  def postToThingworx(postString: String, propertiesBroadcast: Broadcast[Properties]): Unit = {

    val inputJson = "{\"inputJson\":" + postString + "}"
    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    val post = new HttpPost(propertiesBroadcast.value.getProperty("thingworxUrl"))
    post.addHeader("Content-Type", "application/json")
    post.addHeader("appKey", propertiesBroadcast.value.getProperty("appKey"))
    post.setEntity(new StringEntity(inputJson))
    val startTime = System.nanoTime();
    val Response = Gateway.getPostContent(post)
    val endTime = System.nanoTime();
    if (!Response.isEmpty() && Response.contains("failed")) {
      println("### Thingworx API is down! ###" + Response)
    } else {
      println("### data posted to Thingworx : " + dateFormat.format(date) + " ###")
    }
    println("total time to push data on thingworx : " + (endTime - startTime) / 1000000000)
  }

  def loginUsingKeytab(properties: Properties): Unit = {

    println("login using keytab if rdd empty")
    val configuration = new Configuration
    configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.getCurrentUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS)

    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      properties.getProperty("hadoop.kerberos.principal"), properties.getProperty("spark.yarn.keytab"))

  }

  /*
	 * creating singleton hive context
	 *
	 * @param SparkContext
	 *
	 * @param Properties
	 */
  def getHiveContext(sparkContext: SparkContext, properties: Properties): HiveContext = {
    synchronized {

      val configuration = new Configuration
      configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.getCurrentUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS)

      if (instance == null) {
        System.setProperty("hive.metastore.uris", properties.getProperty("hive.metastore.uris"));
        if (properties.getProperty("kerberosSecurity").toBoolean) {
          System.setProperty("hive.metastore.sasl.enabled", "true")
          System.setProperty("hive.metastore.kerberos.keytab.file", sparkContext.getConf.get("spark.yarn.keytab"))
          System.setProperty("hive.security.authorization.enabled", "false")
          System.setProperty("hive.metastore.kerberos.principal", properties.getProperty("hive.metastore.kerberos.principal"))
          System.setProperty("hive.metastore.execute.setugi", "true")
        }

        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          properties.getProperty("hadoop.kerberos.principal"), sparkContext.getConf.get("spark.yarn.keytab"))
          .doAs(new PrivilegedExceptionAction[HiveContext]() {
            @Override
            def run(): HiveContext = {
              instance = new HiveContext(sparkContext)
              instance.setConf("spark.sql.parquet.writeLegacyFormat", "true")

              instance.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
              instance.setConf("hive.exec.dynamic.partition", "true")
              instance.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
              instance
            }
          })

      }

      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        properties.getProperty("hadoop.kerberos.principal"), sparkContext.getConf.get("spark.yarn.keytab"))
        .doAs(new PrivilegedExceptionAction[HiveContext]() {
          @Override
          def run(): HiveContext = {
            instance
          }
        })

    }
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
  def loadXML(jsonRDD: RDD[String], hdfsFileName: String, topic: String): (RDD[String]) = {

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
              // jmapHeader.put("uuid", java.util.UUID.randomUUID.toString())
              jmapHeader.put("xml_status", xmlStatus)
              jmapHeader.put("xml_tag_id", tagId)
              jmapHeader.put("bicoe_load_dttm", currentDateTime)
              jmapHeader.put("file_timestamp", timeStamp)
              jmapHeader.put("hdfs_file_name", hdfsFileName)
              jmapHeader.put("file_name", filename)
              jmapHeader.put("kafka_topic", topic)
              jmapHeader.put("xml_tag_name", xmlTagName)
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
                  // jmapDetail.put("uuid", java.util.UUID.randomUUID.toString())
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

  def getStingValues(x: Row, column: String): String = if (x.getAs[String](column) != null) "\"" + x.getAs[String](column) + "\"" else null

  /*
	 * refresh material cost table
	 *
	 * @param SparkContext
	 *
	 * @param Properties
	 */
  def refreshMaterialCostTable(hiveContext : HiveContext, propertiesBroadcast: Broadcast[Properties]): Unit = {
    println("### refresh material cost table ###")
    hiveContext.sql("select REGEXP_REPLACE(material, \"^0+\", '') as material, plant, unit_cost from " + propertiesBroadcast.value.getProperty("env") + propertiesBroadcast.value.getProperty("material_cost_table")).registerTempTable("material_cost")
  }
}