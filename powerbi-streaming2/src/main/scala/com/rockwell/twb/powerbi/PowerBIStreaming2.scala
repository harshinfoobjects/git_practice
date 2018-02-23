package com.rockwell.twb.powerbi

import java.io.FileInputStream
import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext

import com.google.gson.JsonParser
import com.rockwell.twb.kafka.KafkaSink
import com.rockwell.twb.stores.MySqlOffsetsStore
import com.rockwell.twb.util.Gateway
import com.rockwell.twb.util.SparkUtils

import kafka.serializer.StringDecoder

object PowerBIStreaming2 {

  val properties = new Properties()
  @transient private var instance: HiveContext = _
  var broadcastedEquipmentOnline: Broadcast[DataFrame] = null
  var broadcastedInfo: Broadcast[DataFrame] = null
  var broadcastedWoDetail: Broadcast[DataFrame] = null
  var broadcastedProgorderList: Broadcast[DataFrame] = null
  var broadcastedCompType: Broadcast[DataFrame] = null
  var lastRefreshTime = -1L;
  var lookupRefreshInterval = 0L;

  //Required for email properties
  var to = ""
  var from = ""
  var cc = ""
  var subject = ""
  var key = ""
  var body = ""
  
  /*
	* main function
	*/
  def main(args: Array[String]): Unit = {
    properties.load(new FileInputStream(args(0)))

    //Email Properties
    to = properties.getProperty("to_List");
    from = properties.getProperty("from");
    subject = properties.getProperty("subject");
    key = properties.getProperty("sendGridKey");
    var ssc: StreamingContext = null

    try {
      val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
        "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))

      val split = udf(
        (location: String) => {
          val locationArr = location.split("\\.")
          locationArr(0)
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

      Logger.getRootLogger().setLevel(Level.WARN)
      Logger.getLogger("org").setLevel(Level.WARN);
      Logger.getLogger("akka").setLevel(Level.WARN);

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

      ssc = su.createSparkStreamingContext(sc)

      // environment variable
      val env = properties.getProperty("env")

      val outputTopic = properties.getProperty("outletKafkaTopic")

      val topicCommon = properties.getProperty("inletKafkaTopicCommon")
      val topicC4 = properties.getProperty("inletKafkaTopicC4")

      val debug = properties.getProperty("debug").toBoolean
      val debug_common = properties.getProperty("debug_common").toBoolean

      var lookupRefreshIntervalMiliSec = properties.getProperty("lookupRefreshInterval").toLong * 1000

      val offsetStoreCommon = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, env + properties.getProperty("jdbcDatabase"))
      val dataStreamCommon = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStoreCommon, topicCommon, "_" + topicC4)

      val offsetStoreC4 = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, env + properties.getProperty("jdbcDatabase"))
      val dataStreamC4 = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStoreC4, topicC4, "")

      val powerBIApi = dataStreamC4.context.sparkContext.broadcast(properties.getProperty("powerBIApi"))
      val kafkaSink = dataStreamC4.context.sparkContext.broadcast(KafkaSink(props))
      val propertiesBroadcast = dataStreamC4.context.sparkContext.broadcast(properties)
      val hiveContext = getHiveContext(ssc.sparkContext, propertiesBroadcast)

      println("loading common tables first time")

      // load latest records from raw layer elec_assy.ea_equipmentoutline table
      if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("slot") ||
        propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("timers") ||
        propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("nozzle")) {

        val equipDfHive = hiveContext.sql("select tmp.machine_name, tmp.equipment_make, tmp.equipment_model,"
          + " tmp.equipment_type, tmp.head_1, tmp.head_2, tmp.plant from (select machine_name, equipment_make, equipment_model,equipment_type, head_1,"
          + " head_2, plant, row_number() over (partition by machine_name , plant order by hdfs_file_name desc) row_num from " + env +  properties.getProperty("ea_equipmentoutline_table") + " ) tmp"
          + " where tmp.row_num = 1")

        broadcastedEquipmentOnline = ssc.sparkContext.broadcast(equipDfHive.withColumn("updateTableTimestamp", lit(java.lang.System.currentTimeMillis())))
        if (broadcastedEquipmentOnline != null && broadcastedEquipmentOnline.value != null)
            broadcastedEquipmentOnline.value.registerTempTable("ea_equipmentoutline")
        if (debug) {
          println("###  ea_equipmentoutline ###")
          hiveContext.sql("select * from ea_equipmentoutline").show()

        }
      }

      // load latest records from raw layer elec_assy.ea_wodetail table
      if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("nozzle")) {
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

      // load latest records from raw layer elec_assy.ea_progorderlist table
      if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("defects")) {

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
      }

      // load latest records from raw layer elec_assy.ea_comptype table
      if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("slot")) {
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
      }

      refreshMaterialCostTable(ssc.sparkContext, propertiesBroadcast)
      refreshLocPlantXref(ssc.sparkContext, propertiesBroadcast)
      if (debug) {
        println("###  elctrnc_assy_loc_plant_xref ###")
        hiveContext.sql("select * from elctrnc_assy_loc_plant_xref").show()

      }

      val mappedDataStreamCommon = dataStreamCommon.map(_._2);
      mappedDataStreamCommon.foreachRDD { rdd =>
        try {

          if (!rdd.isEmpty()) {

            println("new common data");

            val sqlContext = SQLContext.getOrCreate(rdd.context)
            import sqlContext.implicits._
            val parser = new JsonParser
            val sparkContext = rdd.sparkContext
            val hiveContext = getHiveContext(sparkContext, propertiesBroadcast)

            var infoDf = sqlContext.emptyDataFrame
            var equipmentonlineDf = sqlContext.emptyDataFrame
            var progorderListDf = sqlContext.emptyDataFrame
            var compTypeDf = sqlContext.emptyDataFrame

            if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("nozzle")) {
              infoDf = sqlContext.read.json(rdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_pap_info_header"))))

            }
            if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("slot") ||
              propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("timers") ||
              propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("nozzle")) {
              equipmentonlineDf = sqlContext.read.json(rdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_equipmentoutline_header"))))
            }
            val wodetailDF = sqlContext.read.json(rdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_wodetail_header"))))

            if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("defects")) {
              progorderListDf = sqlContext.read.json(rdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_progorderlist_header"))))
            }
            if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("slot")) {
              compTypeDf = sqlContext.read.json(rdd.filter(ele => ele.contains(propertiesBroadcast.value.getProperty("ea_comptype_header"))))
            }

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

              if (!compTypeDf.rdd.isEmpty()) {
                println("###  incoming compType Data ###")
                compTypeDf.show()
              } else {
                println("### No data for compType ###")
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
                if (debug_common) {
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
                if (debug_common) {
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
                .withColumn("wo_detail", concatenated($"sap_wo", $"PLANT"))
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
                if (debug_common) {
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

            //updateTableTimestamp is the current time
            if ((java.lang.System.currentTimeMillis() - lastRefreshTime) > lookupRefreshIntervalMiliSec) {
              refreshMaterialCostTable(rdd.sparkContext, propertiesBroadcast)
              refreshLocPlantXref(rdd.sparkContext, propertiesBroadcast)
              lastRefreshTime = java.lang.System.currentTimeMillis()
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
                if (debug_common) {
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
                if (debug_common) {
                  println("### broadcasted ea_comptype ###")
                  hiveContext.sql("select updateTableTimestamp, count(*) from ea_comptype group by updateTableTimestamp").show()
                }
              } else {
                println("### after join ea_info its empty ###")
                compTypeDf.show();
              }
            }
          }
        } catch {
          case all: Exception =>
            if (all.getCause != null && all.getCause.toString().equalsIgnoreCase("kafka.common.OffsetOutOfRangeException")) {
              offsetStoreCommon.deleteOffsets(topicCommon + "_" + topicC4)
            } else {
              all.printStackTrace()
              Gateway.sendEmail(to, from, subject, all, key)
            }
        }
      }

      val mappedDataStreamC4 = dataStreamC4.map(_._2);
      mappedDataStreamC4.foreachRDD { rdd =>
        try {

          if (!rdd.isEmpty()) {
            //sleep for 10 secs for common stream to load new data
            Thread.sleep(10000);
            val sqlContext = SQLContext.getOrCreate(rdd.context)
            import sqlContext.implicits._
            val parser = new JsonParser
            val sparkContext = rdd.sparkContext
            val hiveContext = getHiveContext(sparkContext, propertiesBroadcast)

            val c4Df = sqlContext.read.json(rdd)

            if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("nozzle")) {
              println("new nozzle data");
              c4Df.withColumnRenamed("Product_ID", "product_id")
                .withColumnRenamed("Date_Time", "date_time")
                .withColumnRenamed("User_name", "user_name")
                .withColumnRenamed("location", "location")
                .withColumnRenamed("ulpicks", "ul_picks")
                .withColumnRenamed("ulplacements", "ul_placements")
                .withColumnRenamed("ulcompmissing", "ul_comp_missing")
                .withColumnRenamed("ulPossibleMissing", "ul_possible_missing")
                .withColumnRenamed("ulRejects", "ul_rejects")
                .withColumn("split_plant_id", split($"product_id"))
                .select("xml_tag_name","product_id", "date_time", "user_name", "location", "tooltype", "ul_picks", "ul_placements", "ul_comp_missing", "ul_possible_missing", "ul_rejects", "split_plant_id")
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
              query.append("JOIN elctrnc_assy_loc_plant_xref K ON split(A.xml_tag_name,'-')[2] = K.location ")
              query.append("JOIN ea_equipmentoutline B ON A.user_name = B.equipment_make AND K.plant=B.plant ")
              query.append("JOIN ea_pap_info C ON A.tooltype = C.crc_id AND C.machine = B.machine_name and C.plant_code = B.plant ")
              query.append("JOIN ea_wodetail D ON A.split_plant_id = D.optel_schedule_wo ")
              query.append("LIMIT " + propertiesBroadcast.value.getProperty("maxLimit"))

              val resultDF = hiveContext.sql(query.toString())

              if (debug) {
                println("### result nozzle data ###")
                resultDF.show()
              }
              if (!resultDF.rdd.isEmpty) {
                dumpNozzleResultToPowerBI(resultDF, powerBIApi, propertiesBroadcast, su, propertiesBroadcast.value.getProperty("outletKafkaTopic"), kafkaSink)
                resultDF.unpersist();
              } else {
                println("### No common matched data for nozzle ###")
                c4Df.show()
              }

            } else if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("timers")) {

              println("new timer data");

              c4Df.withColumnRenamed("ulProductionTime", "ul_production_time")
                .withColumnRenamed("Product_ID", "product_id")
                .withColumnRenamed("Date_Time", "date_time")
                .withColumnRenamed("User_name", "user_name")
                .withColumnRenamed("ulSetupTime", "ul_setup_time")
                .withColumnRenamed("ulIdleTime", "ul_idle_time")
                .withColumnRenamed("ulDiagnosticTime", "ul_diagnostic_time")
                .withColumnRenamed("ulWaitingForOperator", "ul_waiting_for_operator")
                .withColumnRenamed("ulWaitingForInterruptRecovery", "ul_waiting_for_interrupt_recovery")
                .withColumnRenamed("ulWaitingForBoardInWorkArea", "ul_waiting_for_board_in_workarea")
                .withColumn("split_plant_id", split($"product_id"))
                .select("xml_tag_name", "product_id", "date_time", "user_name", "ul_production_time", "ul_setup_time", "ul_idle_time", "ul_diagnostic_time", "ul_waiting_for_operator", "ul_waiting_for_interrupt_recovery", "ul_waiting_for_board_in_workarea", "split_plant_id")
                .registerTempTable("ea_pap_timers")

              if (debug) {
                println("### timers table ###")
                hiveContext.sql("select * from ea_pap_timers").show()
              }

              val query = new StringBuilder
              query.append("SELECT A.*, B.machine_name, B.plant, A.product_id, A.date_time, D.optel_schedule_wo, D.setup, ")
              query.append("D.sap_wo, D.side, D.assembly, D.wo_detail ")
              query.append("FROM ea_pap_timers A ")
              query.append("JOIN elctrnc_assy_loc_plant_xref K ON split(A.xml_tag_name,'-')[2] = K.location ")
              query.append("JOIN ea_equipmentoutline B ON A.user_name = B.equipment_make AND K.plant=B.plant ")
              query.append("JOIN ea_wodetail D ON A.split_plant_id = D.optel_schedule_wo ")
              query.append("LIMIT " + propertiesBroadcast.value.getProperty("maxLimit"))
              val resultDF = hiveContext.sql(query.toString())

              if (debug) {
                println("### result timers data ###")
                resultDF.show()
              }

              if (!resultDF.rdd.isEmpty) {
                dumpTimersResultToPowerBI(resultDF, powerBIApi, propertiesBroadcast, su, propertiesBroadcast.value.getProperty("outletKafkaTopic"), kafkaSink)
                resultDF.unpersist();
              } else {
                println("### No common matched data for timers ###")
                c4Df.show()
              }

            } else if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("defects")) {

              println("new defects data");

              c4Df
                .withColumnRenamed("PLACING_PLANT_CODE", "placing_plant_code")
                .withColumnRenamed("ORDER_NO", "order_no")
                .withColumnRenamed("FAULT_BOARD_NO", "fault_board_no")
                .withColumnRenamed("ASSEM_SERIAL_NO", "assem_serial_no")
                .withColumnRenamed("REF_DESIGNATOR", "ref_designator")
                .withColumnRenamed("DEFECT_COUNT", "defect_count")
                .withColumnRenamed("CMPNT_PART_NO", "cmpnt_part_no")
                .withColumnRenamed("DEFECT_CODE", "defect_code")
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

                dumpDefectsResultToPowerBI(resultDF, powerBIApi, propertiesBroadcast, su, propertiesBroadcast.value.getProperty("outletKafkaTopic"), kafkaSink)
                resultDF.unpersist();
              } else {
                println("### No common matched data for defects ###")
                c4Df.show()
              }

            } else if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("slot")) {

              println("new slot data");

              c4Df
                .withColumnRenamed("Product_ID", "product_id")
                .withColumn("split_plant_id", split($"product_id"))
                .withColumnRenamed("User_Name", "user_name")
                .withColumnRenamed("Date_Time", "date_time")
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
              query.append("JOIN elctrnc_assy_loc_plant_xref K ON split(A.xml_tag_name,'-')[2] = K.location ")
              query.append("JOIN ea_equipmentoutline B ON A.user_name = B.equipment_make AND K.plant=B.plant ")
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
                dumpSlotResultToPowerBI(resultDF, powerBIApi, propertiesBroadcast, su, propertiesBroadcast.value.getProperty("outletKafkaTopic"), kafkaSink)
                resultDF.unpersist();
              } else {
                println("### No common matched data for slot ###")
                c4Df.show()
              }

            } else {
              println("no new  data for any type - must not happen");

            }
          } else{
            loginUsingKeytab(propertiesBroadcast)
          }
        } catch {
          case all: Exception =>
            if (all.getCause != null && all.getCause.toString().equalsIgnoreCase("kafka.common.OffsetOutOfRangeException")) {
              offsetStoreC4.deleteOffsets(topicC4)
            } else {
              all.printStackTrace()
              Gateway.sendEmail(to, from, subject, all, key)
            }
        }
      }

      //Saving offsets
      offsetStoreC4.saveOffsets(dataStreamC4, "")
      //Saving offsets
      offsetStoreCommon.saveOffsets(dataStreamCommon, "_" + topicC4)
    } catch {
      case e1: Exception =>
        e1.printStackTrace()
        Gateway.sendEmail(to, from, subject, e1, key)
    }

    sys.addShutdownHook {
      println("------------------Running Shutdown Hook------------------")
     Gateway.sendEmail(to, from, "C4_" + properties.getProperty("inletKafkaTopicC4") + " SHUT DOWN", null, key)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * calling REST API endpoints for dumping nozzle data to PowerBI
   */

  @throws(classOf[Exception])
  def dumpNozzleResultToPowerBI(dataframe: DataFrame, powerBIApi: Broadcast[String], propertiesBroadcast: Broadcast[Properties], su: SparkUtils, outputTopic: String, kafkaSink: Broadcast[KafkaSink]) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting nozzle data to PowerBI : " + dateFormat.format(date) + " ###")

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
            sb.append(""","sn_detail" : """ + null)
            sb.append(""","defect_refid" : """ + null)
            sb.append(""","defect_head_spindle" : """ + null)
            sb.append(""","defect_head" : """ + null)
            sb.append(""","defect_spindle" : """ + null)
            sb.append(""","nozzle_ch_hole" : """ + getStingValues(x, "nozzle_ch_hole"))
            sb.append(""","nozzle_mach_ch_hole" : """ + getStingValues(x, "nozzle_mach_ch_hole"))
            sb.append(""","tooltypenozzle" : """ + getStingValues(x, "tooltypenozzle"))
            sb.append(""","component_name" : """ + null)
            sb.append(""","slot" : """ + null)
            sb.append(""","slot_nozzle1" : """ + null)
            sb.append(""","slot_package" : """ + null)
            sb.append(""","slot_number" : """ + null)
            sb.append(""","slot_track" : """ + null)
            sb.append(""","defect_code" : """ + null)
            sb.append(""","defect_fail_type" : """ + null)
            sb.append(""","defect_count" : """ + null)
            sb.append(""","setup" : """ + null)
            sb.append(""","timer_diagnostic_time" : """ + null)
            sb.append(""","timer_setup_time" : """ + null)
            sb.append(""","timer_waiting_for_operator" : """ + null)
            sb.append(""","timer_waiting_for_board_workarea" : """ + null)
            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + null)
            sb.append(""","timer_idle_time" : """ + null)
            sb.append(""","timer_production_time" : """ + null)
            sb.append(""","slot_vision_failures" : """ + null)
            sb.append(""","picks" : """ + x.getAs[Double]("ul_picks"))
            sb.append(""","placements" : """ + x.getAs[Double]("ul_placements"))
            sb.append(""","possible_missing" : """ + x.getAs[Double]("ul_possible_missing"))
            sb.append(""","rejects" : """ + x.getAs[String]("ul_rejects"))
            sb.append(""","comp_missing" : """ + x.getAs[Double]("ul_comp_missing"))
            sb.append(""","slot_pins" : """ + null)
            sb.append(""","unit_cost" : """ + null)
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the powerBI
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitPowerBI").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToPowerBI(postString, powerBIApi)
          }
        }
      }
    } catch {
      case e1: Exception =>
        Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  /**
   * calling REST API endpoints for dumping timers data to PowerBI
   */

  @throws(classOf[Exception])
  def dumpTimersResultToPowerBI(dataframe: DataFrame, powerBIApi: Broadcast[String], propertiesBroadcast: Broadcast[Properties], su: SparkUtils, outputTopic: String, kafkaSink: Broadcast[KafkaSink]) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting timers data to PowerBI : " + dateFormat.format(date) + " ###")

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
            sb.append(""","sn_detail" : """ + null)
            sb.append(""","defect_refid" : """ + null)
            sb.append(""","defect_head_spindle" : """ + null)
            sb.append(""","defect_head" : """ + null)
            sb.append(""","defect_spindle" : """ + null)
            sb.append(""","nozzle_ch_hole" : """ + null)
            sb.append(""","nozzle_mach_ch_hole" : """ + null)
            sb.append(""","tooltypenozzle" : """ + null)
            sb.append(""","component_name" : """ + null)
            sb.append(""","slot" : """ + null)
            sb.append(""","slot_nozzle1" : """ + null)
            sb.append(""","slot_package" : """ + null)
            sb.append(""","slot_number" : """ + null)
            sb.append(""","slot_track" : """ + null)
            sb.append(""","defect_code" : """ + null)
            sb.append(""","defect_fail_type" : """ + null)
            sb.append(""","defect_count" : """ + null)
            sb.append(""","setup" : """ + x.getAs[Double]("setup"))
            sb.append(""","timer_diagnostic_time" : """ + x.getAs[Double]("ul_diagnostic_time"))
            sb.append(""","timer_setup_time" : """ + x.getAs[Double]("ul_setup_time"))
            sb.append(""","timer_waiting_for_operator" : """ + x.getAs[Double]("ul_waiting_for_operator"))
            sb.append(""","timer_waiting_for_board_workarea" : """ + x.getAs[Double]("ul_waiting_for_board_in_workarea"))
            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + x.getAs[Double]("ul_waiting_for_interrupt_recovery"))
            sb.append(""","timer_idle_time" : """ + x.getAs[Double]("ul_idle_time"))
            sb.append(""","timer_production_time" : """ + x.getAs[Double]("ul_production_time"))
            sb.append(""","slot_vision_failures" : """ + null)
            sb.append(""","picks" : """ + null)
            sb.append(""","placements" : """ + null)
            sb.append(""","possible_missing" : """ + null)
            sb.append(""","rejects" : """ + null)
            sb.append(""","comp_missing" : """ + null)
            sb.append(""","slot_pins" : """ + null)
            sb.append(""","unit_cost" : """ + null)
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the powerBI
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitPowerBI").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToPowerBI(postString, powerBIApi)
          }
        }

      }
    } catch {
      case e1: Exception =>
        Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  /**
   * calling REST API endpoints for dumping defects data to PowerBI
   */

  @throws(classOf[Exception])
  def dumpDefectsResultToPowerBI(dataframe: DataFrame, powerBIApi: Broadcast[String], propertiesBroadcast: Broadcast[Properties], su: SparkUtils, outputTopic: String, kafkaSink: Broadcast[KafkaSink]) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting defects data to PowerBI : " + dateFormat.format(date) + " ###")

    try {
      if (dataframe != null) {
        val resultRdd = dataframe.map {
          x =>
            val sb = new StringBuilder
            sb.append("""{""")
            sb.append(""""table_name" : """" + """defects""")
            sb.append("""","machine_name" : """ + getStingValues(x, "machine_name"))
            sb.append(""","product_id" : """ + null)
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
            sb.append(""","nozzle_ch_hole" : """ + null)
            sb.append(""","nozzle_mach_ch_hole" : """ + null)
            sb.append(""","tooltypenozzle" : """ + null)
            sb.append(""","component_name" : """ + getStingValues(x, "cmpnt_part_no"))
            sb.append(""","slot" : """ + null)
            sb.append(""","slot_nozzle1" : """ + null)
            sb.append(""","slot_package" : """ + null)
            sb.append(""","slot_number" : """ + null)
            sb.append(""","slot_track" : """ + null)
            sb.append(""","defect_code" : """ + getStingValues(x, "defect_code"))
            sb.append(""","defect_fail_type" : """ + getStingValues(x, "fail_type"))
            sb.append(""","defect_count" : """ + x.getAs[Double]("defect_count"))
            sb.append(""","setup" : """ + null)
            sb.append(""","timer_diagnostic_time" : """ + null)
            sb.append(""","timer_setup_time" : """ + null)
            sb.append(""","timer_waiting_for_operator" : """ + null)
            sb.append(""","timer_waiting_for_board_workarea" : """ + null)
            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + null)
            sb.append(""","timer_idle_time" : """ + null)
            sb.append(""","timer_production_time" : """ + null)
            sb.append(""","slot_vision_failures" : """ + null)
            sb.append(""","picks" : """ + null)
            sb.append(""","placements" : """ + null)
            sb.append(""","possible_missing" : """ + null)
            sb.append(""","rejects" : """ + null)
            sb.append(""","comp_missing" : """ + null)
            sb.append(""","slot_pins" : """ + null)
            sb.append(""","unit_cost" : """ + null)
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the powerBI
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitPowerBI").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToPowerBI(postString, powerBIApi)
          }
        }

      }
    } catch {
      case e1: Exception =>
        Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  /**
   * calling REST API endpoints for dumping slot data to PowerBI
   */

  @throws(classOf[Exception])
  def dumpSlotResultToPowerBI(dataframe: DataFrame, powerBIApi: Broadcast[String], propertiesBroadcast: Broadcast[Properties], su: SparkUtils, outputTopic: String, kafkaSink: Broadcast[KafkaSink]) {

    val debug: Boolean = propertiesBroadcast.value.getProperty("debug").toBoolean

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    println("### Posting slot data to PowerBI : " + dateFormat.format(date) + " ###")

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
            sb.append(""","sn_detail" : """ + null)
            sb.append(""","defect_refid" : """ + null)
            sb.append(""","defect_head_spindle" : """ + null)
            sb.append(""","defect_head" : """ + null)
            sb.append(""","defect_spindle" : """ + null)
            sb.append(""","nozzle_ch_hole" : """ + null)
            sb.append(""","nozzle_mach_ch_hole" : """ + null)
            sb.append(""","tooltypenozzle" : """ + null)
            sb.append(""","component_name" : """ + getStingValues(x, "component_name"))
            sb.append(""","slot" : """ + getStingValues(x, "slot"))
            sb.append(""","slot_nozzle1" : """ + getStingValues(x, "nozzle1"))
            sb.append(""","slot_package" : """ + getStingValues(x, "package"))
            sb.append(""","slot_number" : """ + getStingValues(x, "slot_number"))
            sb.append(""","slot_track" : """ + getStingValues(x, "track"))
            sb.append(""","defect_code" : """ + null)
            sb.append(""","defect_fail_type" : """ + null)
            sb.append(""","defect_count" : """ + null)
            sb.append(""","setup" : """ + null)
            sb.append(""","timer_diagnostic_time" : """ + null)
            sb.append(""","timer_setup_time" : """ + null)
            sb.append(""","timer_waiting_for_operator" : """ + null)
            sb.append(""","timer_waiting_for_board_workarea" : """ + null)
            sb.append(""","timer_waiting_for_interrupt_recovery" : """ + null)
            sb.append(""","timer_idle_time" : """ + null)
            sb.append(""","timer_production_time" : """ + null)
            sb.append(""","slot_vision_failures" : """ + x.getAs[Double]("ul_slot_vision_failures"))
            sb.append(""","picks" : """ + x.getAs[Double]("ul_slot_picks"))
            sb.append(""","placements" : """ + x.getAs[Double]("ul_slot_placements"))
            sb.append(""","possible_missing" : """ + x.getAs[Double]("ul_possible_missing"))
            sb.append(""","rejects" : """ + null)
            sb.append(""","comp_missing" : """ + null)
            sb.append(""","slot_pins" : """ + x.getAs[Double]("pins"))
            sb.append(""","unit_cost" : """ + x.getAs[Double]("unit_cost"))
            sb.append("""}""")
            sb.toString()
        }

        // writing to kafka topic
        su.produceToKafka(resultRdd, outputTopic, kafkaSink)

        // create the batch of 10000 and post on the powerBI
        resultRdd.foreachPartition { ele =>
          ele.grouped(propertiesBroadcast.value.getProperty("ApiLimitPowerBI").toInt).foreach { chunk =>
            val postString = """[""" + chunk.toList.mkString(",") + """]"""
            if (debug) {
              println("===================================== Post Body ==================================")
              println(postString)
            }
            postToPowerBI(postString, powerBIApi)
          }
        }

      }
    } catch {
      case e1: Exception =>
        Gateway.sendEmail(to, from, subject, e1, key)
    }
  }

  def postToPowerBI(postString: String, powerBIApi: Broadcast[String]): Unit = {
    // sleep of 1 sec
    Thread.sleep(1000);
    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    val date = new Date();
    val post = new HttpPost(powerBIApi.value.toString())
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(postString))
    val Response = Gateway.getPostContent(post)
    if (!Response.isEmpty() && Response.contains("error")) {
      println("### PowerBI API is down! ###" + Response)
    } else {
      println("### data posted to powerBI : " + dateFormat.format(date) + " ###")
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
	 * creating singleton hive context
	 * 
	 * @param SparkContext
	 * 
	 * @param Properties
	 */
  def getHiveContext(sparkContext: SparkContext, propertiesBroadcast: Broadcast[Properties]): HiveContext = {
    synchronized {

      val configuration = new Configuration
      configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.getCurrentUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS)

      if (instance == null) {

        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          propertiesBroadcast.value.getProperty("hadoop.kerberos.principal"), sparkContext.getConf.get("spark.yarn.keytab"))
          .doAs(new PrivilegedExceptionAction[HiveContext]() {
            @Override
            def run(): HiveContext = {
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
              instance
            }
          })

      }

      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        propertiesBroadcast.value.getProperty("hadoop.kerberos.principal"), sparkContext.getConf.get("spark.yarn.keytab"))
        .doAs(new PrivilegedExceptionAction[HiveContext]() {
          @Override
          def run(): HiveContext = {
            instance
          }
        })

    }
  }

  def getStingValues(x: Row, column: String): String = if (x.getAs[String](column) != null) "\"" + x.getAs[String](column) + "\"" else null

  /*
	 * refresh material cost table
	 * 
	 * @param SparkContext
	 * 
	 * @param Properties
	 */
  def refreshMaterialCostTable(sc: SparkContext, propertiesBroadcast: Broadcast[Properties]): Unit = {
    if (propertiesBroadcast.value.getProperty("inletKafkaTopicC4").contains("slot")) {
      println("### refresh material cost table ###")
      getHiveContext(sc, propertiesBroadcast).sql("select REGEXP_REPLACE(material, \"^0+\", '') as material, plant, unit_cost from " + propertiesBroadcast.value.getProperty("env") + propertiesBroadcast.value.getProperty("material_cost_table")).registerTempTable("material_cost")
    }
  }
  
    /*
	 * refresh elctrnc_assy_loc_plant_xref table data
	 * 
	 * @param SparkContext
	 * 
	 * @param Properties
	 */
  def refreshLocPlantXref(sc: SparkContext, propertiesBroadcast: Broadcast[Properties]): Unit = {
      println("### elctrnc_assy_loc_plant_xref table ###")
      getHiveContext(sc, propertiesBroadcast).sql("select lower(location) as location, plant from " + propertiesBroadcast.value.getProperty("env") + propertiesBroadcast.value.getProperty("loc_plant_xref")).registerTempTable("elctrnc_assy_loc_plant_xref")
  }
}