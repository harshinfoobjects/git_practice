package com.rockwell.twb.util

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import com.rockwell.twb.stores.MySqlOffsetsStore
import org.apache.http.entity.StringEntity
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
import org.apache.spark.broadcast.Broadcast
import com.google.gson.Gson

import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions._
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedExceptionAction
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.conf.Configuration
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.http.client.methods.HttpPost

object HiveUtil {

  @transient private var connection: Connection = _

  val dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val con: Connection = null;
  val stmt: Statement = null;

  def insertInto_hive(readyToSaveDf: DataFrame, table: String, propertiesBroadcast: Broadcast[Properties]) {

    val configuration = new Configuration
    configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
    UserGroupInformation.setConfiguration(configuration)

    val tableName = propertiesBroadcast.value.getProperty("env") + table

    UserGroupInformation.getCurrentUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS)

    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      propertiesBroadcast.value.getProperty("hadoop.kerberos.principal"), propertiesBroadcast.value.getProperty("hadoop.kerberos.keytab"))
      .doAs(new PrivilegedExceptionAction[Unit]() {
        @Override
        def run(): Unit = {
          println("data being saved to :  " + tableName)
          readyToSaveDf.write.mode(SaveMode.Append).format("parquet").insertInto(tableName)
          refreshTable(tableName, propertiesBroadcast.value)
          println("unperists rdd")
          //  readyToSaveDf.rdd.unpersist(true)
        }
      })

  }

  /*
	* impala connection
	*
	* @param jdbcUrl
	 */
  def getImapalConnection(jdbcUrl: String): Connection = {
    synchronized {
      if (connection == null) {
        try {

          Class.forName(jdbcUrl);

          println("Connecting to " + jdbcUrl)
          System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
          System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")

          connection = DriverManager.getConnection(jdbcUrl)
          println("Connected!");

          sys.addShutdownHook {
            connection.close()
          }

        } catch {
          case e1: Exception =>
            e1.printStackTrace()
        }

      }
      connection
    }
  }

  def postMaterialCost(materialCostDF : DataFrame, propertiesBroadcast: Broadcast[Properties]) : Unit ={
    
    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      materialCostDF.show()
    }
     materialCostDF.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "materailcost_service")
      }
    }

    materialCostDF.rdd.unpersist(true)
    
  }
  /*
	* refresh impala table
	*
	* @param tableName
	*
	* @param properties
	*/
  def refreshTable(tableName: String, properties: Properties): Unit = {
    try {

      val driverName: String = properties.getProperty("jdbcdriver")
      println("driverName " + driverName)
      try {
        Class.forName(driverName);
      } catch {
        case e: ClassNotFoundException => e.printStackTrace()
      }
      val jdbcUrl: String = properties.getProperty("jdbcUrl").replace("{env}", properties.getProperty("env"))
      println("Connecting to " + jdbcUrl)
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      val con: Connection = DriverManager.getConnection(jdbcUrl)
      println("Connected!");
      val stmt: Statement = con.createStatement()
      stmt.execute("refresh " + tableName)
      println(tableName + " has been refreshed")
    } catch {
      case e1: Exception =>
        e1.printStackTrace()
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch {
        case e1: SQLException => e1.printStackTrace()

      }
      try {
        if (con != null) {
          con.close();
        }
      } catch {
        case e1: SQLException => e1.printStackTrace()

      }
    }
  }

  def insertInto_ea_pap_slot(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_pap_slot_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("Product_ID", "product_id")
      .withColumn("Date_Time", unix_timestamp(df("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time")
      .withColumnRenamed("User_Name", "user_name")
      .withColumnRenamed("ComponentName", "component_name")
      .withColumnRenamed("FeederName", "feeder_name")
      .withColumnRenamed("FeederType", "feeder_type")
      .withColumnRenamed("SlotNumber", "slot_number")
      .withColumnRenamed("Location", "location")
      .withColumnRenamed("ulCompMissing", "ul_comp_missing")
      .withColumnRenamed("ulCompUpsideDown", "ul_comp_upside_down")
      .withColumnRenamed("ulLeadLocationFailure", "ul_lead_location_failure")
      .withColumnRenamed("ulLeadSpacingFailure", "ul_lead_spacing_failure")
      .withColumnRenamed("ulLeadDeviationFailure", "ul_lead_deviation_failure")
      .withColumnRenamed("ulLeadSpanFailure", "ul_leadspan_failure")
      .withColumnRenamed("ulCompLocationFailure", "ul_comp_location_failure")
      .withColumnRenamed("ulCompSizeFailure", "ul_comp_size_failure")
      .withColumnRenamed("ulFeatureLocationFailure", "ul_feature_location_failure")
      .withColumnRenamed("ulFeatureSpacingFailure", "ul_feature_spacing_failure")
      .withColumnRenamed("ulPitchFailure", "ul_pitch_failure")
      .withColumnRenamed("ulOrientationCheckFailure", "ul_orientation_check_failure")
      .withColumnRenamed("ulLeadCountFailure", "ul_lead_count_failure")
      .withColumnRenamed("ulBallCountFailure", "ul_ball_count_failure")
      .withColumnRenamed("ulFeatureCountFailure", "ul_feature_count_failure")
      .withColumnRenamed("ulCornerToleranceFailure", "ul_corner_tolerance_failure")
      .withColumnRenamed("ulVPSHeightFailure", "ul_vps_height_failure")
      .withColumnRenamed("ulSlotPicks", "ul_slot_picks")
      .withColumnRenamed("ulSlotPlacements", "ul_slot_placements")
      .withColumnRenamed("ulSlotVisionFailures", "ul_slot_vision_failures")
      .withColumnRenamed("ulPossibleMissing", "ul_possible_missing")
      .withColumnRenamed("ulPurged", "ul_purged")
      .withColumnRenamed("ulVPSPartPresenceFailure", "ul_vps_part_presence_failure")
      .withColumnRenamed("ulSmartFeederPicked", "ul_smart_feeder_picked")
      .withColumnRenamed("ulSlotRejects", "ul_slot_rejects")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "product_id", "date_time", "file_name", "hdfs_file_name", "kafka_topic", "user_name", "component_name", "feeder_name", "feeder_type", "slot_number", "location", "ul_comp_missing", "ul_comp_upside_down", "ul_lead_location_failure", "ul_lead_spacing_failure", "ul_lead_deviation_failure", "ul_leadspan_failure", "ul_comp_location_failure", "ul_comp_size_failure", "ul_feature_location_failure", "ul_feature_spacing_failure", "ul_pitch_failure", "ul_orientation_check_failure", "ul_lead_count_failure", "ul_ball_count_failure", "ul_feature_count_failure", "ul_corner_tolerance_failure", "ul_vps_height_failure", "ul_slot_picks", "ul_slot_placements", "ul_slot_vision_failures", "ul_possible_missing", "ul_purged", "ul_vps_part_presence_failure", "ul_smart_feeder_picked", "ul_slot_rejects","uuid")

    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_pap_slot_service")
      }
    }

    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_ea_pap_timers(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_pap_timers_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))

      .withColumnRenamed("Product_ID", "product_id")
      .withColumn("Date_Time", unix_timestamp(df("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time")
      .withColumnRenamed("User_Name", "user_name")
      .withColumnRenamed("ulProductionTime", "ul_production_time")
      .withColumnRenamed("ulSetupTime", "ul_setup_time")
      .withColumnRenamed("ulIdleTime", "ul_idle_time")
      .withColumnRenamed("ulDiagnosticTime", "ul_diagnostic_time")
      .withColumnRenamed("ulWaitingForOperator", "ul_waiting_for_operator")
      .withColumnRenamed("ulWaitingForBoardInput", "ul_waiting_for_board_input")
      .withColumnRenamed("ulWaitingForBoardOutput", "ul_waiting_for_board_output")
      .withColumnRenamed("ulWaitingForInterruptRecovery", "ul_waiting_for_interrupt_recovery")
      .withColumnRenamed("ulWaitingForBoardInWorkArea", "ul_waiting_for_boardin_workarea")
      .withColumnRenamed("ulWaitingForBrdFromUpline", "ul_waiting_for_brd_from_upline")
      .withColumnRenamed("ulWaitingForDownlineAvailable", "ul_waiting_for_downline_available")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "product_id", "date_time",
      "file_name", "hdfs_file_name", "kafka_topic", "user_name", "ul_production_time",
      "ul_setup_time", "ul_idle_time", "ul_diagnostic_time", "ul_waiting_for_operator",
      "ul_waiting_for_board_input", "ul_waiting_for_board_output", "ul_waiting_for_interrupt_recovery",
      "ul_waiting_for_boardin_workarea", "ul_waiting_for_brd_from_upline", "ul_waiting_for_downline_available", "uuid")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_pap_timers_service")
      }
    }
    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_ea_pap_info(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_pap_info_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("Name", "name")
      .withColumnRenamed("IsKeyed", "is_keyed")
      .withColumnRenamed("CRCId", "crc_id")
      .withColumnRenamed("Description", "description")
      .withColumnRenamed("Shape", "shape")
      .withColumnRenamed("Notes", "notes")
      .withColumnRenamed("PlantCode", "plant_code")
      .withColumnRenamed("Machine", "machine")
    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "name", "is_keyed", "file_name", "hdfs_file_name", "kafka_topic", "crc_id", "description", "shape", "notes", "plant_code", "machine")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);

    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_pap_info_service")
      }
    }

    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_ea_pap_nozzle(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {

    var tableName = propertiesBroadcast.value.getProperty("ea_pap_nozzle_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("Product_ID", "product_id")
      .withColumn("Date_Time", unix_timestamp(df("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time")
      .withColumnRenamed("User_name", "User_name")
      .withColumnRenamed("location", "location")
      .withColumnRenamed("tooltype", "tooltype")
      .withColumnRenamed("ulpicks", "ul_picks")
      .withColumnRenamed("ulplacements", "ul_placements")
      .withColumnRenamed("ulcompmissing", "ul_comp_missing")
      .withColumnRenamed("ulcompupsidedown", "ul_comp_up_side_down")
      .withColumnRenamed("ulleadlocationfailure", "ul_lead_location_failure")
      .withColumnRenamed("ulleadspacingfailure", "ul_lead_spacing_failure")
      .withColumnRenamed("ulleaddeviationfailure", "ul_lead_deviation_failure")
      .withColumnRenamed("ulleadspanfailure", "ul_lead_span_failure")
      .withColumnRenamed("ulcomplocationfailure", "ul_comp_location_failure")
      .withColumnRenamed("ulcompsizefailure", "ul_compsize_failure")
      .withColumnRenamed("ulfeaturelocationfailure", "ul_feature_location_failure")
      .withColumnRenamed("ulfeaturespacingfailure", "ul_feature_spacing_failure")
      .withColumnRenamed("ulpitchfailure", "ul_pitch_failure")
      .withColumnRenamed("ulorientationcheckfailure", "ul_orientation_check_failure")
      .withColumnRenamed("ulleadcountfailure", "ul_lead_count_failure")
      .withColumnRenamed("ulballcountfailure", "ul_ball_count_failure")
      .withColumnRenamed("ulfeaturecountfailure", "ul_feature_count_failure")
      .withColumnRenamed("ulPossibleMissing", "ul_possible_missing")
      .withColumnRenamed("ulRejects", "ul_rejects")
      .withColumnRenamed("ulPurged", "ul_purged")
      .withColumnRenamed("ulCornerToleranceFailure", "ul_corner_tolerance_failure")
      .withColumnRenamed("ulVPSHeightFailure", "ul_vps_height_failure")
      .withColumnRenamed("ulVPSPartPresenceFailure", "ul_vps_part_presence_failure")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "product_id", "date_time", "file_name", "hdfs_file_name", "kafka_topic", "user_name", "location", "tooltype", "ul_picks", "ul_placements", "ul_comp_missing", "ul_comp_up_side_down", "ul_lead_location_failure", "ul_lead_spacing_failure", "ul_lead_deviation_failure", "ul_lead_span_failure", "ul_comp_location_failure", "ul_compsize_failure", "ul_feature_location_failure", "ul_feature_spacing_failure", "ul_pitch_failure", "ul_orientation_check_failure", "ul_lead_count_failure", "ul_ball_count_failure", "ul_feature_count_failure", "ul_possible_missing", "ul_rejects", "ul_purged", "ul_corner_tolerance_failure", "ul_vps_height_failure", "ul_vps_part_presence_failure", "uuid")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);

    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_pap_nozzle_service")
      }
    }
    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_progorderlist(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_progorderlist_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
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
      .withColumn("Time Stamp", unix_timestamp(df("Time Stamp"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Time Stamp", "time_stamp")
      .withColumnRenamed("Circuit #", "circuit")
      .withColumnRenamed("Silkscreen", "silkscreen")

    var readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "plant", "machine", "file_name", "hdfs_file_name", "kafka_topic", "work_order", "assembly", "revision", "side", "part_number", "slot", "head", "spindle", "ref_id", "time_stamp", "circuit", "silkscreen")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);

    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_progorderlist_service")
      }
    }
    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_ftdefects(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_ftdefects_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumn("DEFECT_DATE", unix_timestamp(df("DEFECT_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("DEFECT_DATE", "defect_date")
      .withColumnRenamed("PLACING_PLANT_CODE", "placing_plant_code")
      .withColumnRenamed("PLACING_AREA_CODE", "placing_area_code")
      .withColumnRenamed("PLACING_PROCESS_CODE", "placing_process_code")
      .withColumnRenamed("PLACING_WC_CODE", "placing_wc_code")
      .withColumnRenamed("FINDING_PLANT_CODE", "finding_plant_code")
      .withColumnRenamed("FINDING_AREA_CODE", "finding_area_code")
      .withColumnRenamed("FINDING_PROCESS_CODE", "finding_process_code")
      .withColumnRenamed("FINDING_WC_CODE", "finding_wc_code")
      .withColumnRenamed("BUILD_PART_NO", "build_part_no")
      .withColumnRenamed("BUILD_PART_DESC", "build_part_desc")
      .withColumnRenamed("BUILD_SERIAL_NO", "build_serial_no")
      .withColumnRenamed("ASSEM_PART_NO", "assem_part_no")
      .withColumnRenamed("ASSEM_PART_DESC", "assem_part_desc")
      .withColumnRenamed("ASSEM_SERIAL_NO", "assem_serial_no")
      .withColumnRenamed("ORDER_NO", "order_no")
      .withColumnRenamed("DEFECT_CODE", "defect_code")
      .withColumnRenamed("FAILED_TEST_CODE", "failed_test_code")
      .withColumnRenamed("FAIL_TYPE", "fail_type")
      .withColumnRenamed("FAULT_BOARD_NO", "fault_board_no")
      .withColumnRenamed("CMPNT_PART_NO", "cmpnt_part_no")
      .withColumnRenamed("CMPNT_PART_DESC", "cmpnt_part_desc")
      .withColumnRenamed("REF_DESIGNATOR", "ref_designator")
      .withColumnRenamed("DEFECT_COUNT", "defect_count")
      .withColumn("REPAIR_DATE", unix_timestamp(df("REPAIR_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("REPAIR_DATE", "repair_date")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "defect_date", "placing_plant_code", "file_name", "hdfs_file_name", "kafka_topic", "placing_area_code", "placing_process_code", "placing_wc_code", "finding_plant_code", "finding_area_code", "finding_process_code", "finding_wc_code", "build_part_no", "build_part_desc", "build_serial_no", "assem_part_no", "assem_part_desc", "assem_serial_no", "order_no", "defect_code", "failed_test_code", "fail_type", "fault_board_no", "cmpnt_part_no", "cmpnt_part_desc", "ref_designator", "defect_count", "repair_date", "uuid")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);

    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_ftdefects_service")
      }
    }
    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_wodetail(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_wodetail_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("PLANT", "plant")
      .withColumnRenamed("Optel Schedule WO", "optel_schedule_wo")
      .withColumnRenamed("SAP WO", "sap_wo")
      .withColumnRenamed("Assembly", "assembly")
      .withColumnRenamed("Assembly Rev", "assembly_rev")
      .withColumnRenamed("Build", "build")
      .withColumnRenamed("Build Rev", "build_rev")
      .withColumnRenamed("Order Qty", "order_qty")
      .withColumnRenamed("Panel Qty", "panel_qty")
      .withColumnRenamed("Side", "side")
      .withColumnRenamed("Setup#", "setup")
      .withColumnRenamed("Golden", "golden")
      .withColumnRenamed("PiPPlc/panel", "pipplc_panel")
      .withColumnRenamed("PiPPad/panel", "pippad_panel")
      .withColumnRenamed("SMTPlc/panel", "smtplc_panel")
      .withColumnRenamed("SMTPad/panel", "smtpad_panel")
      .withColumnRenamed("Line", "line")
    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "plant", "optel_schedule_wo", "file_name", "hdfs_file_name", "kafka_topic", "sap_wo", "assembly", "assembly_rev", "build", "build_rev", "order_qty", "panel_qty", "side", "setup", "golden", "pipplc_panel", "pippad_panel", "smtplc_panel", "smtpad_panel", "line")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);

    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_wodetail_service")
      }
    }
    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_comptype(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_comptype_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("PLANT", "plant")
      .withColumnRenamed("Machine", "machine")
      .withColumnRenamed("Part Number", "part_number")
      .withColumnRenamed("Package", "package")
      .withColumnRenamed("Feedertype", "feedertype")
      .withColumnRenamed("Head Type", "head_type")
      .withColumnRenamed("Nozzle1", "nozzle1")
      .withColumnRenamed("Nozzle2", "nozzle2")
      .withColumnRenamed("Pressure", "pressure")
      .withColumnRenamed("Accuracy", "accuracy")
      .withColumnRenamed("Preorient", "preorient")
      .withColumnRenamed("APU", "apu")
      .withColumnRenamed("Pick", "pick")
      .withColumnRenamed("Motion", "motion")
      .withColumnRenamed("Center", "center")
      .withColumnRenamed("Place", "place")
      .withColumnRenamed("LPID", "lpid")
      .withColumnRenamed("# Pins", "pins")
      .withColumnRenamed("Max of Inv", "max_of_inv")
      .withColumnRenamed("Comp Height", "comp_height")
      .withColumnRenamed("Comp Length", "comp_length")
      .withColumnRenamed("Comp Width", "comp_width")
      .withColumnRenamed("Body Length", "body_length")
      .withColumnRenamed("Body Width", "body_width")
      .withColumnRenamed("Type", "type")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "plant", "machine", "part_number", "file_name", "hdfs_file_name", "kafka_topic", "package", "feedertype", "head_type", "nozzle1", "nozzle2", "pressure", "accuracy", "preorient", "apu", "pick", "motion", "center", "place", "lpid", "pins", "max_of_inv", "comp_height", "comp_length", "comp_width", "body_length", "body_width", "type")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    
    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }
    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_comptype_service")
      }
    }
    readyToSaveDf.rdd.unpersist(true)
  }

  def insertInto_equipmentoutline(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_equipmentoutline_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumn("TimeStamp", unix_timestamp(df("TimeStamp"), dateTimeFormat).cast("timestamp")).withColumnRenamed("TimeStamp", "timestamp")
      .withColumnRenamed("POSITION", "position")
      .withColumnRenamed("PLANT", "plant")
      .withColumnRenamed("Equipment Make", "equipment_make")
      .withColumnRenamed("Machine Name", "machine_name")
      .withColumnRenamed("Line", "line")
      .withColumnRenamed("Head 1", "head_1")
      .withColumnRenamed("Head 2", "head_2")
      .withColumnRenamed("Equipment Model", "equipment_model")
      .withColumnRenamed("Equipment Type", "equipment_type")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "plant", "equipment_make", "file_name", "hdfs_file_name", "kafka_topic", "machine_name", "line", "head_1", "head_2", "timestamp", "equipment_model", "equipment_type", "position")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    
    val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
    if (debug) {
      readyToSaveDf.show()
    }

    readyToSaveDf.toJSON.foreachPartition { ele =>
      ele.grouped(propertiesBroadcast.value.getProperty("thingworxApiLimit").toInt).foreach { chunk =>
        val postString = """[""" + chunk.toList.mkString(",") + """]"""
        postToThingWorx(postString, propertiesBroadcast, "ea_equipmentoutline_service")
      }
    }

    readyToSaveDf.rdd.unpersist(true)
  }

  def postToThingWorx(postString: String, propertiesBroadcast: Broadcast[Properties], service: String): Unit = {

    try {
      val inputJson = "{\"inputJson\":" + postString + "}"
      val serviceName = propertiesBroadcast.value.getProperty(service).toString()
      val debug = propertiesBroadcast.value.getProperty("debug").toBoolean
      val serviceUrl = propertiesBroadcast.value.getProperty("thingworxUrl").toString() + serviceName
      val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      val date = new Date();
      val post = new HttpPost(serviceUrl)
      post.addHeader("Content-Type", "application/json")
      post.addHeader("appKey", propertiesBroadcast.value.getProperty("appKey"))
      post.setEntity(new StringEntity(inputJson))
      val startTime = System.nanoTime();
      val Response = Gateway.getPostContent(post)
      val endTime   = System.nanoTime();
      if (!Response.isEmpty()) {
        println("### ThingWorx is down! " + serviceName  +  ", " + dateFormat.format(date) +  " : " + Response + "###")
      } else {
        println("### data posted to ThingWorx for service : " + serviceName + ",  " + dateFormat.format(date) + " ###")
      }
      println("total time to push data on thingworx for service: " + serviceName + " => " + (endTime - startTime)/1000000000)
    } catch {
      case all: Exception => all.printStackTrace()
    }

  }
}