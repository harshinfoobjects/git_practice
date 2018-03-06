package com.rockwell.twb.util

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import com.rockwell.twb.stores.MySqlOffsetsStore

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
          //println("total elements :  " + readyToSaveDf.count())
          //println("total partitions :  " + readyToSaveDf.rdd.partitions.length)
          //readyToSaveDf.rdd.coalesce(1);
          readyToSaveDf.write.mode(SaveMode.Append).format("parquet").insertInto(tableName)
          refreshTable(tableName, propertiesBroadcast.value)
          println("unperists rdd")
          readyToSaveDf.rdd.unpersist(true)
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
      val jdbcUrl: String = properties.getProperty("jdbcUrl").replace("{env}",  properties.getProperty("env"))
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

  def insertInto_ea_spi_header(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {

    val toDouble = udf[Double, String](_.toDouble)
    val dateTimeFormat_spi = "MM/dd/yyyy HH:mm:ss"
    var tableName = propertiesBroadcast.value.getProperty("ea_spi_header_table")
    val renameDf = df.withColumnRenamed("PCBID", "pcb_id")
      .withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumn("InspectionStartTime", unix_timestamp(df("InspectionStartTime"), dateTimeFormat_spi).cast("timestamp")).withColumnRenamed("InspectionStartTime", "inspection_start_dttm")
      .withColumn("InspectionEndTime", unix_timestamp(df("InspectionEndTime"), dateTimeFormat_spi).cast("timestamp")).withColumnRenamed("InspectionEndTime", "inspection_end_dttm")
      .withColumn("InspectionTestTime", toDouble(df("InspectionTestTime"))).withColumnRenamed("InspectionTestTime", "inspection_test_duration_sec")
      .withColumnRenamed("PCBName", "pcb_name")
      .withColumnRenamed("PCBResult", "pcb_result")
      .withColumnRenamed("PrintStroke", "print_stroke")
      .withColumnRenamed("MachineID", "machine_id")
      .withColumnRenamed("GeneratedKey", "generated_key")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "file_name", "hdfs_file_name", "kafka_topic", "barcode", "side", "pcb_id", "inspection_start_dttm", "inspection_end_dttm", "inspection_test_duration_sec", "pcb_name", "pcb_result", "print_stroke", "machine_id", "generated_key")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
     val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
     readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_spi_header")

    renameDf.rdd.unpersist(true)

  }

  def insertInto_ea_spi_detail(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    val toDouble = udf[Double, String](_.toDouble)
    var tableName = propertiesBroadcast.value.getProperty("ea_spi_detail_table")
    val renameDf = df.withColumnRenamed("PCBID", "pcb_id")
      .withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("GeneratedKey", "generated_key")
      .withColumnRenamed("ModuleNo", "module_nbr")
      .withColumnRenamed("RefDesignator", "ref_designator")
      .withColumnRenamed("PinNumber", "pin_nbr")
      .withColumnRenamed("SizeX", "size_x")
      .withColumnRenamed("SizeY", "size_y")
      .withColumn("PadStencilHeight", toDouble(df("PadStencilHeight"))).withColumnRenamed("PadStencilHeight", "pad_stencil_height")
      .withColumnRenamed("PadResult", "pad_result")
      .withColumnRenamed("PadDefectType", "pad_defect_type")
      .withColumn("Volume", toDouble(df("Volume"))).withColumnRenamed("Volume", "solder_paste_volume")
      .withColumn("Height", toDouble(df("Height"))).withColumnRenamed("Height", "solder_paste_height")
      .withColumn("Area", toDouble(df("Area"))).withColumnRenamed("Area", "solder_paste_area")
      .withColumn("OffsetX", toDouble(df("OffsetX"))).withColumnRenamed("OffsetX", "offset_X")
      .withColumn("OffsetY", toDouble(df("OffsetY"))).withColumnRenamed("OffsetY", "offset_Y")
    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "file_name", "hdfs_file_name", "kafka_topic", "generated_key", "module_nbr", "ref_designator", "pin_nbr", "size_x", "size_y", "pad_stencil_height", "pad_result", "pad_defect_type", "solder_paste_volume", "solder_paste_height", "solder_paste_area", "offset_X", "offset_Y")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
      val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
     readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_spi_detail")
    renameDf.rdd.unpersist(true)
  }

  def insertInto_ea_pap_gen(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {

    val tableName = propertiesBroadcast.value.getProperty("ea_pap_gen_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("Product_ID", "product_id")
      .withColumn("Date_Time", unix_timestamp(df("Date_Time"), dateTimeFormat).cast("timestamp")).withColumnRenamed("Date_Time", "date_time")
      .withColumnRenamed("User_Name", "user_name")
      .withColumnRenamed("ulFidFindAttempts", "ul_fid_find_attempts")
      .withColumnRenamed("ulFidFindFailures", "ul_fid_find_failures")
      .withColumnRenamed("ulBoardsEntering", "ul_boards_entering")
      .withColumnRenamed("ulBoardsExiting", "ul_boards_exiting")
      .withColumnRenamed("ulTotBadBoardSenseAccepted", "ul_tot_bad_board_sense_accepted")
      .withColumnRenamed("ulTotBadBoardSenseRejected", "ul_tot_bad_board_sense_rejected")
      .withColumnRenamed("ulTotBadCircuitSenseAccepted", "ul_tot_bad_circuit_sense_accepted")
      .withColumnRenamed("ulTotBadCircuitSenseRejected", "ul_tot_bad_circuit_sense_rejected")
      .withColumnRenamed("ulTotBoardCycleTime", "ul_tot_board_cycle_time")
      .withColumnRenamed("ulBestBoardCycleTime", "ul_best_board_cycle_time")
      .withColumnRenamed("ulBoardCycleCount", "ul_board_cycle_count")
      .withColumnRenamed("ulPossibleMissing", "ul_possible_missing")
      .withColumnRenamed("ulFidFindAttempts", "ul_fid_find_attempts")
    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "product_id", "date_time", "file_name", "hdfs_file_name", "kafka_topic", "user_name", "ul_fid_find_attempts", "ul_fid_find_failures", "ul_boards_entering", "ul_boards_exiting", "ul_tot_bad_board_sense_accepted", "ul_tot_bad_board_sense_rejected", "ul_tot_bad_circuit_sense_accepted", "ul_tot_bad_circuit_sense_rejected", "ul_tot_board_cycle_time", "ul_best_board_cycle_time", "ul_board_cycle_count", "ul_possible_missing")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_pap_gen")
    renameDf.rdd.unpersist(true)


    renameDf.rdd.unpersist(true)
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

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "product_id", "date_time", "file_name", "hdfs_file_name", "kafka_topic", "user_name", "component_name", "feeder_name", "feeder_type", "slot_number", "location", "ul_comp_missing", "ul_comp_upside_down", "ul_lead_location_failure", "ul_lead_spacing_failure", "ul_lead_deviation_failure", "ul_leadspan_failure", "ul_comp_location_failure", "ul_comp_size_failure", "ul_feature_location_failure", "ul_feature_spacing_failure", "ul_pitch_failure", "ul_orientation_check_failure", "ul_lead_count_failure", "ul_ball_count_failure", "ul_feature_count_failure", "ul_corner_tolerance_failure", "ul_vps_height_failure", "ul_slot_picks", "ul_slot_placements", "ul_slot_vision_failures", "ul_possible_missing", "ul_purged", "ul_vps_part_presence_failure", "ul_smart_feeder_picked")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
       val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
      readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_pap_slot")
    renameDf.rdd.unpersist(true)

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
      "ul_waiting_for_boardin_workarea", "ul_waiting_for_brd_from_upline", "ul_waiting_for_downline_available")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
      val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
      readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_pap_timers")


    renameDf.rdd.unpersist(true)

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
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_pap_info")

    

    renameDf.rdd.unpersist(true)

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

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "product_id", "date_time", "file_name", "hdfs_file_name", "kafka_topic", "user_name", "location", "tooltype", "ul_picks", "ul_placements", "ul_comp_missing", "ul_comp_up_side_down", "ul_lead_location_failure", "ul_lead_spacing_failure", "ul_lead_deviation_failure", "ul_lead_span_failure", "ul_comp_location_failure", "ul_compsize_failure", "ul_feature_location_failure", "ul_feature_spacing_failure", "ul_pitch_failure", "ul_orientation_check_failure", "ul_lead_count_failure", "ul_ball_count_failure", "ul_feature_count_failure", "ul_possible_missing", "ul_rejects", "ul_purged", "ul_corner_tolerance_failure", "ul_vps_height_failure", "ul_vps_part_presence_failure")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_pap_nozzle")

    renameDf.rdd.unpersist(true)

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

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "plant", "machine", "file_name", "hdfs_file_name", "kafka_topic", "work_order", "assembly", "revision", "side", "part_number", "slot", "head", "spindle", "ref_id", "time_stamp", "circuit", "silkscreen")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/progorderlist")

    renameDf.rdd.unpersist(true)

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

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "defect_date", "placing_plant_code", "file_name", "hdfs_file_name", "kafka_topic", "placing_area_code", "placing_process_code", "placing_wc_code", "finding_plant_code", "finding_area_code", "finding_process_code", "finding_wc_code", "build_part_no", "build_part_desc", "build_serial_no", "assem_part_no", "assem_part_desc", "assem_serial_no", "order_no", "defect_code", "failed_test_code", "fail_type", "fault_board_no", "cmpnt_part_no", "cmpnt_part_desc", "ref_designator", "defect_count", "repair_date")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    
   val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_ftdefects")

    renameDf.rdd.unpersist(true)

  }
  def insertInto_assydetail(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_assydetail_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("PLANT", "plant")
      .withColumnRenamed("Assembly", "assembly")
      .withColumnRenamed("Rev", "rev")
      .withColumnRenamed("Side", "side")
      .withColumnRenamed("PCB", "pcb")
      .withColumnRenamed("Family #", "family")
      .withColumnRenamed("Family Description", "family_description")
      .withColumnRenamed("Side Glue", "side_glue")
      .withColumnRenamed("Circuits Quantity", "circuits_quantity")
      .withColumnRenamed("PCB Length", "pcb_length")
      .withColumnRenamed("PCB Width", "pcb_width")
      .withColumnRenamed("Placement/Panel", "placement_panel")
      .withColumnRenamed("Flatprogram", "flat_program")
      .withColumnRenamed("Stencil Name", "stencil_name")
      .withColumnRenamed("Thickness", "thickness")
    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "plant", "assembly", "rev", "file_name", "hdfs_file_name", "kafka_topic", "side", "pcb", "family", "family_description", "side_glue", "circuits_quantity", "pcb_length", "pcb_width", "placement_panel", "flat_program", "stencil_name", "thickness")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/assydetail")

    renameDf.rdd.unpersist(true)

  }
  def insertInto_units(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_units_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("SERIAL_NO", "serial_no")
      .withColumnRenamed("PANEL_NO", "panel_no")
      .withColumnRenamed("MODULE_NO", "module_no")
      .withColumnRenamed("ORDER_NO", "order_no")
    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "serial_no", "panel_no", "file_name", "hdfs_file_name", "kafka_topic", "module_no", "order_no")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
       val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/units")
    renameDf.rdd.unpersist(true)

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
       val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/wodetail")
    renameDf.rdd.unpersist(true)

  }

  def insertInto_assycircuitsilk(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {

    var tableName = propertiesBroadcast.value.getProperty("ea_assycircuitsilk_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat))
      .withColumnRenamed("PLANT", "plant")
      .withColumnRenamed("Assembly", "assembly")
      .withColumnRenamed("Revision", "revision")
      .withColumnRenamed("Side", "side")
      .withColumnRenamed("Circuit #", "circuit")
      .withColumnRenamed("Silkscreen", "silkscreen")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "file_name", "hdfs_file_name", "kafka_topic", "plant", "assembly", "revision", "side", "circuit", "silkscreen")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/assycircuitsilk")

    renameDf.rdd.unpersist(true)
  }
  def insertInto_parentchild(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_parentchild_table")

    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumnRenamed("PARENT_SERIAL_NO", "parent_serial_no")
      .withColumnRenamed("CHILD_SERIAL_NUMBER", "child_serial_number")
      .withColumn("CHILD_FINISH_DATE", unix_timestamp(df("CHILD_FINISH_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("CHILD_FINISH_DATE", "child_finish_date")
      .withColumn("CONSUMPTION_DATE", unix_timestamp(df("CONSUMPTION_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("CONSUMPTION_DATE", "consumption_date")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "parent_serial_no", "child_serial_number", "file_name", "hdfs_file_name", "kafka_topic", "child_finish_date", "consumption_date")
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath=  propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/parentchild")

    renameDf.rdd.unpersist(true)

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
    val hdfsPath = propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/ea_comptype")

    renameDf.rdd.unpersist(true)

  }

  def insertInto_workorder(df: DataFrame, propertiesBroadcast: Broadcast[Properties]): Unit = {
    var tableName = propertiesBroadcast.value.getProperty("ea_workorder_table")
    val renameDf = df.withColumn("bicoe_load_dttm", unix_timestamp(df("bicoe_load_dttm"), dateTimeFormat).cast("timestamp"))
      .withColumn("RELEASE_DATE", unix_timestamp(df("RELEASE_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("RELEASE_DATE", "release_date")
      .withColumn("BOM_DATE", unix_timestamp(df("BOM_DATE"), dateTimeFormat).cast("timestamp")).withColumnRenamed("BOM_DATE", "bom_date")
      .withColumnRenamed("ORDER_NUMBER", "order_number")
      .withColumnRenamed("PART_NUMBER", "part_number")
      .withColumnRenamed("PART_REVISION", "part_revision")
      .withColumnRenamed("DESCRIPTION", "description")
      .withColumnRenamed("PLANT_CODE", "plant_code")
      .withColumnRenamed("ORDER_TYPE", "order_type")
      .withColumnRenamed("PROD_SCHEDULER", "prod_scheduler")
      .withColumnRenamed("PLANNED_ROUTE", "planned_route")

    val readyToSaveDf = renameDf.select("xml_tag_name", "xml_status", "bicoe_load_dttm", "xml_tag_id", "file_name", "hdfs_file_name", "kafka_topic", "order_number", "part_number", "part_revision", "description", "plant_code", "order_type", "prod_scheduler", "release_date", "bom_date", "planned_route")
    //insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    val hdfsPath = propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/workorder")

    renameDf.rdd.unpersist(true)
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
   // insertInto_hive(readyToSaveDf, tableName, propertiesBroadcast);
    
    val hdfsPath = propertiesBroadcast.value.getProperty("dataFramePath")
    readyToSaveDf.write.mode("append").format("parquet").save(hdfsPath+"/equipmentoutline")
    renameDf.rdd.unpersist(true)
  }

}