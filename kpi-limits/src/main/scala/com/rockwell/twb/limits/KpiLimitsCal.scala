package com.rockwell.twb.limits

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.MutableList
import java.sql.Connection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Properties
import com.rockwell.twb.util.SparkUtils
import java.io.FileInputStream
import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.broadcast.Broadcast
import com.rockwell.twb.kafka.KafkaSink
import com.rockwell.twb.stores.MySqlOffsetsStore
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import org.json.simple.JSONObject
import org.json.simple.JSONArray
import org.apache.spark.sql.functions.unix_timestamp
import com.rockwell.twb.util.Gateway

case class dataMachineId(datapoint: String, avg: Double, machine_id: String, pcb_side: String)

case class machineTagPcb(machine_id: String, machineId_side: String, pcb_side: String)

object KpiLimitsCal {
  val properties = new Properties()
  val time = (java.lang.System.currentTimeMillis() - (24 * 60 * 60 * 1000)) / 1000
  println("last 24 hours time ===========" + time)

  def main(args: Array[String]): Unit = {

    properties.load(new FileInputStream(args(0)))
    val to = properties.getProperty("to_List");
    val from = properties.getProperty("from");
    val subject = properties.getProperty("subject");
    val key = properties.getProperty("sendGridKey");

    try {

      val timestamp = unix_timestamp().cast("Double").cast("timestamp")
      val driverName: String = "org.apache.hive.jdbc.HiveDriver";

      val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
        "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))
      val su = new SparkUtils(properties)

      val sc = su.getSparkContext()

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

      val kafkaSink = sc.broadcast(KafkaSink(props))
      val fpyTableName = properties.getProperty("fpyTable")
      val ffrTableName = properties.getProperty("ffrTable")
      val volumeTableName = properties.getProperty("volumeTable")

      try {
        Class.forName(driverName);
      } catch {
        case e: ClassNotFoundException => e.printStackTrace() // TODO:
      }

      val jdbcUrl: String = properties.getProperty("driverPath");
      var krbCfg: String = "krb5.conf";
      if (args.length > 1) {
        krbCfg = args(1);
      }

      if (debug) {
        println("using " + krbCfg);
        println("Connecting to " + jdbcUrl);
      }

      System.setProperty("java.security.auth.login.config", "gss-jaas.conf");
      System.setProperty("sun.security.jgss.debug", "true");
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      System.setProperty("java.security.krb5.conf", krbCfg);

      val con: Connection = DriverManager.getConnection(jdbcUrl);
      if (debug) {
        System.out.println("Connected!");
      }

      val stmt: Statement = con.createStatement();
      val fpyDf = performCalculations(sc, fpyTableName, stmt, debug, "fpy")
      val ffrDf = performCalculations(sc, ffrTableName, stmt, debug, "ffr")
      val volumeDf = performCalculations(sc, volumeTableName, stmt, debug, "volume")

      if (debug) {
        println("========================= FPY Limits ===============================")
        fpyDf.show()
        println("========================= FFR Limits ===============================")
        ffrDf.show()
        println("========================= Volume Limits ===============================")
        volumeDf.show()
      }

      if (fpyDf != null && ffrDf != null && volumeDf != null) {
        val df1 = fpyDf.join(ffrDf, Seq("machine_id", "pcb_side"))
        val df2 = df1.join(volumeDf, Seq("machine_id", "pcb_side"))

        var sql4 = "select distinct machine_id,concat(machine_id,'-',pcb_side) as machineId_side,pcb_side from " + fpyTableName
        var res2: ResultSet = stmt.executeQuery(sql4);
        var machineTagPcblis = MutableList[machineTagPcb]()
        while (res2.next()) {
          val machine_id = res2.getString("machine_id")
          val machineId_side = res2.getString("machineId_side")
          val pcb_side = res2.getString("pcb_side")

          machineTagPcblis += machineTagPcb(machine_id, machineId_side, pcb_side)
        }
        val machineTagPcbRdd = sc.parallelize(machineTagPcblis)
        var result2: DataFrame = null
        val sqlContext = SQLContext.getOrCreate(machineTagPcbRdd.context)
        import sqlContext.implicits._
        if (!machineTagPcbRdd.isEmpty()) {
          result2 = machineTagPcbRdd.toDF()
        }

        var df: DataFrame = df2.join(result2, Seq("machine_id", "pcb_side"), "right").withColumn("kpi_limit_calculation_time", lit(unix_timestamp().cast("Double").cast("timestamp")))

        if (debug) {
          println("=================================== final Df to produce to kafka ==========================")
          df.show()
        }

        su.produceToKafka(df.toJSON, properties.getProperty("outletKafkaTopic"), kafkaSink)
      } else {
        println("No KPI calculations found for last 24 hours")
      }
    } catch {
      case e1: Exception =>
        Gateway.sendEmail(to, from, subject, e1, key)
        e1.printStackTrace()
    }
  }

  @throws(classOf[Exception])
  def performCalculations(sc: SparkContext, tablename: String, stmt: Statement, debug: Boolean, datapoint_name: String): DataFrame = {
    var sql = "select group_concat(cast(result as string)) as datapoints, machine_id,pcb_side, avg (result) as avg from " + tablename + "  where UNIX_TIMESTAMP(bicoe_load_dttm) > " + time + " group by machine_id,pcb_side "
    if (debug) {
      System.out.println("Running: " + sql);
    }
    var res: ResultSet = stmt.executeQuery(sql);
    var dataMachineIdslis = MutableList[dataMachineId]()
    var machineTagPcblis = MutableList[machineTagPcb]()
    while (res.next()) {
      val datapoints = res.getString("datapoints")
      val avg_fpy = res.getString("avg").toDouble
      val machine_id = res.getString("machine_id")
      val pcb_side = res.getString("pcb_side")
      dataMachineIdslis += dataMachineId(datapoints, avg_fpy, machine_id, pcb_side)
    }
    if (debug) {
      println("dataMachineIdslis.. ", dataMachineIdslis)
    }

    var df_datapoint = calculateLimit(sc, datapoint_name, dataMachineIdslis, debug)
    df_datapoint

  }

  @throws(classOf[Exception])
  def calculateLimit(sc: SparkContext, datapoint_name: String, dataMachineIdslis: MutableList[dataMachineId], debug: Boolean): DataFrame = {
    val dataMachineIdsRdd = sc.parallelize(dataMachineIdslis)
    var result: DataFrame = null
    val sqlContext = SQLContext.getOrCreate(dataMachineIdsRdd.context)
    import sqlContext.implicits._
    if (!dataMachineIdsRdd.isEmpty()) {
      result = dataMachineIdsRdd.map {
        x =>
          val strArr = x.datapoint.split(",")

          var sum = 0.0
          for (i <- 1 until strArr.size) {
            val diff = strArr(i).toDouble - strArr(i - 1).toDouble
            sum += Math.abs(diff)
            if (debug) {
              println("diff..  " + diff + "of strArr(i) " + strArr(i) + "and " + strArr(i - 1))
              println(sum)
            }
          }

          if (strArr.size <= 1) {
            println("invalid number of datapoints")
          }

          var datapoint_ucl = 0.0
          var datapoint_lcl = 0.0
          if ((x.avg + sum / (strArr.size - 1) * 2.660) * 100.0 > 100.0) {
            datapoint_ucl = 100.0
          } else {
            datapoint_ucl = (x.avg + sum / (strArr.size - 1) * 2.660) * 100.0
          }
          //lcl calc
          if ((x.avg - sum / (strArr.size - 1) * 2.660) * 100.0 < 0) {
            datapoint_lcl = 0.0
          } else {
            datapoint_lcl = (x.avg - sum / (strArr.size - 1) * 2.660) * 100.0
          }
          (x.machine_id, x.pcb_side, datapoint_ucl, datapoint_lcl, x.avg)
      }.toDF()

      val columnsRenamed = Seq("machine_id", "pcb_side", datapoint_name + "_ucl", datapoint_name + "_lcl", datapoint_name + "_mean")
      result = result.toDF(columnsRenamed: _*)
    }
    result
  }

}

