package com.rockwell.twb

import java.io.FileInputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util.Properties

import com.rockwell.twb.util.SparkUtils

object ImpalaTest {
  val properties = new Properties()

  def main(args: Array[String]): Unit = {

    properties.load(new FileInputStream(args(0)))

    try {

      val driverName: String = properties.getProperty("jdbcdriver")
      

      try {
        Class.forName(driverName);
      } catch {
        case e: ClassNotFoundException => e.printStackTrace() // TODO:
      }

      val su = new SparkUtils(properties)
      val conf = su.getSparkConf();

      val sc = su.getSparkContext(conf)

      val jdbcUrl: String = properties.getProperty("jdbcUrl")
      println("Connecting to " + jdbcUrl);

      
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

      val con: Connection = DriverManager.getConnection(jdbcUrl);
      System.out.println("Connected!");

      val stmt: Statement = con.createStatement();

      var sql4 = properties.getProperty("query");
      var res2: ResultSet = stmt.executeQuery(sql4);
      println(res2.getRow)
      while (res2.next()) {
        println(res2.getString(properties.getProperty("column")))
      }

    } catch {
      case e1: Exception =>
        e1.printStackTrace()
    }
  }

}

