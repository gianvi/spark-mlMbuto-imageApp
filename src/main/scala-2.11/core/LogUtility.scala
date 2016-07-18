package core

import java.util.logging.{Level => JLevel, Logger => JLogger}

import org.apache.log4j.{Level => L4JLevel, Logger => L4JLogger}
import org.apache.parquet.{Log => ParquetLog}
import org.apache.spark.SparkContext

/**
  * Created by gian on 12/06/16.
  */
object LogUtility {

  //ALL,DEBUG,ERROR,FATAL,INFO,OFF,TRACE,WARN
  def setLogger(sc:SparkContext, scl:String, jl:JLevel, l4jl:L4JLevel) {

    val sclevel: String = scl
    val jlevel: JLevel = jl
    val l4jlevel: L4JLevel = l4jl

    println("Initializing Spark/SQL contexts and setting loggers...")

    val parquetJLogger = JLogger.getLogger(classOf[ParquetLog].getPackage.getName)
    parquetJLogger.setLevel(jlevel)
    println("JLogger parquet: "+parquetJLogger.getName+" set to "+jlevel.toString)

    sc.setLogLevel("WARN")

    val parquetLogger = L4JLogger.getLogger(classOf[ParquetLog].getPackage.getName)
    parquetLogger.setLevel(l4jlevel)
    println("LOG4J parquet: "+parquetLogger.getName+" set to "+l4jlevel.toString)
  }
}
