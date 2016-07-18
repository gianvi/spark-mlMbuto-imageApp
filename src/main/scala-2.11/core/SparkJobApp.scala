package core

import java.util.logging.{Level => JLevel, Logger => JLogger}

import org.apache.log4j.{Level => L4JLevel, Logger => L4JLogger}
import org.apache.parquet.{Log => ParquetLog}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


trait SparkJobApp
  extends App
    with JobRunner {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("GianPipelines")

  implicit val sc = new SparkContext(conf)
  implicit val sqlContext = new SQLContext(sc)


  LogUtility.setLogger(sc, "WARN", JLevel.OFF, L4JLevel.OFF)

}


















//TODO set file log
//PropertyConfigurator.configure("myLog4j.properties")
//val log = LogManager.getRootLogger()
//log.setLevel(L4JLevel.WARN)
//val conf = new SparkConf().setAppName("TestLog")
//log.info("Hello Gian!!")
//val ssc = new StreamingContext(conf, Seconds(1))

//val apacheParquetLogger: Logger = Logger.getLogger("parquet")
//apacheParquetLogger.setLevel(Level.ERROR)




//println("LLLLLLLL: "+apacheParquetLogger.getName)
// and if not it creates them. If this method executes prior
//val parquetLogger: JLogger = JLogger.getLogger("parquet")

//
//    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
//    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
//    parquetLogger.setUseParentHandlers(true)