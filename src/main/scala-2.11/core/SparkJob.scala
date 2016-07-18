package core

import org.apache.log4j.{Logger => L4JLogger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


abstract trait SparkJob[U] {
  @transient lazy val log = L4JLogger.getLogger(getClass.getName)

  var next: Option[(U) => SparkJob[_]] = None

  def execute(implicit sc: SparkContext, sqlContext: SQLContext): U

  def run(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {
    val start = System.currentTimeMillis()
    log.fatal(s"   job execution started")
    val res = execute
    log.fatal(s"   job execution done in ${(System.currentTimeMillis() - start) / 100 / 10.0}s")

    next match {
      case None => log.fatal("\n\nNo job found. End of Pipeline.\n\n")
      case Some(e) =>
        val job = e(res)
        log.fatal(s" \n\nNext job ${job.getClass} detected\n\n")

        job.run(sc, sqlContext)

    }
  }

  def map(job: (U) => Unit) = this

  def flatMap[T <: SparkJob[_]](job: (U) => T) = {
    next = Some(job)

    this
  }
}


