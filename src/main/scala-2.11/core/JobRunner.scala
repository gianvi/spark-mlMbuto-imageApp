package core

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

trait JobRunner {
  def run[T <: SparkJob[_]](job: T)(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {
    try {
      job.run(sc, sqlContext)
    } finally {
      sc.stop()
    }
  }
}



