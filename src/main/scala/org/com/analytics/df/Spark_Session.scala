package org.com.analytics.df

import org.apache.spark.sql.SparkSession

object Spark_Session {

  val spark = SparkSession
    .builder()
    .appName("spark_application_name")
    .master(("local"))
    .getOrCreate()

}
