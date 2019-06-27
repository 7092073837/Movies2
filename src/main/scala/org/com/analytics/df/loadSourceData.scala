package org.com.analytics.df

import Spark_Session.spark

object loadSourceData {

  val  load_movie = spark.read.option("header","true")
    .option("inferSchema","true").option("delimiter", ",").option("encoding", "windows-1252")
    .csv("file:///C:/Users/Public/source/movies.csv")

  val load_ratings = spark.read.option("header", "true")
    .option("inferSchema", "true").option("delimiter", ",").option("encoding", "windows-1252")
    .csv("file:///C:/Users/Public/source/ratings.csv")

  val load_tags = spark.read.option("header", "true")
    .option("inferSchema", "true").option("delimiter", ",").option("encoding", "windows-1252")
    .csv("file:///C:/Users/Public/source/tags.csv")


}
