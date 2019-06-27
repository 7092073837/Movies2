package org.com.analytics.df

import org.com.analytics.df.loadSourceData.load_movie
import org.com.analytics.df.Spark_Session._
object dramaMovies {
  def filterDramaMovies(): Unit = {


    val movieDF = load_movie.createOrReplaceTempView("movie_tbl")

    val dramaDF = spark.sql("select * from movie_tbl where genres like '%Drama%' ")

    println("Number of Drama movies are:  " + dramaDF.count)

  }
}