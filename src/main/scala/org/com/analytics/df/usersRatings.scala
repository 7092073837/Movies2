package org.com.analytics.df

import loadSourceData.load_ratings
import Spark_Session._

object usersRatings {

  def userMostRatings(): Unit = {


    val ratingsDF = load_ratings.createOrReplaceTempView("ratings_tbl")
    val usersWithRatingsDF = spark.sql("select userId , count(rating) as most_ratings from" +
      " ratings_tbl group by userId order by most_ratings desc")

    println("Users rating sorted : ")
    usersWithRatingsDF.show(20)
  }


}
