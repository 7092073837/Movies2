package com.movies.org

import com.movies.org.Load_Source_Data.load_ratings
import com.movies.org.Spark_Session._
object Users_most_Ratings {

    def userMostRatings(): Unit = {



    val ratings = load_ratings.createOrReplaceTempView("ratings_tbl")
   val user_most_rating = spark.sql("select userId , count(rating) as most_ratings from" +
     " ratings_tbl group by userId order by most_ratings desc")

    println("Users most number  of ratings to words movie : ")
    user_most_rating.show(20)
  }


}
