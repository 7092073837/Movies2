package com.movies.org

import com.movies.org.Spark_Session._
import com.movies.org.Load_Source_Data.load_ratings
import com.movies.org.Load_Source_Data.load_tags

object Movies_Rated_Taged {
  def movieRatedTags() {


    val tags_movies  = load_tags.createOrReplaceTempView("tag_movies_tbl")
    val rated_movies = load_ratings.createOrReplaceTempView("rating_movies_tbl")


    val join_rated_tags = spark.sql("select * from rating_movies_tbl where userId in (select userId from tag_movies_tbl )")
    println("Users rated and tag the movies list  :  "+join_rated_tags.count())

    val join_rated_not_tags = spark.sql("select * from rating_movies_tbl where userId not in (select userId from tag_movies_tbl )")
    println("Users rated and not tag the movies list :  "+join_rated_not_tags.count())


  }

}
