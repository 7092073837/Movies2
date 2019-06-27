package org.com.analytics.df

import loadSourceData.load_movie
import loadSourceData.load_ratings
import Spark_Session._
object UniqueMovieRatings {

  def uniqueMovies() {


    val moviesDF = load_movie.createOrReplaceTempView("movie_tbl")
    val ratingsDF = load_ratings.createOrReplaceTempView("ratings_tbl")

    val uniqueMoviesRatedDF = spark.sql("select * from movie_tbl where movieId in (select movieId  from ratings_tbl ) ")

    println("Unique rated movies are :  "+uniqueMoviesRatedDF.count())

   val uniqueMoviesNotRatedDF = spark.sql("select * from movie_tbl where movieId not in (select movieId  from ratings_tbl ) ")

    println("Unique movies which are not rated :  "+uniqueMoviesNotRatedDF.count())


  }

}
