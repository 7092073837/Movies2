package com.movies.org

import com.movies.org.Load_Source_Data.load_movie
import com.movies.org.Load_Source_Data.load_ratings
import com.movies.org.Spark_Session._
object Unique_Movie_Ratings {

  def uniqueMovies() {


    val movie_id = load_movie.createOrReplaceTempView("movie_tbl")
    val ratings = load_ratings.createOrReplaceTempView("ratings_tbl")

    val unique_movies_rated = spark.sql("select * from movie_tbl where movieId in (select movieId  from ratings_tbl ) ")

    println("Unique  Rated movies are :  "+unique_movies_rated.count())

   val unique_movies_notrated = spark.sql("select * from movie_tbl where movieId not in (select movieId  from ratings_tbl ) ")

    println("Unique  movies Which are not Rated :  "+unique_movies_notrated.count())


  }

}
