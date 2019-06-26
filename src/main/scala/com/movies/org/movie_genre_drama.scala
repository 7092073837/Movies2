package com.movies.org

import com.movies.org.Load_Source_Data.load_movie
import com.movies.org.Spark_Session._
object movie_genre_drama {
  def movieGenresDrama(): Unit = {


    val load_movie_drama = load_movie.createOrReplaceTempView("movie_tbl")

    val movie_drama_df = spark.sql("select * from movie_tbl where genres like '%Drama%' ")

    println("the number of Drama movies are:  " + movie_drama_df.count)

  }
}
