package com.movies.org

import com.movies.org.Spark_Session._
import org.apache.spark.sql.functions._
import com.movies.org.Load_Source_Data.load_movie

/**
  *
  */
object NumberOfMovies {
    def numOfGenresPerYear(): Unit = {


        val load_movie_data = load_movie.createOrReplaceTempView("movie__tbl")
        import spark.implicits._

        val movie_title = spark.sql("select movieId,substring(title, 1, length(title)-6)as movie_title," +
          "substring(title,length(title)-4,length(title)-3)as year, genres from movie_tbl ")

        movie_title.createOrReplaceTempView("drama_movies_tbl")
        val movie_drama_title_year = spark.sql("select movieid ,movie_title, substring(year,length(year)-4,length(year)-1)as R_year, genres from drama_movies_tbl ")

        println("The list of movies released in associated year: " )
         movie_drama_title_year.show(20)

        val movie_drama_flat_genres = movie_drama_title_year.withColumn("genres", explode(split($"genres", "[|]")))

        movie_drama_flat_genres.createOrReplaceTempView("final_movie_table")

        val num_movies_year = spark.sql("select R_year,movie_title,genres,count(*) over (partition by R_year order by genres asc)as num_of_movies from  final_movie_table")

        println("the number of movies per Genre per Year : " + num_movies_year.show(20))

    }
}