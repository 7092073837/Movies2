package com.movies.org

import com.movies.org.Load_Source_Data.load_ratings
import com.movies.org.Load_Source_Data.load_tags

import com.movies.org.Spark_Session._

object Movie_Ratings {

  def movieRatings(): Unit = {


    val ratings = load_ratings.createOrReplaceTempView("ratings_tbl")

    val tags = load_tags.createOrReplaceTempView("tags_tbl")

    val ratings_tags = spark.sql("select r.userId,r.movieId,tag,rating " +
      "from ratings_tbl r inner join tags_tbl t where r.movieId = t.movieId ")

    val movie_ratings_table = ratings_tags.createOrReplaceTempView("ratings_tags_tbl")


    val movie_ratings = spark.sql("select distinct movieId,min(rating) over (partition by movieId )as min_rating, avg(rating)over (partition by movieId ) as avg_rating ," +
      " max(rating)over (partition by movieId ) as max_rating from ratings_tags_tbl ")

    println("The ratings for movies are : ")
    movie_ratings.show()

  }
}