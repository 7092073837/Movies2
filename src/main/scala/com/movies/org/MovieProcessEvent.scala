package com.movies.org
import org.apache.log4j.{Level, Logger}
import com.movies.org.Unique_Movie_Ratings.uniqueMovies
import com.movies.org.movie_genre_drama.movieGenresDrama
import com.movies.org.Movie_Ratings.movieRatings
import com.movies.org.Movies_Rated_Taged.movieRatedTags
import com.movies.org.NumberOfMovies.numOfGenresPerYear
import com.movies.org.Users_most_Ratings.userMostRatings

object MovieProcessEvent {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    movieGenresDrama()
    movieRatings()
    uniqueMovies()
    userMostRatings()
    movieRatedTags()
    numOfGenresPerYear()


  }
}
