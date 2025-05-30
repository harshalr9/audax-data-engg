package question

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MovieRatingsTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("MovieRatingsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val reviewsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/test/resources/movie_review.csv")

  val detailsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/test/resources/movie_details.csv")

  test("Test 1: What is the average ratings of the movie Dune?  Round to 2 decimal places.") {
    val answer = reviewsDF
      .filter($"movie_name" === "Dune")
      .agg(round(avg("rating"), 2).as("avg_rating"))
      .first()
      .getDouble(0)

    assert(answer == 9.67)
  }

  test("Test 2: What is total number of ratings given to movie Star Wars?") {
    val count = reviewsDF
      .filter($"movie_name" === "Star Wars")
      .count()

    assert(count == 3)
  }

  test("Test 3: Display the user_id who gives lowest average rating in all their reviews.") {
    val userId = reviewsDF
      .groupBy("user_id")
      .agg(avg("rating").as("avg_rating"))
      .orderBy(asc("avg_rating"))
      .first()
      .getString(0)

    assert(userId == "U003")
  }

  test("Test 4: Sort and display the name of the movies from lowest average rating to highest average rating.") {
    val sortedMovieNames = reviewsDF
      .groupBy("movie_name")
      .agg(avg("rating").as("avg_rating"))
      .orderBy(asc("avg_rating"))
      .select("movie_name")
      .as[String]
      .collect()
      .toList

    assert(sortedMovieNames == List( "Avatar", "Star Wars", "Dune"))
  }

  test("Test 5: Display the name of director of movie Avatar along with the user_id who gave the movie the highest rating.  Display director name and user_id in a Tuple.") {
    val avatarReviews = reviewsDF
      .filter($"movie_name" === "Avatar")
      .orderBy(desc("rating"))

    val topUser = avatarReviews.first().getString(0)

    val director = detailsDF
      .filter($"movie_name" === "Avatar")
      .select("director")
      .first()
      .getString(0)

    val result = (director, topUser)

    assert(result == ("James Cameron", "U001"))
  }
}
