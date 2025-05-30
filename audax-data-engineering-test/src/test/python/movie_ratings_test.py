import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round as spark_round, col, desc, asc

class MovieRatingsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("MovieRatingsTest") \
            .master("local[*]") \
            .getOrCreate()

        cls.reviews_df = cls.spark.read.option("header", True).option("inferSchema", True) \
            .csv("src/test/resources/movie_review.csv")

        cls.details_df = cls.spark.read.option("header", True).option("inferSchema", True) \
            .csv("src/test/resources/movie_details.csv")

    def test_average_rating_dune(self):
        avg_rating = self.reviews_df \
            .filter(col("movie_name") == "Dune") \
            .agg(spark_round(avg("rating"), 2).alias("avg_rating")) \
            .first()["avg_rating"]

        self.assertEqual(avg_rating, 9.67)

    def test_total_ratings_star_wars(self):
        count = self.reviews_df \
            .filter(col("movie_name") == "Star Wars") \
            .count()

        self.assertEqual(count, 3)

    def test_lowest_avg_user(self):
        result = self.reviews_df.groupBy("user_id") \
            .agg(avg("rating").alias("avg_rating")) \
            .orderBy(asc("avg_rating")) \
            .first()["user_id"]

        self.assertEqual(result, "U003")

    def test_sort_movies_by_avg_rating(self):
        sorted_movies = self.reviews_df.groupBy("movie_name") \
            .agg(avg("rating").alias("avg_rating")) \
            .orderBy(asc("avg_rating")) \
            .select("movie_name") \
            .rdd.flatMap(lambda x: x).collect()

        self.assertEqual(sorted_movies, ["Avatar", "Star Wars", "Dune"])

    def test_avatar_director_and_top_user(self):
        top_user = self.reviews_df \
            .filter(col("movie_name") == "Avatar") \
            .orderBy(desc("rating")) \
            .first()["user_id"]

        director = self.details_df \
            .filter(col("movie_name") == "Avatar") \
            .select("director") \
            .first()["director"]

        self.assertEqual((director, top_user), ("James Cameron", "U001"))


if __name__ == "__main__":
    unittest.main()
