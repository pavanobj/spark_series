package com.pavanobj.sparkseries1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

object MoviesAndSpark {

  case class ratingsRec(val user_id: Integer, val mov_watch_count: Integer, val latest_movie_rated: Integer, val first_movie_rated: Integer, val more_than_six_rtngs: String
                        , val less_than_two_rtngs: String, val highest_rating: Double, val lowest_rating: Double)

  //Create LayOut for your Output Table
  val userRatingsLayout = StructType(
    StructField("user_id", IntegerType, true) ::
      StructField("mov_watch_count", IntegerType, true) ::
      StructField("latest_movie_rated", IntegerType, true) ::
      StructField("first_movie_rated", IntegerType, true) ::
      StructField("more_than_six_rtngs", StringType, true) ::
      StructField("less_than_two_rtngs", StringType, true) ::
      StructField("highest_rating", DoubleType, true) ::
      StructField("lowest_rating", DoubleType, true) ::
      Nil)
  //Create RowEncoder for the above Structype
  val userRatingsColumnsEncoder = RowEncoder(userRatingsLayout)

  //Create main method to Spark up the job.
  def main(args: Array[String]): Unit = {
    //Print Errors only
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Use Spark-Session interface in Spark 2.0
    //Will run it on Local.
    val spark = SparkSession
      .builder()
      .appName("userIdAndRatings")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val rawRecords = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("C:/sparktempfiles/sample.csv")
    rawRecords.printSchema()
    rawRecords.createOrReplaceTempView("raw_records_view")

    //You can also use a map() and apply date converter function to above dataset, instead of Creating a view.
    val rawRecordsTimeFix =
      """
        |select userId, movieId, rating, date_format(from_unixtime(timestamp/1000), 'yyyy-MM-dd') as dateOfRating
        |from raw_records_view
      """.stripMargin
    //Optional to validate the Data.
    //spark.sql(rawRecordsTimeFix).show(false)
    import spark.implicits._
    val rawRecordsConstruct = spark.sql(rawRecordsTimeFix).map(RawRatings.parse(_)).groupByKey(_.user_id).mapGroups {
      case (user_id, ratings_iter) => {
        val ratings_List = ratings_iter.toList
        val ratings_attr_vals = getRatingsAttributes(ratings_List)

        Row(user_id, ratings_attr_vals.mov_watch_count, ratings_attr_vals.latest_movie_rated, ratings_attr_vals.first_movie_rated, ratings_attr_vals.more_than_six_rtngs, ratings_attr_vals.less_than_two_rtngs, ratings_attr_vals.highest_rating, ratings_attr_vals.lowest_rating)
      }
    }(userRatingsColumnsEncoder)

    println("Showing the Results now")
    rawRecordsConstruct.show(false)

  }

  def getRatingsAttributes(rawList: List[RawRecords]): ratingsRec = {
    //Create variables which has to be returned.

    var user_id = 999999
    var mov_watch_count = 999999
    var latest_movie_rated = 999999
    var first_movie_rated = 999999
    var more_than_six_rtngs = "NA"
    var less_than_two_rtngs = "NA"
    var highest_rating: Double = 999999.0
    var lowest_rating: Double = 999999.0


    user_id = rawList.map(x => x.user_id).head.toInt
    mov_watch_count = rawList.map(x => x.movie_id).size
    latest_movie_rated = rawList.sortWith(_.date_of_rating > _.date_of_rating).map(x => x.movie_id).head
    first_movie_rated = rawList.sortWith(_.date_of_rating < _.date_of_rating).map(x => x.movie_id).last
    more_than_six_rtngs = if (rawList.map(x => x.rating).size > 6) "Y" else "NA"
    less_than_two_rtngs = if (rawList.map(x => x.rating).size < 2) "Y" else "NA"
    highest_rating = rawList.map(x => x.rating).sortWith(_ > _).head
    lowest_rating = rawList.map(x => x.rating).sortWith(_ < _).head

    ratingsRec(user_id, mov_watch_count, latest_movie_rated, first_movie_rated, more_than_six_rtngs, less_than_two_rtngs, highest_rating, lowest_rating)
  }

}
