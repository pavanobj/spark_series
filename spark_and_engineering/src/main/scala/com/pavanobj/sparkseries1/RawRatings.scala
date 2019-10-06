package com.medium_spark

import org.apache.spark.sql.Row

case class RawRecords(
                       user_id: Integer = 0,
                       movie_id: Integer = 0,
                       rating: Double = 0,
                       date_of_rating: String = "0000-00-00"
                     )

object RawRatings extends Serializable {
  val userId_indx = 0
  val movieId_indx = 1
  val rating_indx = 2
  val dateOfRating_indx = 3

  def apply(x: Row): RawRecords = {
    new RawRecords(
      user_id = getInt(userId_indx)(x),
      movie_id = getInt(movieId_indx)(x),
      rating = getDbl(rating_indx)(x),
      date_of_rating = getS(dateOfRating_indx)(x)
    )
  }

  def parse(x: Row): RawRecords = {
    apply(x)
  }

  def getInt(ind: Int)(implicit row: Row): Integer = {
    val s = if (row.isNullAt(ind)) 0 else row.getInt(ind)
    s
  }

  def getDbl(ind: Int)(implicit row: Row): Double = {
    val s = if (row.isNullAt(ind)) 0 else row.getDouble(ind)
    s
  }

  def getS(ind: Int)(implicit row: Row): String = {
    getStr(ind)
  }

  def getStr(ind: Int)(implicit row: Row): String = {
    val s = if (row.isNullAt(ind)) "" else row.getString(ind)
    s
  }
}

