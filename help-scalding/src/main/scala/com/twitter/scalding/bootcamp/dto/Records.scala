package com.twitter.scalding.bootcamp.dto

object Records {

  case class LogEvent(user: String, track: String, timestamp: Long)
  case class Rating(user: String, item: String, score: Double)
  case class UserMeta(user: String, age: Int)

}