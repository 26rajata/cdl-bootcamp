package com.twitter.scalding.bootcamp.basicpipelines

import com.twitter.scalding._
import com.twitter.scalding.bootcamp.dto.Records.Rating
class AverageScorePerItem(args:Args) extends Job(args){

  def scalding(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    input
      .groupBy(_.user)
      // Map into (sum, count)
      .mapValues(x => (x.score, 1L))
      // Sum both per key with an implicit `Semigroup[(Double, Long)]`
      .sum
      // Map (sum, count) into average
      .mapValues(p => p._1 / p._2)
      .toTypedPipe
  }
}
