package com.twitter.scalding.bootcamp.join


import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.pipe.joiner.{LeftJoin, OuterJoin, RightJoin}

class JoinOperations(args:Args) extends Job(args){

  joinWithSmallerMetadata()

  def joinWithSmallerMetadata():Unit ={
    val movieMetadataPipe =
      IterableSource[(Int,String)](getMoviesMetadata, ('movieId,'title))

    val movieWatchedPipe =
      IterableSource[(String, Int)](getUserMoviesData, ('userId,'movieId))

      movieWatchedPipe.joinWithSmaller('movieId->'movieId,movieMetadataPipe)
      .write(Csv("src/main/resources/join/joinWithSmaller-output.txt"))


      val movieWatchedPipe_ = movieWatchedPipe.rename('movieId -> 'movieId_)

      val leftJoinedPipe = movieWatchedPipe_.joinWithSmaller('movieId_ -> 'movieId, movieMetadataPipe, joiner= new LeftJoin)
        .write(Csv("src/main/resources/join/joinWithSmaller-leftJoinOutput.txt"))

      val rightJoinedPipe = movieWatchedPipe_.joinWithSmaller('movieId_ -> 'movieId, movieMetadataPipe, joiner= new RightJoin)
        .write(Csv("src/main/resources/join/joinWithSmaller-rightJoinOutput.txt"))

      val outerJoinedPipe = movieWatchedPipe_.joinWithSmaller('movieId_ -> 'movieId, movieMetadataPipe, joiner= new OuterJoin)
        .map('title -> 'title2) { x:String => x+"a" }
        .write(Csv("src/main/resources/join/joinWithSmaller-outerJoinOutput.txt"))

  }

  //  val movieMetadataPipe =
  //    IterableSource[(Int,String)](movieMetadataList, ('movieId,'title))
  //
  //  val movieWatchedPipe =
  //    IterableSource[(String, Int)](moviesWatchedPipe, ('userId,'movieId))
  //
  //  // By default all joins are inner-joins
  //  val joinedPipe = movieMetadataPipe.joinWithSmaller('movieId -> 'movieId, movieWatchedPipe)
  //    .write(Tsv("Output-joinWithSmaller"))


  def getUserMoviesData = {
    List(
      ("user1", 1),
      ("user2", 1),
      ("user2", 2),
      ("user3", 4))
  }

  def getMoviesMetadata = {
    List(
      (1, "title1"),
      (2, "title2"),
      (3, "title3"))
  }
}
