package com.twitter.scalding.bootcamp.aggregations

import cascading.pipe.Pipe
import com.twitter.scalding.{Args, IterableSource, Job, TextLine}

class GroupSortTakeOperations (args: Args) extends Job(args) {

  groupBySortedTake()
  groupAllSortedTake()
  groupAllSortWithTake()
  groupBySortWithTake()


  def groupBySortedTake():Pipe = {

    IterableSource[(String, Int, String)](getInMemoryList, ('kid, 'age, 'fruit)).read
      .groupBy('fruit) { group =>
       // group.sortedTake[(Int, String)](('age, 'kid) -> 'sortedAge, 2)
        group.sortedReverseTake[(Int, String)](('age, 'kid) -> 'sortedAge, 2)
      }.write(TextLine("src/main/resources/aggregations/groupBySortedTake-output.txt"))
  }

  def groupAllSortedTake():Pipe = {
      IterableSource[(String, Int, String)](getInMemoryList, ('kid, 'age, 'fruit)).read
        .groupAll{ group =>
          //group.sortedTake[(Int, String, String)](('age, 'kid, 'fruit) -> 'sortedAge, 2)
          group.sortedReverseTake[(Int, String, String)](('age, 'kid, 'fruit) -> 'sortedAge, 2)
        }.write(TextLine("src/main/resources/aggregations/groupAllSortedTake-output.txt"))
  }

  def groupBySortWithTake():Pipe = {
    IterableSource[(String, Int, String)](getInMemoryList, ('kid, 'age, 'fruit)).read
      .groupBy('fruit) { group =>
        group.sortWithTake(('kid,'age)->'sortedList,2){
          (prev:(String,Int),curr:(String,Int)) => {
            prev._2<=curr._2
          }
        }
      }.write(TextLine("src/main/resources/aggregations/groupBySortWithTake-output.txt"))
  }


  def groupAllSortWithTake():Pipe = {
    IterableSource[(String, Int, String)](getInMemoryList, ('kid, 'age, 'fruit)).read
      .groupAll { group =>
        group.sortWithTake(('kid,'age,'fruit)->'sortedList,2){
          (prev:(String,Int,String),curr:(String,Int,String)) => {
            prev._2<=curr._2
          }
        }
      }.write(TextLine("src/main/resources/aggregations/groupAllSortWithTake-output.txt"))
  }

  def getInMemoryList()={
    val kidsList = List(
      ("john", 4,"orange"),
      ("liza", 5,"apple"),
      ("nina", 5,"apple"),
      ("nick", 6,"apple")
    )
    kidsList
  }
}
