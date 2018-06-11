package com.twitter.scalding.bootcamp.reduce

import cascading.pipe.Pipe
import com.twitter.scalding._
class ReduceOperations(args:Args) extends Job(args){

  scanLeftOperations()
  foldLeftOperations()
  scanLeftOperationsGroupAll()

  def scanLeftOperations():Pipe={
    val scanLeftExample =
      IterableSource[(String, Double,Double)](getInMemorySampleData(), ('gender, 'height, 'marks))
        .groupBy('gender) { group =>
          group.sortBy('height).reverse
            .scanLeft(('gender, 'height, 'marks) -> ('rankHeight, 'totalScore, 'uniqueId))(0L, 0.0, "") {
              (prevTuple: (Long, Double, String), currTuple: (String, Double, Double)) => {
                (prevTuple._1 + 1, prevTuple._2 + currTuple._3, currTuple._1 + (prevTuple._1 + 1))
              }
            }
        }
        .debug
        .write( Csv( "src/main/resources/reduce/scanLeft-output.txt"))
    scanLeftExample
  }

  def scanLeftOperationsGroupAll():Pipe={
    val scanLeftExample =
      IterableSource[(String, Double,Double)](getInMemorySampleData(), ('gender, 'height, 'marks))
        .groupAll { group =>
          group.sortBy('height).reverse
            .scanLeft(('gender, 'marks) -> ('rankHeight, 'totalScore, 'uniqueId))(0L, 0.0, "") {
              (prevTuple: (Long, Double, String), currTuple: (String, Double)) => {
                (prevTuple._1 + 1, prevTuple._2 + currTuple._2, currTuple._1 + (prevTuple._1 + 1))
              }
            }
        }
        .debug
        .write( Csv( "src/main/resources/reduce/scanLeftGroupAll-output.txt"))
    scanLeftExample
  }

  def foldLeftOperations():Pipe={
    val scanLeftExample =
      IterableSource[(String, Double,Double)](getInMemorySampleData(), ('gender, 'height, 'marks))
        .groupBy('gender) { group =>
          group.sortBy('height).reverse
            .foldLeft(('gender, 'height, 'marks) -> ('rankHeight, 'totalScore, 'uniqueId))(0L, 0.0, "") {
              (prevTuple: (Long, Double, String), currTuple: (String, Double, Double)) => {
                (prevTuple._1 + 1, prevTuple._2 + currTuple._3, currTuple._1 + (prevTuple._1 + 1))
              }
            }
        }
        .debug
        .write( Csv( "src/main/resources/reduce/foldLeft-output.txt"))
    scanLeftExample
  }

  def getInMemorySampleData()={
    val sampleInput = List(
      ("male", 165.2,50.0),
      ("female", 172.2,100.0),
      ("male", 184.1,23.0),
      ("male", 125.4,39.0),
      ("female", 128.6,79.0)
    )
    sampleInput
  }

}
