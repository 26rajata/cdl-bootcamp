package com.twitter.scalding.bootcamp.aggregations

import cascading.pipe.Pipe
import com.twitter.scalding._
class GroupAllOperations(args:Args) extends Job(args){

  topXPercentage()
  def topXPercentage():Pipe = {

    val X:Double = 0.3
    val inputSchema = List('name, 'marks)
    val outputSinkPath = "src/main/resources/aggregations/topXPercentage-output.txt"
    val outputSink = Csv(outputSinkPath)
    val csvSourceTap = Csv("src/main/resources/aggregations/topXPercentage-input.txt", ",", inputSchema, false, false, "\"").read

    val topXPercentagePipe: Pipe = csvSourceTap.map('marks,'marksInt){
          tuple:Int => tuple
        }.discard('marks).groupAll{
          group => group.sortBy('marksInt).reverse
        }.sample(X)
    topXPercentagePipe.write(outputSink)
  }

}
