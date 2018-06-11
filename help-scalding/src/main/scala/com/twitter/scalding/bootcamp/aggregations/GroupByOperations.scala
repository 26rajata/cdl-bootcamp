package com.twitter.scalding.bootcamp.aggregations

import cascading.pipe.Pipe
import com.twitter.scalding._
class GroupByOperations(args:Args) extends Job(args) {

  aggregateOperationGroupBy()
  operationGroupBySortBy()

  def aggregateOperationGroupBy(): Pipe = {
    val inputSchema = List('name, 'age, 'fruit)
    val outputSinkPath = "src/main/resources/aggregations/groupBy-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/aggregations/groupBy-input.txt", ",", inputSchema, false, false, "\"").read

    val groupByAggregation: Pipe = csvSourceTap.groupBy('fruit) {
      group => {
        group.min('age -> 'minAge)
          .max('age -> 'maxAge)
          .average('age -> 'averageAge)
          .sum[Double]('age -> 'totalAge)
          .size
      }
    }
    groupByAggregation.write(outputSink)
  }

  def operationGroupBySortBy(): Pipe = {

    val inputSchema = List('name, 'age, 'fruit)
    val outputSinkPath = "src/main/resources/aggregations/groupBySortBy-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/aggregations/groupBy-input.txt", ",", inputSchema, false, false, "\"").read

    val groupBySortByAggregation: Pipe = csvSourceTap.groupBy('fruit) {
      group => {
        group.sortBy('age).take(1)
        //group.sortBy('age)
      }
    }
    groupBySortByAggregation.write(outputSink)
  }

  def sortWithTake():Pipe = {
    val inputSchema = List('name, 'age, 'fruit)
    val outputSinkPath = "src/main/resources/aggregations/groupBySortWithTakeBy-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/aggregations/groupBy-input.txt", ",", inputSchema, false, false, "\"").read

    val groupBySortByAggregation: Pipe = csvSourceTap.groupBy('fruit) {
      group => {
//        group.sortWithTake('age,1)
        group.sortBy('age).take(1)
        //group.sortBy('age)
      }
    }
    groupBySortByAggregation.write(outputSink)
  }
}
