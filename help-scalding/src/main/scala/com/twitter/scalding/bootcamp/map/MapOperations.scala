package com.twitter.scalding.bootcamp.map

import cascading.pipe.Pipe
import com.twitter.scalding._


class MapOperations(args:Args) extends Job(args){

  mapOperation()
  mapToOperation()

  def mapOperation():Pipe={
    val inputSchema = List('productId, 'price, 'quantity)
    val outputSinkPath = "src/main/resources/map/map-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/map/map-input.txt",",",inputSchema,false,false).read
   // val sourceIterableListTap:Pipe = IterableSource(generateTestData(),inputSchema)
    val mapOperation:Pipe =  csvSourceTap.map(('price,'quantity)->('total)){
      tuple:(Int,Int) =>{
        val (price,quantity) = tuple
        price*quantity
      }
    }
    mapOperation.write(outputSink)
  }

  def mapToOperation():Pipe={
    val inputSchema = List('productId, 'price, 'quantity)
    val outputSinkPath = "src/main/resources/map/mapTo-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/map/mapTo-input.txt",",",inputSchema,false,false).read
    // val sourceIterableListTap:Pipe = IterableSource(generateTestData(),inputSchema)
    val mapOperation:Pipe =  csvSourceTap.mapTo(('productId,'price,'quantity)->('productId,'total)){
      tuple:(String,Int,Int) =>{
        val (productId,price,quantity) = tuple
        (productId,price*quantity)
      }
    }
    mapOperation.write(outputSink)
  }


  def generateTestData():List[(String,Int,Int)]={

    val sampleList = List(
      ("1", 10, 5),
      ("1", 9, 3),
      ("1", 11, 7),
      ("1", 3, 13)
    )
    sampleList
  }

}
