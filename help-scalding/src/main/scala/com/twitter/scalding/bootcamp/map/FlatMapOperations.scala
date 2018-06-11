package com.twitter.scalding.bootcamp.map

import cascading.pipe.Pipe
import com.twitter.scalding._

class FlatMapOperations(args:Args) extends Job(args){

  flatMapOperation()
  flatMapToOperation1()
  flatMapToOperation2()

  def flatMapOperation():Pipe={
    val inputSchema = List('name, 'age, 'fruits)
    val outputSinkPath = "src/main/resources/map/flatMap-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/map/flatMap-input.txt",",",inputSchema,false,false,"\"").read
 //   val sourceIterableListTap:Pipe = IterableSource(generateTestData(),inputSchema)
    val mapOperation:Pipe =  csvSourceTap.flatMap(('fruits)->('fruit)){
      fruits:String =>{
        fruits.split(",")
      }
    }
    mapOperation.write(outputSink)
  }

  def flatMapToOperation2():Pipe={
    val inputSchema = List('name, 'age, 'fruits)
    val outputSinkPath = "src/main/resources/map/flatMapTo2-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/map/flatMap-input.txt",",",inputSchema,false,false,"\"").read
    //val sourceIterableListTap:Pipe = IterableSource(generateTestData(),inputSchema)
    val mapOperation:Pipe =  csvSourceTap.flatMapTo(('name,'fruits)->('name,'fruit)){
      tuple:(String,String) => {
        val (name:String,fruits:String) = tuple
        val list:List[String] = fruits.split(",").toList
        list.map(fruit => (name,fruit))
      }
    }
    mapOperation.write(outputSink)
  }

  def flatMapToOperation1():Pipe={
    val inputSchema = List('name, 'age, 'fruits)
    val outputSinkPath = "src/main/resources/map/flatMapTo1-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/map/flatMap-input.txt",",",inputSchema,false,false,"\"").read
   // val sourceIterableListTap:Pipe = IterableSource(generateTestData(),inputSchema)
    val mapOperation:Pipe =  csvSourceTap.flatMapTo('fruits->'fruit){
      fruits:String => {
        fruits.split(",").toList
      }
    }
    mapOperation.write(outputSink)
  }

  def generateTestData():List[(String,Int,String)]={

    val sampleList = List(
      ("john", 4, "orange,apple"),
      ("liza", 5, "banana,mango"),
      ("nina", 5 ,"orange"),
      ("nick", 6, "")
    )
    sampleList
  }
}
