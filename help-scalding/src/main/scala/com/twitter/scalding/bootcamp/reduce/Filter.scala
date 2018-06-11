package com.twitter.scalding.bootcamp.reduce
import cascading.pipe.Pipe
import com.twitter.scalding._


class FilterOperations (args:Args) extends Job(args) {
  filterOperation1()
  filterOperation2()

  def filterOperation1(): Pipe = {
    val inputSchema = List('name,'fruit)
    val outputSinkPath = "src/main/resources/reduce/filter1-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/reduce/filter-input.txt",",",inputSchema,false,false,"\"").read
    val filterOperation:Pipe =  csvSourceTap.filter(('name)){
      name:String =>{
        name == "john"
      }
    }
    filterOperation.write(outputSink)
  }
  def filterOperation2(): Pipe = {
    val inputSchema = List('name,'fruit)
    val outputSinkPath = "src/main/resources/reduce/filter2-output.txt"
    val outputSink = Csv(outputSinkPath)

    val csvSourceTap = Csv("src/main/resources/reduce/filter-input.txt",",",inputSchema,false,false,"\"").read
    val filterOperation:Pipe =  csvSourceTap.filter(('name,'fruit)){
      tuple:(String,String) =>{
        val (name, fruit) = tuple
        name == "liza" || fruit == "mango"
      }
    }
    filterOperation.write(outputSink)
  }
}