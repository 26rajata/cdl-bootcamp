package com.twitter.scalding.bootcamp.exceptions
import cascading.pipe.Pipe
import com.twitter.scalding._

class AddTrap (args: Args) extends Job(args){

  handleExceptionsInPipeline()

  def handleExceptionsInPipeline():Pipe = {
    val inputSourcePath = "src/main/resources/exceptions/csv-input.txt"
    val outputSinkPath = "src/main/resources/exceptions/csv-output.txt"
    val trapSinkPath = "src/main/resources/exceptions/trap-output.txt"


    val inputSchema = List('a, 'b, 'c)
    val separator = ","

    val inputSourceTap:Csv = Csv(inputSourcePath,separator,inputSchema)
    val outputSink:Csv = Csv(outputSinkPath)
    val trapSink:Csv = Csv(trapSinkPath)
    val pipe:Pipe = inputSourceTap.read.map(('b,'c)->'d){
      tuple:(Int,Int) => {
        tuple._1/tuple._2
      }
    }.addTrap(trapSink)
    pipe.write(outputSink)
  }
}
