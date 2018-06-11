package com.twitter.scalding.bootcamp.fileformats

import cascading.pipe.Pipe
import com.twitter.scalding._
class FileFormatsReader(args:Args) extends Job(args){

  textFileReader()
  csvFileReader()
  tabFileReader()

  def textFileReader():Pipe = {
    val inputSourcePath = "src/main/resources/formats/text-input.txt"
    val outputSinkPath = "src/main/resources/formats/text-output.txt"
    val inputSourceTap:TextLine = TextLine(inputSourcePath)
    val outputSink:TextLine = TextLine(outputSinkPath)
    val pipe:Pipe = inputSourceTap.read
    pipe.write(outputSink)
  }

  def csvFileReader():Pipe = {
    val inputSourcePath = "src/main/resources/formats/csv-input.txt"
    val outputSinkPath = "src/main/resources/formats/csv-output.txt"

    val inputSchema = List('column1, 'column2, 'column3)
    val separator = ","

    val inputSourceTap:Csv = Csv(inputSourcePath,separator,inputSchema)
    val outputSink:Csv = Csv(outputSinkPath)
    val pipe:Pipe = inputSourceTap.read
    pipe.write(outputSink)
  }

  def tabFileReader():Pipe ={
    val inputSourcePath = "src/main/resources/formats/tab-input.txt"
    val outputSinkPath = "src/main/resources/formats/tab-output.txt"

    val inputSchema = List('column1, 'column2)

    val inputSourceTap:Tsv = Tsv(inputSourcePath,inputSchema)
    val outputSink:Tsv = Tsv(outputSinkPath)
    val pipe:Pipe = inputSourceTap.read
    pipe.write(outputSink)

  }
}
