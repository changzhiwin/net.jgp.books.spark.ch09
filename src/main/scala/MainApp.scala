package net.jgp.books.spark

import net.jgp.books.spark.basic.Basic

object MainApp extends Basic {
  def main(args: Array[String]) = {

    val spark = getSession("EXIF to Dataset")
    
    val importDirectory = "data"

    val df = spark.read.
      format("exif").
      option("recursive", "true").
      option("limit", "10000").
      option("extensions", "jpg,jpeg").
      load(importDirectory)

      df.show(5)
      df.printSchema()
      println(s"I have imported ${df.count()} photos.")
  }
}