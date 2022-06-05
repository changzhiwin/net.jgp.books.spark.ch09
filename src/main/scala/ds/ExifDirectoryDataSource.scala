package net.jgp.books.spark.ch09.x.ds.exif

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider

class ExifDirectoryDataSource extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    
    val photoLister = new RecursiveExtensionFilteredLister(parameters)

    new ExifDirectoryRelation(sqlContext, photoLister)
  }

}