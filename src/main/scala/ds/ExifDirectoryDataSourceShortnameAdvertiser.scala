package net.jgp.books.spark.ch09.x.ds.exif

import org.apache.spark.sql.sources.DataSourceRegister

class ExifDirectoryDataSourceShortnameAdvertiser extends ExifDirectoryDataSource with DataSourceRegister {
  def shortName(): String = "exif"
}