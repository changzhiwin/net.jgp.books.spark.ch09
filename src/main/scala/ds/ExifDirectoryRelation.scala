package net.jgp.books.spark.ch09.x.ds.exif

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

import net.jgp.books.spark.ch09.x.extlib.PhotoMetadata

class ExifDirectoryRelation(val sqlContext: SQLContext, val photoLister: RecursiveExtensionFilteredLister) 
    extends BaseRelation with TableScan {

  // abstract BaseRelation
  def schema: StructType = {
    Encoders.product[PhotoMetadata].schema
  }

  // abstract BaseRelation
  // def sqlContext: SQLContext = ???

  // abstract 
  def buildScan(): RDD[Row] = {
    //1, get File collection
    //2, mapping to schema   SparkBeanUtils.getRowFromBean
    //3, paralleliz step 2

    val table = collectData()

    sqlContext.sparkContext.parallelize(table).map( t => Row.fromTuple(t))

  }

  private def collectData(): Seq[PhotoMetadata] = {

    val files = photoLister.collectFiles()

    files.map( f => {
      val name = f.getName()
      val size = f.length()
      val directory = f.getParent

      /*
      Path file = Paths.get(absPath)
      val (fileCreationDate, fileLastAccessDate, fileLastModifiedDate) = try {
        val attr = Files.readAttributes(f, BasicFileAttributes.getClass())

        (attr.creationTime(), attr.lastAccessTime(), attr.lastModifiedTime())

      } catch {
        case e: IOException => println("I/O error while reading attributes of {}. Got {}. Ignoring attributes.")
      }
      
      */

      PhotoMetadata(name, size, directory) // fileCreationDate, fileLastAccessDate, fileLastModifiedDate)
    })
  }

}