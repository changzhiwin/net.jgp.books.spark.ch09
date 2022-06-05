package net.jgp.books.spark.ch09.x.ds.exif

import java.io.File

// path / recursive / limit / extensions
class RecursiveExtensionFilteredLister(val parameters: Map[String, String]) {

  def collectFiles(): Seq[File] = {
    val path = parameters.getOrElse("path", "./data")
    val limit = parameters.getOrElse("limit", "1000").toInt
    val recursive = parameters.getOrElse("recursive", "true").toBoolean
    val extensions = parameters.getOrElse("extensions", "jpg").split(',').toSeq

    getListOfFiles(new File(path), recursive, extensions, limit)
  }

  private def getListOfFiles(d: File, recursive: Boolean, extensions: Seq[String], limit: Int): Seq[File] = {

    if (d.exists && d.isDirectory) {

      val (files, dirs) = d.listFiles.partition(_.isFile)

      val matchFiles = files.filter(file => {
        val suffix = file.getName().split('.').last
        extensions.contains(suffix)
      }).toSeq

      val allFiles = recursive match {
        case true  => {
          dirs.foldLeft(matchFiles)((had, dd) => {
            if (had.length < limit) {
              had ++ getListOfFiles(dd, recursive, extensions, limit - had.length)
            } else {
              had
            }
          })
        }
        case false => matchFiles
      }

      if (allFiles.length > limit) {
        allFiles.take(limit)
      } else {
        allFiles
      }
      
    } else {
      Seq.empty
    }
  }
}