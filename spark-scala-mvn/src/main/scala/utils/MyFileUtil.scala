package utils

import java.io.File

/**
 * 文件操作
 *
 * @author admin 2020-5-25
 */
object MyFileUtil {
  var inputPath = "file:///".concat("C:/tatas/spark/")

  /**
   * 删除文件夹及其子文件
   *
   * @param path 文件目录（使用最后一层目录）
   * @return
   */
  def deleteOutputPath(path: String) = {
    //  var file = Source.fromFile("C:\\Users\\workspace\\work_idea\\sparks\\result\\part-00000")
    val checkPath = new File(path)
    println(checkPath.exists())
    // if not exists return false
    println(checkPath.isFile())
    if (checkPath.isDirectory) {
      val files = checkPath.listFiles()
      for (file <- files) {
        /* 这里默认为最后一层目录，里面全是文件不含文件夹，不再校验*/
        println("=== deleted " + file.getName)
        file.delete()
      }
      // delete 空文件夹
      if (files.size == 0) {
        checkPath.delete()
      }
    } else if (checkPath.isFile()) {
      checkPath.delete()
    }
  }
}
