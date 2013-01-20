package com.redis.operations

object OperationUtil {

  def toArgsArray(key: String, argsSoFar: Iterable[String]): Array[String] = {
    val arr = new Array[String](argsSoFar.size + 1)
    arr(0) = key
    val iter = argsSoFar.iterator
    for (i <- 1 until arr.length;
         arg = iter.next())
      arr(i) = arg
    arr
  }

  private def fill(arr: Array[String], offset: Int, vals: Iterable[String]) = {
    var i = offset
    for (v <- vals) {
      arr(i) = v
      i += 1
    }
    arr
  }

  def toFlatArray(parts: Iterable[Array[String]]): Array[String] = {
    val arr = new Array[String](parts.map(_.length).sum)
    var i = 0
    for (part <- parts) {
      System.arraycopy(part, 0, arr, i, part.length)
      i += part.size
    }
    arr
  }

  def toFlatArray(prefix: Array[String], suffix: Iterable[String]): Array[String] = {
    val arr = new Array[String](prefix.length + suffix.size)
    System.arraycopy(prefix, 0, arr, 0, prefix.length)
    fill(arr, prefix.length, suffix)
  }

}
