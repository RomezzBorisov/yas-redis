package com.redis

import annotation.tailrec

object SomeTest extends App {
  val page = "aaaaaa" + ("cd"*200) + "bbb"
  val maxBestMatchSubStringDistance = 200

  @tailrec
  def getBestMatchSubString(pageIndex: Int, originalTokens: List[String], tokensLeft: List[String], matchSoFar: List[(String, Int)]): Option[Int] = tokensLeft match {
    case Nil => Some(matchSoFar.head._2)
    case h :: tail =>
      page.substring(pageIndex).indexOf(h) match {
        case -1 => None
        case index =>
          //println("###################### " + pageIndex +"        " + tokensLeft + "   " + matchSoFar + " index " + index)
          if (matchSoFar.isEmpty || index <= maxBestMatchSubStringDistance)
            getBestMatchSubString(pageIndex + index + h.length, originalTokens, tail, (h, index + pageIndex) :: matchSoFar)
          else
            getBestMatchSubString(matchSoFar.reverse.head._2 + originalTokens.head.length, originalTokens, originalTokens, Nil)
      }
  }

  getBestMatchSubString(0, List("aaa","bbb"), List("aaa","bbb"), Nil)

}
