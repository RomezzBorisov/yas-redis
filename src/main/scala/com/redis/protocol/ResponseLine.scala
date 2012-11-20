package com.redis.protocol

sealed trait ResponseLine

object SingleLine extends ResponseLine {
  def unapply(s: String): Option[String] = if (s.charAt(0) == '+')
    Some(s.substring(1))
  else
    None
}

object ErrorLine extends ResponseLine {
  def unapply(s: String): Option[String] = if (s.charAt(0) == '-')
    Some(s.substring(1))
  else
    None
}

object IntegralLine extends ResponseLine {
  def unapply(s: String): Option[Long] = if (s.charAt(0) == ':')
    Some(s.substring(1).toLong)
  else
    None
}

object BytesNumberLine extends ResponseLine {
  def unapply(s: String): Option[Int] = if (s.charAt(0) == '$')
    Some(s.substring(1).toInt)
  else
    None
}

object LinesNumberLine extends ResponseLine {
  def unapply(s: String): Option[Int] = if (s.charAt(0) == '*')
    Some(s.substring(1).toInt)
  else
    None
}

object ValueLine extends ResponseLine {
  def unapply(s: String): Option[String] = Some(s)
}





