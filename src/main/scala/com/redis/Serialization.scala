package com.redis

object Format {
  def apply(f: PartialFunction[Any, String]): Format = new Format(f)

  implicit val default: Format = new Format(Map.empty)

  def formatDouble(d: Double, inclusive: Boolean = true) =
    (if (inclusive) ("") else ("(")) + {
      if (d.isInfinity) {
        if (d > 0.0) "+inf" else "-inf"
      } else {
        d.toString
      }
    }

}

class Format(val format: PartialFunction[Any, String]) {
  def apply(in: Any): String =
    (if (format.isDefinedAt(in)) (format(in)) else (in)) match {
      case s: String => s
      case d: Double => Format.formatDouble(d, inclusive = true)
      case x => x.toString
    }

  def orElse(that: Format): Format = Format(format orElse that.format)

  def orElse(that: PartialFunction[Any, String]): Format = Format(format orElse that)
}

object Parse {
  def apply[T](f: (String) => T) = new Parse[T](f)

  object Implicits {
    implicit val parseString = Parse[String](s => s)
    implicit val parseInt = Parse[Int](s => java.lang.Integer.valueOf(s))
    implicit val parseLong = Parse[Long](s => java.lang.Long.valueOf(s))
    implicit val parseDouble = Parse[Double](s => java.lang.Double.valueOf(s))
  }

  implicit val parseDefault = Implicits.parseString


}

class Parse[A](val f: (String) => A) extends ((String) => A) {
  def apply(in: String): A = f(in)
}
