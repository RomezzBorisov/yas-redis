package com.redis

import protocol._
import protocol.ErrorReply
import protocol.IntegralReply

object ResponseUnbox {

  val UnboxError: PartialFunction[Reply, Nothing] = {
    case ErrorReply(msg) => throw new Exception("Error executing redis command: " + msg)
  }

  val UnboxIntegral: PartialFunction[Reply, Long] = UnboxError orElse {
    case IntegralReply(i) => i
  }

  val UnboxBulkAsDouble : PartialFunction[Reply, Double] = UnboxBulk andThen {
    case Some(v) => v.toDouble
  }

  val UnboxSingleLine: PartialFunction[Reply, String] = UnboxError orElse {
    case SingleLineReply(s) => s
  }

  val UnboxStatusAsBoolean: PartialFunction[Reply, Boolean] = UnboxSingleLine andThen(_ == "OK")

  val UnboxIntAsBoolean: PartialFunction[Reply, Boolean] = UnboxIntegral.andThen(_ == 1l)

  val UnboxBulk: PartialFunction[Reply, Option[String]] = UnboxError orElse {
    case EmptyBulkReply => None
    case BulkReply(s) => Some(s)
  }

  val UnboxMultibulk: PartialFunction[Reply, Option[Iterable[Option[String]]]] = UnboxError orElse {
    case MultibulkReply(replies) =>
      Some(replies.map {
        case BulkReply(s) => Some(s)
        case EmptyBulkReply => None
      })
    case EmptyMultiBulkReply =>
      None
  }

  def multibulkAsMap[T](mapKeys: Iterable[String])(implicit parse: Parse[T]): PartialFunction[Reply, Option[Map[String, T]]] =
    UnboxMultibulk andThen {
      case Some(valOpts) => Some(mapKeys.zip(valOpts).foldLeft(Map.empty[String, T]) {
        case (m, (mapKey, Some(v))) => m + (mapKey -> parse(v))
        case (m, (_, None)) => m
      })
      case None => None
    }

  def multibulkAsPairMap[T](implicit parse: Parse[T]): PartialFunction[Reply, Option[Map[String, T]]] =
    UnboxMultibulk andThen(_.map(_.flatten.grouped(2).map(_.toList match {
      case k :: v :: Nil => (k, parse(v))
    }).toMap))

  def flattenMultibulk[T](implicit parse: Parse[T]): PartialFunction[Reply, Option[Iterable[T]]] =
    UnboxMultibulk andThen {
      case Some(valOpts) => Some(valOpts.foldLeft(List.empty[T]) {
        case (l, Some(v)) => v :: l
        case (l, None) => l
      })
      case None => None
    }


}
