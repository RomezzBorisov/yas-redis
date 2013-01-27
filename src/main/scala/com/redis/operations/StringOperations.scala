package com.redis.operations

import com.redis.{Parse, Format, RedisClient}
import ResponseUnbox._
import akka.dispatch.Future
import StringArrayUtil._

trait StringOperations {
  self: RedisClient =>

  def append[T](key: String, v: T)(implicit fmt: Format): Future[Long] =
    submitCommand("APPEND", key, Array(key, fmt(v))).map(UnboxIntegral)

  def bitcount(key: String, start: Int = 0, end: Int = -1): Future[Long] =
    submitCommand("BITCOUNT", key, Array(key, start.toString, end.toString)).map(UnboxIntegral)

  def bitop(op: BitOperation.Value, dest: String, src: Seq[String]): Future[Long] =
    submitCommand("BITOP", toArgsArray(dest, src), toFlatArray(Array(op.toString , dest), src)).map(UnboxIntegral)

  def decr(key: String): Future[Long] =
    submitCommand("DECR", key).map(UnboxIntegral)

  def decrby(key: String, value: Long): Future[Long] =
    submitCommand("DECRBY", key, Array(key, value.toString)).map(UnboxIntegral)

  def get[T](key: String)(implicit parse: Parse[T]): Future[Option[T]] =
    submitCommand("GET", key).map(UnboxBulk andThen(_.map(parse)))

  def getbit(key: String, offset: Int): Future[Long] =
    submitCommand("GETBIT", key, Array(key, offset.toString)).map(UnboxIntegral)

  def getrange(key: String, start: Int, end: Int): Future[Option[String]] =
    submitCommand("GETRANGE", key, Array(key, start.toString, end.toString)).map(UnboxBulk)

  def getset[T](key: String, value: T)(implicit fmt: Format, p: Parse[T]): Future[Option[T]] =
    submitCommand("GETSET", key, Array(key, fmt(value))).map(UnboxBulk.andThen(_.map(p.apply)))

  def incr(key: String): Future[Long] =
    submitCommand("INCR", key).map(UnboxIntegral)

  def incrby(key: String, v: Long): Future[Long] =
    submitCommand("INCRBY", key, Array(key, v.toString)).map(UnboxIntegral)

  def incrbyfloat(key: String, v: Double): Future[Double] =
    submitCommand("INCRBYFLOAT", key, Array(key, v.toString)).map(UnboxBulkAsDouble)

  def mget[T](keys: String*)(implicit p: Parse[T]): Future[Option[Map[String,T]]] =
    submitCommand("MGET", keys.toArray).map(multibulkAsMap(keys))

  def mset[T](keyValues: Map[String, T])(implicit fmt: Format): Future[Boolean] =
    submitCommand("MSET", keyValues.keys.toArray, toFlatArray(keyValues.map {case (k,v) => Array(k,  fmt(v))})).map(UnboxStatusAsBoolean)

  def msetnx[T](keyValues: Map[String, T])(implicit fmt: Format): Future[Boolean] =
    submitCommand("MSETNX",  keyValues.keys.toArray, toFlatArray(keyValues.map {case (k,v) => Array(k,  fmt(v))})).map(UnboxIntAsBoolean)

  def psetex[T](key: String, millis: Long, value: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("PSETEX", key, Array(key, millis.toString, fmt(value))).map(UnboxStatusAsBoolean)

  def set[T](key: String, value: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("SET", key, Array(key, fmt(value))).map(UnboxStatusAsBoolean)

  def setbit(key: String, offset: Long, value: Boolean): Future[Boolean] =
    submitCommand("SETBIT", key, Array(key, offset.toString, if (value) "1" else "0")).map(UnboxIntAsBoolean)

  def setex[T](key: String, seconds: Long, value: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("SETEX", key, Array(key, seconds.toString, fmt(value))).map(UnboxStatusAsBoolean)

  def setnx[T](key: String, value: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("SETNX", key, Array(key, fmt(value))).map(UnboxIntAsBoolean)

  def setrange[T](key: String, offset: Long, value: T)(implicit fmt: Format): Future[Long] =
    submitCommand("SETRANGE", key, Array(key, offset.toString, fmt(value))).map(UnboxIntegral)

  def strlen(key: String): Future[Long] =
    submitCommand("STRLEN", key).map(UnboxIntegral)
}

object BitOperation extends Enumeration {
  val AND = Value("AND")
  val OR = Value("OR")
  val XOR = Value("XOR")
  val NOT = Value("NOT")
}
