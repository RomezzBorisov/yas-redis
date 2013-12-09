package com.redis.operations

import com.redis.{Parse, Format, RedisClient}
import ResponseUnbox._
import StringArrayUtil._
import scala.concurrent.{ExecutionContext, Future}

trait StringOperations {
  self: RedisClient =>

  def append[T](key: String, v: T)(implicit fmt: Format, ctx: ExecutionContext): Future[Long] =
    submitCommand("APPEND", key, Array(key, fmt(v))).map(UnboxIntegral)

  def bitcount(key: String, start: Int = 0, end: Int = -1)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("BITCOUNT", key, Array(key, start.toString, end.toString)).map(UnboxIntegral)

  def bitop(op: BitOperation.Value, dest: String, src: Seq[String])(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("BITOP", toArgsArray(dest, src), toFlatArray(Array(op.toString , dest), src)).map(UnboxIntegral)

  def decr(key: String)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("DECR", key).map(UnboxIntegral)

  def decrby(key: String, value: Long)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("DECRBY", key, Array(key, value.toString)).map(UnboxIntegral)

  def get[T](key: String)(implicit parse: Parse[T], ctx: ExecutionContext): Future[Option[T]] =
    submitCommand("GET", key).map(UnboxBulk andThen(_.map(parse)))

  def getbit(key: String, offset: Int)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("GETBIT", key, Array(key, offset.toString)).map(UnboxIntegral)

  def getrange(key: String, start: Int, end: Int)(implicit ctx: ExecutionContext): Future[Option[String]] =
    submitCommand("GETRANGE", key, Array(key, start.toString, end.toString)).map(UnboxBulk)

  def getset[T](key: String, value: T)(implicit fmt: Format, p: Parse[T], ctx: ExecutionContext): Future[Option[T]] =
    submitCommand("GETSET", key, Array(key, fmt(value))).map(UnboxBulk.andThen(_.map(p.apply)))

  def incr(key: String)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("INCR", key).map(UnboxIntegral)

  def incrby(key: String, v: Long)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("INCRBY", key, Array(key, v.toString)).map(UnboxIntegral)

  def incrbyfloat(key: String, v: Double)(implicit ctx: ExecutionContext): Future[Double] =
    submitCommand("INCRBYFLOAT", key, Array(key, v.toString)).map(UnboxBulkAsDouble)

  def mget[T](keys: String*)(implicit p: Parse[T], ctx: ExecutionContext): Future[Option[Map[String,T]]] =
    submitCommand("MGET", keys.toArray).map(multibulkAsMap(keys))

  def mset[T](keyValues: Map[String, T])(implicit fmt: Format, ctx: ExecutionContext): Future[Boolean] =
    submitCommand("MSET", keyValues.keys.toArray, toFlatArray(keyValues.map {case (k,v) => Array(k,  fmt(v))})).map(UnboxStatusAsBoolean)

  def msetnx[T](keyValues: Map[String, T])(implicit fmt: Format, ctx: ExecutionContext): Future[Boolean] =
    submitCommand("MSETNX",  keyValues.keys.toArray, toFlatArray(keyValues.map {case (k,v) => Array(k,  fmt(v))})).map(UnboxIntAsBoolean)

  def psetex[T](key: String, millis: Long, value: T)(implicit fmt: Format, ctx: ExecutionContext): Future[Boolean] =
    submitCommand("PSETEX", key, Array(key, millis.toString, fmt(value))).map(UnboxStatusAsBoolean)

  def set[T](key: String, value: T)(implicit fmt: Format, ctx: ExecutionContext): Future[Boolean] =
    submitCommand("SET", key, Array(key, fmt(value))).map(UnboxStatusAsBoolean)

  def setbit(key: String, offset: Long, value: Boolean)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("SETBIT", key, Array(key, offset.toString, if (value) "1" else "0")).map(UnboxIntAsBoolean)

  def setex[T](key: String, seconds: Long, value: T)(implicit fmt: Format, ctx: ExecutionContext): Future[Boolean] =
    submitCommand("SETEX", key, Array(key, seconds.toString, fmt(value))).map(UnboxStatusAsBoolean)

  def setnx[T](key: String, value: T)(implicit fmt: Format, ctx: ExecutionContext): Future[Boolean] =
    submitCommand("SETNX", key, Array(key, fmt(value))).map(UnboxIntAsBoolean)

  def setrange[T](key: String, offset: Long, value: T)(implicit fmt: Format, ctx: ExecutionContext): Future[Long] =
    submitCommand("SETRANGE", key, Array(key, offset.toString, fmt(value))).map(UnboxIntegral)

  def strlen(key: String)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("STRLEN", key).map(UnboxIntegral)
}

object BitOperation extends Enumeration {
  val AND = Value("AND")
  val OR = Value("OR")
  val XOR = Value("XOR")
  val NOT = Value("NOT")
}
