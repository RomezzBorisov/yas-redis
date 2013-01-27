package com.redis.operations

import akka.dispatch.Future
import ResponseUnbox._
import com.redis.{Format, Parse, RedisClient}

import StringArrayUtil._

trait HashOperations {
  self: RedisClient =>

  def hdel(key: String, fields: String*): Future[Long] =
    submitCommand("HDEL", key, toArgsArray(key, fields)).map(UnboxIntegral)

  def hexists(key: String, field: String): Future[Boolean] =
    submitCommand("HEXISTS", key, Array(key, field)).map(UnboxIntAsBoolean)

  def hget[T](key: String, field: String)(implicit parse: Parse[T]): Future[Option[T]] =
    submitCommand("HGET", key, Array(key, field)).map(UnboxBulk).map(_.map(parse))

  def hgetall[T](key: String)(implicit parse: Parse[T]): Future[Option[Map[String,T]]] =
    submitCommand("HGETALL", key, key).map(multibulkAsPairMap[T])

  def hincrby(key: String, field: String, v: Long): Future[Long] =
    submitCommand("HINCRBY", key , Array(key , field ,v.toString)).map(UnboxIntegral)

  def hincrbyfloat(key: String, field: String, v: Double): Future[Double] =
    submitCommand("HINCRBYFLOAT", key , Array(key , field , v.toString)).map(UnboxBulkAsDouble)

  def hkeys(key: String): Future[Option[Iterable[String]]] =
    submitCommand("HKEYS", key, key).map(UnboxMultibulkWithNonemptyParts)

  def hlen(key: String): Future[Long] =
    submitCommand("HLEN", key, key).map(UnboxIntegral)

  def hmget[T](key: String, fields: String*)(implicit parse: Parse[T]): Future[Option[Map[String, T]]] =
    submitCommand("HMGET", key, toArgsArray(key, fields)).map(multibulkAsMap(fields))

  def hmset[T](key: String, vals: Map[String,T])(implicit fmt: Format): Future[Boolean] =
    submitCommand("HMSET", key, toArgsArray(key, vals.map(kv => Iterable(kv._1 , fmt(kv._2))).flatten.toIterable)).map(UnboxStatusAsBoolean)

  def hset[T](key: String, field: String, v: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("HSET", key, Array(key, field ,fmt(v))).map(UnboxIntAsBoolean)

  def hsetnx[T](key: String, field: String, v: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("HSETNX", key, Array(key ,field, fmt(v))).map(UnboxIntAsBoolean)

  def hvals[T](key: String)(implicit parse: Parse[T]): Future[Option[Iterable[T]]] =
    submitCommand("HVALS", key, key).map(flattenMultibulk[T])



}
