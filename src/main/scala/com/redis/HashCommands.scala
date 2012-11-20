package com.redis

import akka.dispatch.Future
import ResponseUnbox._

trait HashCommands {
  self: RedisClient =>

  def hdel(key: String, fields: String*): Future[Long] =
    submitCommand("HDEL", key, Seq(key) ++ fields).map(UnboxIntegral)

  def hexists(key: String, field: String): Future[Boolean] =
    submitCommand("HEXISTS", key, key :: field :: Nil).map(UnboxIntAsBoolean)

  def hget[T](key: String, field: String)(implicit parse: Parse[T]): Future[Option[T]] =
    submitCommand("HGET", key, key :: field :: Nil).map(UnboxBulk).map(_.map(parse))

  def hgetall[T](key: String)(implicit parse: Parse[T]): Future[Option[Map[String,T]]] =
    submitCommand("HGETALL", key, key :: Nil).map(multibulkAsPairMap[T])

  def hincrby(key: String, field: String, v: Long): Future[Long] =
    submitCommand("HINCRBY", key , key :: field :: v.toString :: Nil).map(UnboxIntegral)

  def hincrbyfloat(key: String, field: String, v: Double): Future[Double] =
    submitCommand("HINCRBYFLOAT", key , key :: field :: v.toString :: Nil).map(UnboxBulkAsDouble)

  def hkeys(key: String): Future[Option[Iterable[String]]] =
    submitCommand("HKEYS", key, key :: Nil).map(UnboxMultibulk andThen(_.map(_.flatten)))

  def hlen(key: String): Future[Long] =
    submitCommand("HLEN", key, key :: Nil).map(UnboxIntegral)

  def hmget[T](key: String, fields: String*)(implicit parse: Parse[T]): Future[Option[Map[String, T]]] =
    submitCommand("HMGET", key, Seq(key) ++ fields).map(multibulkAsMap(fields))

  def hmset[T](key: String, vals: Map[String,T])(implicit fmt: Format): Future[Boolean] =
    submitCommand("HMSET", key, Seq(key) ++ vals.map(kv => kv._1 :: fmt(kv._2) :: Nil).flatten).map(UnboxStatusAsBoolean)

  def hset[T](key: String, field: String, v: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("HSET", key, key :: field :: fmt(v) :: Nil).map(UnboxIntAsBoolean)

  def hsetnx[T](key: String, field: String, v: T)(implicit fmt: Format): Future[Boolean] =
    submitCommand("HSETNX", key, key :: field :: fmt(v) :: Nil).map(UnboxIntAsBoolean)

  def hvals[T](key: String)(implicit parse: Parse[T]): Future[Option[Iterable[T]]] =
    submitCommand("HVALS", key, key :: Nil).map(flattenMultibulk[T])



}
