package com.redis.operations

import akka.dispatch.Future
import com.redis.{Parse, RedisClient}
import ResponseUnbox._
import akka.util.Duration
import java.util.Date

trait KeysOperations {
  self: RedisClient =>

  def del(keys: String*): Future[Long] =
    submitCommand("DEL", keys.toArray).map(UnboxIntegral)

  def exists(key: String): Future[Boolean] =
    submitCommand("EXISTS", key).map(UnboxIntAsBoolean)

  def expire(key: String, dur: Duration): Future[Boolean] =
    submitCommand("EXPIRE", key, Array(key, dur.toSeconds.toString)).map(UnboxIntAsBoolean)

  def expireAt(key: String, datetime: Date): Future[Boolean] =
    expireAt(key, datetime.getTime)

  def expireAt(key: String, timestampSeconds: Long): Future[Boolean] =
    submitCommand("EXPIREAT", key, Array(key, timestampSeconds.toString)).map(UnboxIntAsBoolean)

  def persist(key: String): Future[Boolean] =
    submitCommand("PERSIST", key).map(UnboxIntAsBoolean)

  def pexpire(key: String, dur: Duration): Future[Boolean] =
    submitCommand("EXPIRE", key, Array(key , dur.toMillis.toString)).map(UnboxIntAsBoolean)

  def pexpireAt(key: String, datetime: Date): Future[Boolean] =
    expireAt(key, datetime.getTime)

  def pexpireAt(key: String, timestamp: Long): Future[Boolean] =
    submitCommand("EXPIREAT", key, Array(key , timestamp.toString )).map(UnboxIntAsBoolean)

  def pttl(key: String): Future[Long] =
    submitCommand("PTTL", key).map(UnboxIntegral)

  def rename(from: String, to: String): Future[Boolean] =
    submitCommand("RENAME", Array(from , to)).map(UnboxStatusAsBoolean)

  def renamenx(from: String, to: String): Future[Boolean] =
    submitCommand("RENAMENX", Array(from , to)).map(UnboxStatusAsBoolean)

  def sort[T](key: String, req: SortRequest)(implicit parse: Parse[T]): Future[Option[Seq[T]]] =
    submitCommand("SORT", req.keys.toArray, req.args.toArray).map(UnboxMultibulkWithNonemptyParts.andThen(_.map(_.map(parse.apply).toSeq)))

  def ttl(key: String): Future[Long] =
    submitCommand("TTL", key).map(UnboxIntegral)

}

case class SortRequest(by: Option[String],
                       limit: Option[(Long, Long)],
                       get: Seq[String],
                       order: OrderDirection.Value,
                       alpha: Boolean,
                       store: Option[String]) {

  def sortBy(pattern: String) = copy(by = Some(pattern))
  def limit(offset: Long, count: Long) = copy(limit = Some((offset, count)))
  def asc = copy(order = OrderDirection.ASC)
  def desc = copy(order = OrderDirection.DESC)
  def withAlpha = copy(alpha = true)
  def storeTo(dest: String) = copy(store = Some(dest))
  def get(patterns: String*): SortRequest = copy(get = patterns)

  def keys = List(by.toIterable, store.toIterable, get).flatten.toSeq

  def args = List(
    by.map(v => "BY" :: v :: Nil).getOrElse(Nil),
    limit.map(v => "LIMIT" :: v._1.toString :: v._2.toString :: Nil).getOrElse(Nil),
    get.flatMap(v => "GET" :: v :: Nil),
    order.toString :: Nil,
    if(alpha) ("ALPHA" :: Nil) else Nil,
    store.map(v => "STORE" :: v :: Nil).getOrElse(Nil)
  ).flatten.toSeq
}

object SortRequest {
  val Default = SortRequest(None, None, Seq.empty, OrderDirection.ASC, alpha = false, None)
}
