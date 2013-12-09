package com.redis.operations

import com.redis.{Parse, RedisClient}
import ResponseUnbox._
import java.util.Date
import StringArrayUtil._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

trait KeysOperations {
  self: RedisClient =>

  def del(keys: String*)(implicit ctx: ExecutionContext): Future[Long] =
    failFast("DEL: illegal keys: " + keys, !keys.isEmpty)(submitCommand("DEL", keys.toArray)).map(UnboxIntegral)

  def exists(key: String)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("EXISTS", key).map(UnboxIntAsBoolean)

  def expire(key: String, dur: Duration)(implicit ctx: ExecutionContext): Future[Boolean] =
    expire(key, dur.toSeconds)

  def expire(key: String, seconds: Long)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("EXPIRE", key, Array(key, seconds.toString)).map(UnboxIntAsBoolean)

  def expireat(key: String, datetime: Date)(implicit ctx: ExecutionContext): Future[Boolean] =
    expireat(key, datetime.getTime)

  def expireat(key: String, timestampSeconds: Long)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("EXPIREAT", key, Array(key, timestampSeconds.toString)).map(UnboxIntAsBoolean)

  def persist(key: String)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("PERSIST", key).map(UnboxIntAsBoolean)

  def pexpire(key: String, dur: Duration)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("EXPIRE", key, Array(key , dur.toMillis.toString)).map(UnboxIntAsBoolean)

  def pexpireat(key: String, datetime: Date)(implicit ctx: ExecutionContext): Future[Boolean] =
    expireat(key, datetime.getTime)

  def pexpireat(key: String, timestamp: Long)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("EXPIREAT", key, Array(key , timestamp.toString )).map(UnboxIntAsBoolean)

  def pttl(key: String)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("PTTL", key).map(UnboxIntegral)

  def rename(from: String, to: String)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("RENAME", Array(from , to)).map(UnboxStatusAsBoolean)

  def renamenx(from: String, to: String)(implicit ctx: ExecutionContext): Future[Boolean] =
    submitCommand("RENAMENX", Array(from , to)).map(UnboxStatusAsBoolean)

  def sort[T](key: String, req: SortRequest)(implicit parse: Parse[T], ctx: ExecutionContext): Future[Option[Seq[T]]] =
    submitCommand("SORT", req.keys.toArray, req.args.toArray).map(UnboxMultibulkWithNonemptyParts.andThen(_.map(_.map(parse.apply).toSeq)))

  def ttl(key: String)(implicit ctx: ExecutionContext): Future[Long] =
    submitCommand("TTL", key).map(UnboxIntegral)

}

case class SortRequest(by: Option[String],
                       limit: Option[(Long, Long)],
                       get: Array[String],
                       order: OrderDirection.Value,
                       alpha: Boolean,
                       store: Option[String]) {

  def sortBy(pattern: String) = copy(by = Some(pattern))
  def limit(offset: Long, count: Long) = copy(limit = Some((offset, count)))
  def asc = copy(order = OrderDirection.ASC)
  def desc = copy(order = OrderDirection.DESC)
  def withAlpha = copy(alpha = true)
  def storeTo(dest: String) = copy(store = Some(dest))
  def get(patterns: String*): SortRequest = copy(get = patterns.toArray)

  def keys = toFlatArray(List(by.toArray, store.toArray, get))

  def args = toFlatArray(List(
    by.map(v => Array("BY", v)).getOrElse(EmptyStringArray),
    limit.map(v => Array("LIMIT", v._1.toString , v._2.toString)).getOrElse(EmptyStringArray),
    get.flatMap(v => Array("GET" , v)).toArray,
    Array(order.toString),
    if(alpha) Array("ALPHA") else EmptyStringArray,
    store.map(v => Array("STORE" , v)).getOrElse(EmptyStringArray)
  ))
}

object SortRequest {
  val Default = SortRequest(None, None, EmptyStringArray, OrderDirection.ASC, alpha = false, None)
}
