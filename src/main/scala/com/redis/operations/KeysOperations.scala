package com.redis.operations

import akka.dispatch.Future
import com.redis.RedisClient
import ResponseUnbox._

trait KeysOperations {
  self: RedisClient =>

  def del(keys: String*): Future[Long] =
    submitCommand("DEL", keys, keys).map(UnboxIntegral)

}
