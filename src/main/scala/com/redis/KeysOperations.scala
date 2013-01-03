package com.redis

import akka.dispatch.Future
import com.redis.ResponseUnbox._

trait KeysOperations {
  self: RedisClient =>

  def del(keys: String*): Future[Long] =
    submitCommand("DEL", keys, keys).map(UnboxIntegral)


}
