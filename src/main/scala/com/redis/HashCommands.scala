package com.redis

import akka.dispatch.Future
import ResponseUnbox._

trait HashCommands {
  self: RedisClient =>

  def hdel(key: String, fields: String*): Future[Int] =
    submitCommand("HDEL", key::Nil, Seq(key) ++ fields).map(UnboxInt)

  def

}
