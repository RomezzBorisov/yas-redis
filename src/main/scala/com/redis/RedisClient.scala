package com.redis

import akka.dispatch.Future
import protocol.Reply

trait RedisClient {
  def submitCommand(name: String, keys: Iterable[String], args: Iterable[String]): Future[Reply]
}
