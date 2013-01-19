package com.redis

import akka.dispatch.Future
import protocol.Reply

trait RedisClient {
  protected def submitCommand(name: String, keys: Iterable[String], args: Seq[String]): Future[Reply]
  protected def submitCommand(name: String, key: String, args: Seq[String]): Future[Reply] =
    submitCommand(name, key :: Nil, args)

  protected def submitCommand(name: String, key: String, arg: String): Future[Reply] =
    submitCommand(name, key :: Nil, arg :: Nil)
}
