package com.redis

import akka.dispatch.Future
import protocol.Reply

trait RedisClient {

  protected def submitCommand(name: String, keys: Array[String], args: Array[String]): Future[Reply]

  protected def submitCommand(name: String, key: String, args: Array[String]): Future[Reply] =
    submitCommand(name, Array(key), args)

  protected def submitCommand(name: String, key: String, arg: String): Future[Reply] =
    submitCommand(name, Array(key), Array(arg))

  protected def submitCommand(name: String, keys: Array[String]): Future[Reply] =
    submitCommand(name, keys, keys)

  protected def submitCommand(name: String, key: String): Future[Reply] =
    submitCommand(name, Array(key))

}
