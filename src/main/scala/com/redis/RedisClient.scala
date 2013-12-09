package com.redis

import protocol.Reply
import scala.concurrent.{ExecutionContext, Future}

trait RedisClient {

  protected def submitCommand(name: String, keys: Array[String], args: Array[String])(implicit ctx: ExecutionContext): Future[Reply]

  protected def submitCommand(name: String, key: String, args: Array[String])(implicit ctx: ExecutionContext): Future[Reply] =
    submitCommand(name, Array(key), args)

  protected def submitCommand(name: String, key: String, arg: String)(implicit ctx: ExecutionContext): Future[Reply] =
    submitCommand(name, Array(key), Array(arg))

  protected def submitCommand(name: String, keys: Array[String])(implicit ctx: ExecutionContext): Future[Reply] =
    submitCommand(name, keys, keys)

  protected def submitCommand(name: String, key: String)(implicit ctx: ExecutionContext): Future[Reply] =
    submitCommand(name, Array(key))

  protected def failFast(errMsg: => String, precondition: Boolean)(f: => Future[Reply])(implicit ctx: ExecutionContext): Future[Reply] = {
    if (!precondition) {
      instantError(new IllegalArgumentException(errMsg))
    } else {
      f
    }
  }

  protected def instantError(ex: Exception)(implicit ctx: ExecutionContext): Future[Reply] = Future.failed(ex)

}
