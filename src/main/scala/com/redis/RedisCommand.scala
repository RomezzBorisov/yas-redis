package com.redis

import protocol.Reply
import akka.dispatch.Promise

case class RedisCommand(name: String, args: Seq[String])
