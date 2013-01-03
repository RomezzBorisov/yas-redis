package com.redis.protocol

case class RedisCommand(name: String, args: Seq[String])
