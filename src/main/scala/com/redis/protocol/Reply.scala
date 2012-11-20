package com.redis.protocol

sealed trait Reply

case class IntegralReply(res: Long) extends Reply

case class ErrorReply(line: String) extends Reply

case class BulkReply(msg: String) extends Reply

case object EmptyBulkReply extends Reply

case class MultibulkReply(replies: Array[Reply]) extends Reply

case class SingleLineReply(line: String) extends Reply
