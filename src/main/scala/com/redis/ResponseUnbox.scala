package com.redis

import protocol.{IntegralReply, ErrorReply, Reply}

object ResponseUnbox {

  val UnboxError: PartialFunction[Reply, Nothing] = {
    case ErrorReply(msg) => throw new Exception("Error executing redis command: " + msg)
  }


  val UnboxInt: PartialFunction[Reply, Int] = UnboxError orElse {
    case IntegralReply(i) => i
  }



}
