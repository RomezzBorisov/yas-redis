package com.redis.protocol

import org.jboss.netty.channel.{WriteCompletionEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}


class CommandSentEventHandler(f: () => Unit) extends SimpleChannelUpstreamHandler {
  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
    f()
  }
}
