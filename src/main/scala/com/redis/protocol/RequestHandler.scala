package com.redis.protocol

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import java.nio.charset.Charset

class RequestHandler extends SimpleChannelDownstreamHandler {



  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case OutgoingCommand(_, _, buffer) =>
        ctx.sendDownstream(new DownstreamMessageEvent(e.getChannel, e.getFuture, buffer, e.getRemoteAddress))


      case other => throw new IllegalArgumentException("Unsupported outgoing message: " + other)
    }
  }
}
