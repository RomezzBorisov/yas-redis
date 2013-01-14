package com.redis.protocol

import org.jboss.netty.channel._

class ConnectionHandler(successHandler: Channel => Unit, failureHandler: (Channel, Throwable) => Unit) extends SimpleChannelHandler {

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    successHandler(e.getChannel)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    failureHandler(e.getChannel, e.getCause)
    e.getChannel.disconnect()
  }


}
