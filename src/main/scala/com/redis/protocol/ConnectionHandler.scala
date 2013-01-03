package com.redis.protocol

import org.jboss.netty.channel._

class ConnectionHandler(successHandler: Channel => Unit, failureHandler: Throwable => Unit) extends SimpleChannelHandler {
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    println("Connection established")
    successHandler(e.getChannel)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    failureHandler(e.getCause)
    e.getChannel.disconnect()
  }


}
