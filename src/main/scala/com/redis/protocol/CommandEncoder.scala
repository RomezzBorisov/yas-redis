package com.redis.protocol

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.buffer.ChannelBuffers


object CommandEncoder extends OneToOneEncoder {
  private val UTF8 = Charset.forName("UTF-8")
  private val CRLF = copiedBuffer("\r\n", UTF8)
  private val DOLLAR = copiedBuffer("$", UTF8)
  private val STAR = copiedBuffer("*", UTF8)

  private def strBuffer(o: Any) = copiedBuffer(o.toString, UTF8)
  private def lineBuffer(s: String) = wrappedBuffer(DOLLAR, strBuffer(s.length), CRLF, strBuffer(s), CRLF)

  private def requestBuffer(name: String, args: Seq[String]) = wrappedBuffer(
    wrappedBuffer(STAR, strBuffer(args.size + 1), CRLF),
    lineBuffer(name),
    wrappedBuffer(args.map(lineBuffer): _*)
  )

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = msg match {
    case RedisCommand(name, args) => requestBuffer(name, args)
    case _ => ChannelBuffers.EMPTY_BUFFER
  }
}

