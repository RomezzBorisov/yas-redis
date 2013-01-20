package com.redis.protocol

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.buffer.ChannelBuffers
import io.Source


object CommandEncoder extends OneToOneEncoder {
  private val UTF8 = Charset.forName("UTF-8")
  private val CRLF = copiedBuffer("\r\n", UTF8).toByteBuffer
  private val DOLLAR = copiedBuffer("$", UTF8).toByteBuffer
  private val STAR = copiedBuffer("*", UTF8).toByteBuffer

  private def strBuffer(o: Any) = copiedBuffer(o.toString, UTF8).toByteBuffer
  private def lineBuffer(s: String)  = wrappedBuffer(DOLLAR, intToBuffer(s.length), CRLF, strBuffer(s), CRLF).toByteBuffer

  private val IntBufferCache = (0 to 1024).map(i => strBuffer(i)).toArray
  private def intToBuffer(i: Int) = if (i >= 0 && i <= 1024) IntBufferCache(i) else strBuffer(i)

  private val ArgsSizeBufferCache = (1 to 1024).map { i =>
    wrappedBuffer(STAR, intToBuffer(i), CRLF).toByteBuffer
  }.toArray
  private def argsSizeToBuffer(n: Int) = if(n <= 1024) ArgsSizeBufferCache(n - 1) else wrappedBuffer(STAR, intToBuffer(n), CRLF).toByteBuffer

  private val CommandNameBufferCache = Source.fromInputStream(getClass.getResourceAsStream("/commands.txt")).getLines().map { cmd =>
    cmd -> lineBuffer(cmd)
  }.toMap
  private def commandNameToBuffer(cmd: String) = CommandNameBufferCache.getOrElse(cmd, lineBuffer(cmd))



  private def requestBuffer(name: String, args: Seq[String]) = wrappedBuffer(
    argsSizeToBuffer(args.size + 1),
    commandNameToBuffer(name),
    wrappedBuffer(args.map(lineBuffer): _*).toByteBuffer
  )

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = msg match {
    case RedisCommand(name, args) => requestBuffer(name, args)
    case _ => ChannelBuffers.EMPTY_BUFFER
  }
}

