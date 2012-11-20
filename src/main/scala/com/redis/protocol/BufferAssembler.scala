package com.redis.protocol

import org.jboss.netty.buffer.ChannelBuffer
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers._

object BufferAssembler {
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

  def apply(name: String): ChannelBuffer = requestBuffer(name, Nil)
  def apply(name: String, args: Seq[String]): ChannelBuffer = requestBuffer(name, args)
}
