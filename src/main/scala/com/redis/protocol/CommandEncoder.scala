package com.redis.protocol

import java.nio.charset.Charset
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.buffer.Unpooled._
import scala.io.Source
import io.netty.channel.ChannelHandlerContext
import java.util
import io.netty.channel.ChannelHandler.Sharable
import io.netty.buffer.ByteBuf

@Sharable
object CommandEncoder extends MessageToMessageEncoder[RedisCommand] {
  val UTF8 = Charset.forName("UTF-8")

  private def extractBytes(buf: ByteBuf) = {
    val n = buf.readableBytes()
    val res = new Array[Byte](n)
    buf.getBytes(buf.readerIndex(), res)
    res
  }

  private val CRLF_BYTES = extractBytes(copiedBuffer("\r\n", UTF8))
  private def CRLF = wrappedBuffer(CRLF_BYTES)

  private val DOLLAR_BYTES = extractBytes(copiedBuffer("$", UTF8))
  private def DOLLAR = wrappedBuffer(DOLLAR_BYTES)

  private val STAR_BYTES = extractBytes(copiedBuffer("*", UTF8))
  private def STAR = wrappedBuffer(STAR_BYTES)

  private def strBuffer(o: Any) = copiedBuffer(o.toString, UTF8)
  private def lineBuffer(s: String)  = wrappedBuffer(DOLLAR, intToBuffer(s.length), CRLF, strBuffer(s), CRLF)

  private val IntBufferCache = (0 to 1024).map(i => extractBytes(strBuffer(i))).toArray
  private def intToBuffer(i: Int) = if (i >= 0 && i <= 1024) {
    wrappedBuffer(IntBufferCache(i))
  } else {
    strBuffer(i)
  }



  private val ArgsSizeBufferCache = (1 to 1024).map { i =>
    extractBytes(wrappedBuffer(STAR, intToBuffer(i), CRLF))
  }.toArray
  private def argsSizeToBuffer(n: Int) = if(n <= 1024) {
    wrappedBuffer(ArgsSizeBufferCache(n - 1))
  } else {
    wrappedBuffer(STAR, intToBuffer(n), CRLF)
  }


  private val CommandNameBufferCache = Source.fromInputStream(getClass.getResourceAsStream("/commands.txt")).getLines().map { cmd =>
    cmd -> extractBytes(lineBuffer(cmd))
  }.toMap

  private def commandNameToBuffer(cmd: String) = CommandNameBufferCache.get(cmd).map(wrappedBuffer).getOrElse(lineBuffer(cmd))

  private def requestBuffer(name: String, args: Seq[String]) = wrappedBuffer(
    argsSizeToBuffer(args.size + 1),
    commandNameToBuffer(name),
    wrappedBuffer(args.map(lineBuffer): _*)
  )

  def encode(ctx: ChannelHandlerContext, msg: RedisCommand, out: util.List[AnyRef]) {
    out.add(requestBuffer(msg.name, msg.args))
  }
}

