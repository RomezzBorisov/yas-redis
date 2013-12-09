package com.redis.protocol

import java.nio.charset.Charset
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.buffer.Unpooled._
import scala.io.Source
import io.netty.channel.ChannelHandlerContext
import java.util
import io.netty.channel.ChannelHandler.Sharable

@Sharable
object CommandEncoder extends MessageToMessageEncoder[RedisCommand] {
  val UTF8 = Charset.forName("UTF-8")
  private def CRLF = copiedBuffer("\r\n", UTF8)
  private def DOLLAR = copiedBuffer("$", UTF8)
  private def STAR = copiedBuffer("*", UTF8)

  private def strBuffer(o: Any) = copiedBuffer(o.toString, UTF8)
  private def lineBuffer(s: String)  = wrappedBuffer(DOLLAR, intToBuffer(s.length), CRLF, strBuffer(s), CRLF)

  private val IntBufferCache = (0 to 1024).map(i => strBuffer(i)).toArray
  private def intToBuffer(i: Int) = //if (i >= 0 && i <= 1024) IntBufferCache(i) else
     strBuffer(i)

  private val ArgsSizeBufferCache = (1 to 1024).map { i =>
    wrappedBuffer(STAR, intToBuffer(i), CRLF)
  }.toArray
  private def argsSizeToBuffer(n: Int) = //if(n <= 1024) ArgsSizeBufferCache(n - 1) else
    wrappedBuffer(STAR, intToBuffer(n), CRLF)

  private val CommandNameBufferCache = Source.fromInputStream(getClass.getResourceAsStream("/commands.txt")).getLines().map { cmd =>
    cmd -> lineBuffer(cmd)
  }.toMap
  private def commandNameToBuffer(cmd: String) = //CommandNameBufferCache.getOrElse(cmd, lineBuffer(cmd))
    lineBuffer(cmd)


  private def requestBuffer(name: String, args: Seq[String]) = wrappedBuffer(
    argsSizeToBuffer(args.size + 1),
    commandNameToBuffer(name),
    wrappedBuffer(args.map(lineBuffer): _*)
  )


  def encode(ctx: ChannelHandlerContext, msg: RedisCommand, out: util.List[AnyRef]) {
    out.add(requestBuffer(msg.name, msg.args))
  }
}

