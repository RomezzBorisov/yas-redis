package com.redis

import akka.dispatch.{Promise, ExecutionContext}
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import protocol.{BufferAssembler, RedisResponseHandler, Reply}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.string.StringDecoder
import java.net.InetSocketAddress

class RedisNodeClient(config: ConnectionConfig)(implicit executionContext: ExecutionContext) extends RedisClient {

  private val promiseQueue = new ConcurrentLinkedQueue[Promise[Reply]]()

  private val factory = new NioClientSocketChannelFactory(
    config.bossExecutor,
    config.workerExecutor,
    config.bossCount,
    config.workerCount
  )

  private val bootstrap = new ClientBootstrap(factory)

  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    def getPipeline = {
      Channels.pipeline(
        new DelimiterBasedFrameDecoder(config.maxLineLength, ChannelBuffers.copiedBuffer("\r\n", Charset.forName("UTF-8"))),
        new StringDecoder(Charset.forName("UTF-8")),
        new RedisResponseHandler(promiseQueue))
    }
  })

  val channel = bootstrap.connect(new InetSocketAddress(config.host, config.port)).getChannel


  def submitCommand(name: String, key: String, args: Iterable[String]) =
    submitCommand(name, Nil, args)

  def submitCommand(name: String, keys: Iterable[String], args: Iterable[String]) = {
    val buf = BufferAssembler(name, args.toSeq)
    val promise = Promise[Reply]
    channel.synchronized {
      promiseQueue.add(promise)
      channel.write(buf)
    }
    promise
  }
}
