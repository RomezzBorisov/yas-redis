package com.redis

import akka.dispatch.{Future, Promise, ExecutionContext}
import connection._
import connection.Connected
import connection.ReplyReceived
import java.util.concurrent.ConcurrentLinkedQueue
import protocol.{CommandEncoder, RedisResponseHandler, Reply}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.string.StringDecoder
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import socket.ClientSocketChannelFactory
import akka.actor.{Props, ActorSystem}

class RedisNodeClient(factory: ClientSocketChannelFactory, cfg: ConnectionConfig)(implicit executionContext: ExecutionContext, actorSystem: ActorSystem) extends RedisClient with RedisOperations {

  private val bootstrap = {
    val bs = new ClientBootstrap(factory)

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        Channels.pipeline(
          new DelimiterBasedFrameDecoder(cfg.maxLineLength, ChannelBuffers.copiedBuffer("\r\n", Charset.forName("UTF-8"))),
          new StringDecoder(Charset.forName("UTF-8")),
          new RedisResponseHandler(r => notifyActor(ReplyReceived(r))),
          //Downstream
          CommandEncoder)
      }
    })

    bs.setOption("remoteAddress", new InetSocketAddress(cfg.host, cfg.port))
    bs
  }

  private val actor = actorSystem.actorOf(Props(new ConnectionStateActor(bootstrap)))

  private def notifyActor(msg: ConnectionMessage) {
    actor ! msg
  }

  def submitCommand(name: String, keys: Iterable[String], args: Iterable[String]) = {
    val promise = Promise[Reply]
    notifyActor(CommandSendAttempt(RedisCommand(name, args.toSeq), promise))
    promise
  }

  def submitCommand(name: String, key: String, args: Iterable[String]) =
    submitCommand(name, Nil,args)
}

