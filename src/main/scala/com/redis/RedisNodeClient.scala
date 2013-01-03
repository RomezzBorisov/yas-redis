package com.redis

import akka.dispatch.{Promise, ExecutionContext}
import connection._
import protocol._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.string.StringDecoder
import java.net.InetSocketAddress
import socket.ClientSocketChannelFactory
import akka.actor.{Props, ActorSystem}
import ConnectionStateActor._

class RedisNodeClient(factory: ClientSocketChannelFactory, cfg: ConnectionConfig)(implicit executionContext: ExecutionContext, actorSystem: ActorSystem) extends RedisClient with RedisOperations {

  private val bootstrap = {
    val bs = new ClientBootstrap(factory)

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        Channels.pipeline(
          new ConnectionHandler(ch => notifyActor(ConnectionEstablished(ch)), ex => notifyActor(ConnectionBroken(ex))),
          new CommandSentEventHandler(() => notifyActor(CommandSent)),
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

  protected def submitCommand(name: String, keys: Iterable[String], args: Seq[String]) = {
    val promise = Promise[Reply]
    notifyActor(SubmitCommand(RedisCommand(name, args.toSeq), promise))
    promise
  }
}

