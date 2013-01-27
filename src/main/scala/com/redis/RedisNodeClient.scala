package com.redis

import akka.dispatch.{Promise, ExecutionContext}
import connection._
import operations.{ResponseUnbox, StringArrayUtil}
import protocol._
import org.jboss.netty.bootstrap.{Bootstrap, ClientBootstrap}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.string.StringDecoder
import java.net.InetSocketAddress
import socket.ClientSocketChannelFactory
import akka.actor.{Props, ActorSystem}
import ConnectionStateActor._

class RedisNodeClient(factory: ClientSocketChannelFactory, cfg: ConnectionConfig)(implicit executionContext: ExecutionContext, actorSystem: ActorSystem) extends RedisClient with RedisDataOperations {

  private val actor = actorSystem.actorOf(Props(new ConnectionStateActor(bootstrap)))

  private lazy val bootstrap: ClientBootstrap = {
    val bs = new ClientBootstrap(factory)

    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        Channels.pipeline(
          new ConnectionHandler(ch => actor ! ConnectionEstablished(ch), (ch,ex) => actor ! ConnectionBroken(ch,ex)),
          new CommandSentEventHandler(() => actor ! CommandSent),
          new DelimiterBasedFrameDecoder(cfg.maxLineLength, ChannelBuffers.copiedBuffer("\r\n", Charset.forName("UTF-8"))),
          new StringDecoder(Charset.forName("UTF-8")),
          new RedisResponseHandler(r => actor ! ReplyReceived(r)),
          //Downstream
          CommandEncoder)
      }
    })

    bs.setOption("remoteAddress", new InetSocketAddress(cfg.host, cfg.port))
    bs
  }

  protected def submitCommand(name: String, keys: Array[String], args: Array[String]) = {
    val promise = Promise[Reply]
    actor ! SubmitCommand(RedisCommand(name, args), promise)
    promise
  }


  protected def instantError(ex: Exception) =
    Promise.failed(ex)

  def close() {
    submitCommand("QUIT", StringArrayUtil.EmptyStringArray, StringArrayUtil.EmptyStringArray).map(ResponseUnbox.UnboxStatusAsBoolean).onSuccess {
      case _ => actorSystem.stop(actor)
    }
  }
}

