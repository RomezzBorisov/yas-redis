package com.redis

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.handler.codec.string.StringDecoder
import java.nio.charset.Charset
import java.net.InetSocketAddress
import protocol.{OutgoingCommand, Reply, RedisResponseHandler}
import akka.dispatch.{Await, ExecutionContext, Future, Promise}
import akka.util.duration._

object RedisNettyClient extends App {




  private implicit val executionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))
  private val promiseQueue = new ConcurrentLinkedQueue[Promise[Reply]]()


  val factory = new NioClientSocketChannelFactory(
    Executors.newCachedThreadPool(),
    Executors.newCachedThreadPool()
  )

  val bootstrap = new ClientBootstrap(factory)

  /*val delimBuffer = ChannelBuffers.buffer(2)
  delimBuffer.writeBytes("\r\n".getBytes("UTF-8"))*/


  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    def getPipeline = {
      Channels.pipeline(
        new DelimiterBasedFrameDecoder(1024 * 1024, ChannelBuffers.copiedBuffer("\r\n", Charset.forName("UTF-8"))),
        new StringDecoder(Charset.forName("UTF-8")),
        new RedisResponseHandler(promiseQueue))
    }
  })

  val channel = bootstrap.connect(new InetSocketAddress("localhost", 6379)).getChannel

  def sendCommand[T](cmd: OutgoingCommand)(f: Reply => T): Future[T] = {
    val promise = Promise[Reply]
    promiseQueue.add(promise)
    channel.write(cmd.buffer)
    promise.map(f)
  }

  /**********************************************************/

  val pong = sendCommand(OutgoingCommand("PING"))(_.toString)
  val hincr = sendCommand(OutgoingCommand("HINCRBY", Iterable("a"), Seq("a", "b", "1") ))(_.toString)
  val hincr2 = sendCommand(OutgoingCommand("HINCRBY", Iterable("a"), Seq("a", "b", "2") ))(_.toString)

  val allFuts = Future.sequence(List(pong,hincr, hincr2)).onSuccess{
    case list => list.foreach(println)
  }

  Await.result(allFuts, 10 seconds)
  executionContext.shutdown()
  channel.close()


  //Thread.sleep(300000)


}
