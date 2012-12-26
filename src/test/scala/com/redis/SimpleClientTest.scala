package com.redis

import org.specs2.mutable.Specification
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.Executors
import akka.dispatch.{Future, Await}
import akka.util.duration._
import akka.actor.ActorSystem

class SimpleClientTest extends  Specification {

  "client" should {
    "work" in {
      implicit val actorSystem = ActorSystem("test")

      val factory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()
      )

      val client = new RedisNodeClient(factory, new ConnectionConfig())

      for (i <- 1 to 10) {
        val start = System.currentTimeMillis()
        val futures = for (j <- 1 to 300000) yield {
          client.hincrby(i.toString,j.toString,j)
        }
        Await.result(Future.sequence(futures), intToDurationInt(10).minutes)
        val end = System.currentTimeMillis()
        println("Processing 30000 commands in " + (end - start))
      }
    }
  }

}
