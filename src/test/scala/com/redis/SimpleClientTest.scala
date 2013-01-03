package com.redis

import org.specs2.mutable.Specification
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.Executors
import akka.dispatch.{Future, Await}
import akka.util.duration._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor

class SimpleClientTest extends  Specification {

  "client" should {
    "work" in {
      val myConfig =  ConfigFactory.parseString(
        """akka.actor.default-dispatcher.executor=thread-pool-executor
        """.stripMargin)
      val regularConfig =        ConfigFactory.load()
      val combined = myConfig.withFallback(regularConfig)
      val complete =  ConfigFactory.load(combined)

      implicit val actorSystem = actor.ActorSystem("test",complete)

      val factory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()
      )

      val client = new RedisNodeClient(factory, new ConnectionConfig())

      val nIterations = 10
      val nFutures = 300000

      for (i <- 1 to nIterations) {
        val start = System.currentTimeMillis()
        val futures = for (j <- 1 to nFutures) yield {

          client.hincrby(i.toString,j.toString,j)
        }
        Await.result(Future.sequence(futures), intToDurationInt(10).minutes)
        val end = System.currentTimeMillis()
        println("Processing " + nFutures +" commands in " + (end - start))
      }
    }
  }

}
