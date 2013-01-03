package com.redis

import org.specs2.mutable.Specification
import akka.actor
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.{CyclicBarrier, Executors}
import akka.dispatch.{Await, Future}
import akka.util.duration._

class ParallelTest extends Specification {

  "client" should {
    "handle parallel operations correctly" in {
      implicit val actorSystem = actor.ActorSystem("test")

      val factory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()
      )

      val client = new RedisNodeClient(factory, new ConnectionConfig())

      val nThreads = 16
      val nIterations=1500
      val startBarrier = new CyclicBarrier(nThreads)

      val expected = (1 to nIterations).foldLeft(List.empty[Long]) {
        case (list, i) => (i + list.headOption.getOrElse(0l)) :: list
      }.reverse

      class TestThread(id: Int) extends Thread {
        override def run() {
          startBarrier.await()
          val futures = for (i <- 1 to nIterations) yield {
            client.hincrby("parallel-key",id.toString, i)
          }

          val listFut = Future.sequence(futures)
          val res = Await.result(listFut, intToDurationInt(5).minutes)
          res.toList must_== expected
        }
      }

      val threads = (1 to nThreads).map(new TestThread(_))
      client.del("parallel-key")
      threads.foreach(_.start())
      threads.foreach(_.join())
      success
    }
  }

}
