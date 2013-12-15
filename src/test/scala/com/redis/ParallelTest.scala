package com.redis

import org.specs2.mutable.Specification
import java.util.concurrent.{Executors, TimeUnit, CyclicBarrier}
import com.redis.connection.RedisNodeClient
import io.netty.channel.nio.NioEventLoopGroup
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration.Duration

class ParallelTest extends Specification {

  "client" should {
    "handle parallel operations correctly" in {

      implicit val ctx = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))
      val client = RedisNodeClient(new NioEventLoopGroup(1), new ConnectionConfig())

      val nThreads = 4
      val nIterations=5000
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
          val res = Await.result(listFut, Duration(5, TimeUnit.MINUTES))
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
