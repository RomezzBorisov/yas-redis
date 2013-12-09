package com.redis.operations

import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope
import java.util.concurrent.{Executors, TimeUnit}
import com.redis.ConnectionConfig
import com.redis.connection.RedisNodeClient
import io.netty.channel.nio.NioEventLoopGroup
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration.Duration

trait RedisEnv {
  self: Specification =>

  trait env extends Scope with After {
    implicit val ctx = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))
    val eventGroup = new NioEventLoopGroup(1)

    val client = RedisNodeClient(eventGroup, new ConnectionConfig())

    def result[T](fut: Future[T]): T = {
      Await.result(fut, Duration(5, TimeUnit.SECONDS))
    }


    def after = {
      client.close()
      eventGroup.shutdownGracefully().sync()
    }
  }

}
