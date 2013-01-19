package com.redis.operations

import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope
import akka.actor
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.Executors
import com.redis.{ConnectionConfig, RedisNodeClient}

class KeysOperationsSpec extends Specification {

  trait env extends Scope with After {
    implicit val actorSystem = actor.ActorSystem("test")

    val factory = new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool()
    )

    val client = new RedisNodeClient(factory, new ConnectionConfig())


    def after = {
      client.close()
      factory.releaseExternalResources()
    }
  }

}
