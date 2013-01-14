package com.redis.connection

import org.specs2.mutable.Specification
import akka.testkit.{TestFSMRef, TestKit}
import akka.actor.ActorSystem
import org.specs2.specification.Scope
import org.specs2.mock.Mockito
import org.jboss.netty.bootstrap.ClientBootstrap
import ConnectionStateActor._
import org.jboss.netty.channel.Channel
import collection.immutable.Queue
import com.redis.protocol.{Reply, RedisCommand}
import akka.dispatch.Promise

class ConnectionStateActorSpec extends TestKit(ActorSystem("test")) with Specification with Mockito {

  trait env extends Scope {
    val bootstrap = mock[ClientBootstrap]
    val actorRef = TestFSMRef(new ConnectionStateActor(bootstrap))
  }

  "initial state" should {
    "be FastReconnecting" in new env {
      actorRef.stateName must_== FastReconnecting
      actorRef.stateData must_== FastReconnectingData(None, MessageAccumulator(0l))
    }
  }

  "FastReconnecting" should {
    "move to Processing when connected and no resubmits pending" in new env {
      val channel = mock[Channel]
      actorRef ! ConnectionEstablished(channel)
      actorRef.stateName must_== Processing
      actorRef.stateData must_== ProcessingData(channel, Queue.empty, Queue.empty, 0)
    }

    "move to ConnectedWaitingResubmits when connected in case of pending resubmits" in new env {
      val oldChannel = mock[Channel]
      val channel = mock[Channel]
      actorRef.setState(FastReconnecting, FastReconnectingData(Some(oldChannel), MessageAccumulator(1l)))
      actorRef ! ConnectionEstablished(channel)
      actorRef.stateName must_== ConnectedWaitingResubmits
      actorRef.stateData must_== ConnectedWaitingResubmitsData(channel, MessageAccumulator(1l))
    }

    "move to Connecting in case of ConnectionBroken and no resubmits pending and brokenChannel is different from the on in a message" in new env {
      val oldChannel = mock[Channel]
      val channel = mock[Channel]
      actorRef.setState(FastReconnecting, FastReconnectingData(Some(oldChannel), MessageAccumulator(0l)))
      actorRef ! ConnectionBroken(channel, new Exception)
      actorRef.stateName must_== Connecting
      actorRef.stateData must_== Nothing
      there was two(bootstrap).connect()

      actorRef.setState(FastReconnecting, FastReconnectingData(None, MessageAccumulator(0l)))
      actorRef ! ConnectionBroken(channel, new Exception)
      actorRef.stateName must_== Connecting
      actorRef.stateData must_== Nothing
      there were three(bootstrap).connect()
    }

    "move to ConnectionBrokenWaitingResubmits in case of ConnectionBroken and some resubmits pending and brokenChannel is different from the on in a message" in new env {
      val oldChannel = mock[Channel]
      val channel = mock[Channel]
      val ex = new Exception
      actorRef.setState(FastReconnecting, FastReconnectingData(Some(oldChannel), MessageAccumulator(1l)))
      actorRef ! ConnectionBroken(channel, ex)
      actorRef.stateName must_== ConnectionFailedWaitingResubmits
      actorRef.stateData must_== ConnectionFailedWaitingResubmitsData(ex, 1l)
      there were one(bootstrap).connect()

      actorRef.setState(FastReconnecting, FastReconnectingData(None, MessageAccumulator(1l)))
      actorRef ! ConnectionBroken(channel, ex)
      actorRef.stateName must_== ConnectionFailedWaitingResubmits
      actorRef.stateData must_== ConnectionFailedWaitingResubmitsData(ex, 1l)
      there were one(bootstrap).connect()

    }

    "stay and keep incoming resubmit in a temp queue" in new env {
      val oldChannel = mock[Channel]
      val channel = mock[Channel]
      val cmd = RedisCommand("DEL", "key" :: Nil)
      val promise = mock[Promise[Reply]]
      actorRef.setState(FastReconnecting, FastReconnectingData(Some(oldChannel), MessageAccumulator(1l)))
      actorRef ! ResubmitCommand(cmd, promise)
      actorRef.stateName must_== FastReconnecting
      actorRef.stateData must_== FastReconnectingData(Some(oldChannel), MessageAccumulator(Queue.empty, Queue(ResubmitCommand(cmd, promise)), 0l))

    }

    "stay and keep incoming submit in a temp queue" in new env {
      val oldChannel = mock[Channel]
      val channel = mock[Channel]
      val cmd = RedisCommand("DEL", "key" :: Nil)
      val promise = mock[Promise[Reply]]
      actorRef.setState(FastReconnecting, FastReconnectingData(Some(oldChannel), MessageAccumulator(1l)))
      actorRef ! SubmitCommand(cmd, promise)
      actorRef.stateName must_== FastReconnecting
      actorRef.stateData must_== FastReconnectingData(Some(oldChannel), MessageAccumulator(Queue(SubmitCommand(cmd, promise)), Queue.empty, 1l))
    }

    "ignore connect failures for a previously broken channel" in new env {
      val oldChannel = mock[Channel]
      actorRef.setState(FastReconnecting, FastReconnectingData(Some(oldChannel), MessageAccumulator(1l)))
      actorRef ! ConnectionBroken(oldChannel, new Exception)
      actorRef.stateName must_== FastReconnecting
      actorRef.stateData must_== FastReconnectingData(Some(oldChannel), MessageAccumulator(1l))
    }
  }

  "ConnectedWaitingResubmits" should {
    "put submits to temporary queue" in new env {
      val channel = mock[Channel]
      val cmd = RedisCommand("DEL", "key" :: Nil)
      val promise = mock[Promise[Reply]]
      actorRef.setState(ConnectedWaitingResubmits, ConnectedWaitingResubmitsData(channel, MessageAccumulator(1l)))
      actorRef ! SubmitCommand(cmd, promise)
      actorRef.stateName must_== ConnectedWaitingResubmits
      actorRef.stateData must_== ConnectedWaitingResubmitsData(channel, MessageAccumulator(Queue(SubmitCommand(cmd, promise)), Queue.empty, 1l))
    }

    "put resubmits in temprorary queue if not all resubmits got" in new env {
      val channel = mock[Channel]
      val cmd = RedisCommand("DEL", "key" :: Nil)
      val promise = mock[Promise[Reply]]
      actorRef.setState(ConnectedWaitingResubmits, ConnectedWaitingResubmitsData(channel, MessageAccumulator(2l)))
      actorRef ! ResubmitCommand(cmd, promise)
      actorRef.stateName must_== ConnectedWaitingResubmits
      actorRef.stateData must_== ConnectedWaitingResubmitsData(channel, MessageAccumulator(Queue.empty, Queue(ResubmitCommand(cmd, promise)), 1l))

    }

    "send resubmits then submits and move to Processing when the last resubmit received" in new env {

      val channel = mock[Channel]
      val cmd1 = RedisCommand("DEL", "key" :: Nil)
      val promise1 = mock[Promise[Reply]]
      val cmd2 = RedisCommand("DEL", "key1" :: Nil)
      val promise2 = mock[Promise[Reply]]
      val cmd3 = RedisCommand("DEL", "key2" :: Nil)
      val promise3 = mock[Promise[Reply]]

      actorRef.setState(ConnectedWaitingResubmits, ConnectedWaitingResubmitsData(channel, MessageAccumulator(Queue(SubmitCommand(cmd1, promise1)), Queue(ResubmitCommand(cmd2, promise2)), 1l)))
      actorRef ! ResubmitCommand(cmd3, promise3)

      actorRef.stateName must_== Processing
      actorRef.stateData must_== ProcessingData(channel, Queue(promise2, promise3, promise1), Queue.empty, 3)
    }

    "ignore disconnects" in new env {
      val channel = mock[Channel]
      val cmd1 = RedisCommand("DEL", "key" :: Nil)
      val promise1 = mock[Promise[Reply]]
      val cmd2 = RedisCommand("DEL", "key1" :: Nil)
      val promise2 = mock[Promise[Reply]]


      actorRef.setState(ConnectedWaitingResubmits, ConnectedWaitingResubmitsData(channel, MessageAccumulator(Queue(SubmitCommand(cmd1, promise1)), Queue(ResubmitCommand(cmd2, promise2)), 1l)))
      actorRef ! ConnectionBroken(channel, new Exception)

      actorRef.stateName must_== ConnectedWaitingResubmits
      actorRef.stateData must_== ConnectedWaitingResubmitsData(channel, MessageAccumulator(Queue(SubmitCommand(cmd1, promise1)), Queue(ResubmitCommand(cmd2, promise2)), 1l))

    }
  }

  "ConnectionBrokenWaitingResubmits" should {

    "reject submits immediately" in new env {
      val channel = mock[Channel]
      val cmd1 = RedisCommand("DEL", "key" :: Nil)
      val promise1 = mock[Promise[Reply]]
      val ex = new Exception()

      actorRef.setState(ConnectionFailedWaitingResubmits, ConnectionFailedWaitingResubmitsData(ex, 1l))
      actorRef ! SubmitCommand(cmd1, promise1)

      there was one(promise1).failure(ex)
      actorRef.stateName must_== ConnectionFailedWaitingResubmits
      actorRef.stateData must_== ConnectionFailedWaitingResubmitsData(ex, 1l)

    }

    "reject resubmits immediately and stay if more resubmits expected" in new env {
      val channel = mock[Channel]
      val cmd1 = RedisCommand("DEL", "key" :: Nil)
      val promise1 = mock[Promise[Reply]]
      val ex = new Exception()

      actorRef.setState(ConnectionFailedWaitingResubmits, ConnectionFailedWaitingResubmitsData(ex, 2l))

      actorRef ! ResubmitCommand(cmd1, promise1)
      there was one(promise1).failure(ex)
      actorRef.stateName must_== ConnectionFailedWaitingResubmits
      actorRef.stateData must_== ConnectionFailedWaitingResubmitsData(ex, 1l)

    }

    "reject the last expected resubmit and move to Connecting" in new env {
      val channel = mock[Channel]
      val cmd1 = RedisCommand("DEL", "key" :: Nil)
      val promise1 = mock[Promise[Reply]]
      val ex = new Exception()

      actorRef.setState(ConnectionFailedWaitingResubmits, ConnectionFailedWaitingResubmitsData(ex, 1l))

      there was one(bootstrap).connect()
      actorRef ! ResubmitCommand(cmd1, promise1)
      there was one(promise1).failure(ex)
      there were two(bootstrap).connect()

      actorRef.stateName must_== Connecting
      actorRef.stateData must_== Nothing
    }
  }

  "Connecting" should {
    "reject any submitted command" in new env {
      val channel = mock[Channel]
      val cmd1 = RedisCommand("DEL", "key" :: Nil)
      val promise1 = mock[Promise[Reply]]

      actorRef.setState(Connecting, Nothing)

      actorRef ! SubmitCommand(cmd1, promise1)
      there was one(promise1).failure(any[Throwable])

      actorRef.stateName must_== Connecting
      actorRef.stateData must_== Nothing


    }

    "move to processing when connected" in new env {
      val channel = mock[Channel]
      actorRef.setState(Connecting, Nothing)
      actorRef ! ConnectionEstablished(channel)

      actorRef.stateName must_== Processing
      actorRef.stateData must_== ProcessingData(channel, Queue.empty, Queue.empty, 0)
    }

    "stay and reschedule connect when failed to connect" in new env {
      val channel = mock[Channel]
      actorRef.setState(Connecting, Nothing)

      actorRef ! ConnectionBroken(channel, new Exception)

      actorRef.stateName must_== Connecting
      actorRef.stateData must_== Nothing
    }

  }

}
