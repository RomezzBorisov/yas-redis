package com.redis.connection

import akka.actor.{LoggingFSM, Actor}
import org.jboss.netty.channel.Channel
import com.redis.protocol.{RedisCommand, Reply}
import org.jboss.netty.bootstrap.ClientBootstrap
import akka.dispatch.Promise
import collection.immutable.Queue
import akka.util.duration._
import com.redis.operations.StringArrayUtil


object ConnectionStateActor {

  def safeWrite(cmd: RedisCommand, replyHandler: Promise[Reply], ch: Channel) = try {
    ch.write(cmd)
    true
  } catch {
    case e: Exception =>
      replyHandler.failure(e)
      false
  }


  sealed trait ConnectionMessage

  //send
  case class SubmitCommand(cmd: RedisCommand, replyHandler: Promise[Reply]) extends ConnectionMessage

  case class ResubmitCommand(cmd: RedisCommand, replyHandler: Promise[Reply]) extends ConnectionMessage

  case object CommandSent extends ConnectionMessage

  //receive
  case class ReplyReceived(reply: Reply) extends ConnectionMessage

  //connection
  case object ConnectAttempt extends ConnectionMessage

  case class ConnectionEstablished(channel: Channel) extends ConnectionMessage

  case class ConnectionBroken(channel: Channel,ex: Throwable) extends ConnectionMessage

  sealed trait ConnectionState

  case object FastReconnecting extends ConnectionState

  case object ConnectedWaitingResubmits extends ConnectionState

  case object ConnectionFailedWaitingResubmits extends ConnectionState

  case object Processing extends ConnectionState

  case object Connecting extends ConnectionState

  sealed trait StateData
  trait ChannelHolder {
    def channel: Channel
  }

  case class MessageAccumulator(pendingSubmits: Queue[SubmitCommand],
                                pendingResubmits: Queue[ResubmitCommand],
                                resubmitsRemaining: Long) {

    def expectsMoreResubmits = resubmitsRemaining > 0

    def submitted(cmd: SubmitCommand) = copy(pendingSubmits = pendingSubmits.enqueue(cmd))

    def resubmitted(cmd: ResubmitCommand) = copy(pendingResubmits = pendingResubmits.enqueue(cmd), resubmitsRemaining = resubmitsRemaining - 1)

    def sendAll(ch: Channel): Queue[Promise[Reply]] = {
      val queue = pendingResubmits.foldLeft(Queue.empty[Promise[Reply]]) {
        case (q, cmd) =>
          if (safeWrite(cmd.cmd, cmd.replyHandler, ch))
            q.enqueue(cmd.replyHandler)
          else
            q
      }
      pendingSubmits.foldLeft(queue) {
        case (q, cmd) =>
          if (safeWrite(cmd.cmd, cmd.replyHandler, ch))
            q.enqueue(cmd.replyHandler)
          else
            q
      }
    }

    def failAll(ex: Throwable) {
      pendingResubmits.foreach(_.replyHandler.failure(ex))
      pendingSubmits.foreach(_.replyHandler.failure(ex))
    }

    def size = pendingResubmits.size + pendingSubmits.size

  }

  object MessageAccumulator {
    def apply(expectedResubmits: Long): MessageAccumulator = MessageAccumulator(Queue.empty, Queue.empty, expectedResubmits)
  }

  case class FastReconnectingData(brokenChannelOpt: Option[Channel], acc: MessageAccumulator) extends StateData

  case class ConnectedWaitingResubmitsData(channel: Channel, acc: MessageAccumulator) extends StateData with ChannelHolder

  case class ConnectionFailedWaitingResubmitsData(ex: Throwable, resubmitsRemaining: Long) extends StateData

  case class ProcessingData(channel: Channel, pendingReplies: Queue[Promise[Reply]], resubmits: Queue[ResubmitCommand], nPendingSends: Long) extends StateData with ChannelHolder

  case object Nothing extends StateData


}

import ConnectionStateActor._

class ConnectionStateActor(bootstrap: ClientBootstrap) extends Actor with LoggingFSM[ConnectionState, StateData] {

  startWith(FastReconnecting, FastReconnectingData(None, MessageAccumulator(0l)))

  when(FastReconnecting) {
    case Event(cmd: SubmitCommand, FastReconnectingData(chOpt, acc)) =>
      stay() using FastReconnectingData(chOpt, acc.submitted(cmd))

    case Event(cmd: ResubmitCommand, FastReconnectingData(chOpt, acc)) =>
      stay() using FastReconnectingData(chOpt, acc.resubmitted(cmd))

    case Event(ConnectionEstablished(channel), FastReconnectingData(_, acc)) if acc.expectsMoreResubmits =>
      goto(ConnectedWaitingResubmits) using ConnectedWaitingResubmitsData(channel, acc)

    case Event(ConnectionEstablished(channel), FastReconnectingData(_, acc)) =>
      goto(Processing) using ProcessingData(channel, acc.sendAll(channel), Queue.empty, acc.size)

    case Event(ConnectionBroken(ch, ex), FastReconnectingData(chOpt, acc)) if chOpt.forall(_ != ch) && acc.expectsMoreResubmits =>
      acc.failAll(ex)
      goto(ConnectionFailedWaitingResubmits) using ConnectionFailedWaitingResubmitsData(ex, acc.resubmitsRemaining)

    case Event(ConnectionBroken(ch, ex), FastReconnectingData(chOpt, acc)) if chOpt.forall(_ != ch) =>
      acc.failAll(ex)
      bootstrap.connect()
      goto(Connecting) using Nothing
  }

  when(ConnectedWaitingResubmits) {
    case Event(cmd: SubmitCommand, ConnectedWaitingResubmitsData(ch, acc)) =>
      stay() using ConnectedWaitingResubmitsData(ch, acc.submitted(cmd))

    case Event(cmd: ResubmitCommand, ConnectedWaitingResubmitsData(ch, acc)) =>
      val newAcc = acc.resubmitted(cmd)
      if (newAcc.expectsMoreResubmits)
        stay() using ConnectedWaitingResubmitsData(ch, newAcc)
      else
        goto(Processing) using ProcessingData(ch, newAcc.sendAll(ch), Queue.empty, newAcc.size)
  }

  when(ConnectionFailedWaitingResubmits) {
    case Event(cmd: SubmitCommand, ConnectionFailedWaitingResubmitsData(ex, _)) =>
      cmd.replyHandler.failure(ex)
      stay()

    case Event(cmd: ResubmitCommand, ConnectionFailedWaitingResubmitsData(ex, 1)) =>
      cmd.replyHandler.failure(ex)
      bootstrap.connect()
      goto(Connecting) using Nothing

    case Event(cmd: ResubmitCommand, ConnectionFailedWaitingResubmitsData(ex, n)) =>
      cmd.replyHandler.failure(ex)
      stay() using ConnectionFailedWaitingResubmitsData(ex, n - 1)
  }

  when(Processing) {
    case Event(SubmitCommand(cmd, replyPromise), data@ProcessingData(ch, pendingReplies, resubmits, n)) =>
      if (safeWrite(cmd, replyPromise, ch))
        stay() using ProcessingData(ch, pendingReplies.enqueue(replyPromise), resubmits, n + 1)
      else
        stay() using data

    case Event(cmd: ResubmitCommand, ProcessingData(ch, pendingReplies, resubmits, n)) =>
      stay() using ProcessingData(ch, pendingReplies, resubmits.enqueue(cmd), n - 1)

    case Event(CommandSent, ProcessingData(ch, pendingReplies, resubmits, n)) =>
      stay() using ProcessingData(ch, pendingReplies, resubmits, n - 1)

    case Event(ReplyReceived(r), ProcessingData(ch, pendingReplies, resubmits, n)) =>
      val (replyHandler, soFar) = pendingReplies.dequeue
      replyHandler.success(r)
      stay() using ProcessingData(ch, soFar, resubmits, n)

    case Event(ConnectionBroken(brokenChannel, _), ProcessingData(ch, pendingReplies, resubmits, n)) if brokenChannel == ch =>
      bootstrap.connect()
      goto(FastReconnecting) using FastReconnectingData(Some(brokenChannel), MessageAccumulator(Queue.empty, resubmits, n))
  }

  when(Connecting) {
    case Event(ConnectAttempt, _) =>
      bootstrap.connect()
      stay()

    case Event(ConnectionEstablished(ch), _) =>
      goto(Processing) using ProcessingData(ch, Queue.empty, Queue.empty, 0)

    case Event(ConnectionBroken(_, ex), _) =>
      context.system.scheduler.scheduleOnce(5 seconds, self, ConnectAttempt)
      stay()

    case Event(SubmitCommand(_, handler), _) =>
      handler.failure(new Exception("Disconnected"))
      stay()
  }

  /*whenUnhandled {
    case Event(Close, holder: ChannelHolder) =>
      holder.channel.close()
      stop(Normal)
  } */



  override def preStart() {
    super.preStart()
    bootstrap.connect()
  }

  override def postStop() {
    this.stateData match {
      case s: ChannelHolder => s.channel.close()
    }
    super.postStop()
  }
}
