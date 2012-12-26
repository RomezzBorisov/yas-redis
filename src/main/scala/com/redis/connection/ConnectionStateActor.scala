package com.redis.connection

import akka.actor.{Actor, FSM}
import org.jboss.netty.channel.{ChannelFuture, ChannelFutureListener, Channel}
import com.redis.RedisCommand
import com.redis.protocol.Reply
import org.jboss.netty.bootstrap.ClientBootstrap
import akka.dispatch.Promise
import collection.immutable.Queue
import akka.util.duration._

sealed trait ConnectionMessage

case object ConnectAttempt extends ConnectionMessage

case class Connected(channel: Channel) extends ConnectionMessage

case class Disconnected(ex: Throwable) extends ConnectionMessage

case class CommandSendAttempt(cmd: RedisCommand, promise: Promise[Reply]) extends ConnectionMessage

case class CommandSent(replyHandler: Promise[Reply]) extends ConnectionMessage

case class ReplyReceived(r: Reply) extends ConnectionMessage

sealed trait ConnectionState

case object Initial extends ConnectionState

case object Connecting extends ConnectionState

case object Processing extends ConnectionState

case object ConnectionBroken extends ConnectionState

sealed trait StateData

case object Nothing extends StateData

case class ConnectingData(pending: Queue[CommandSendAttempt]) extends StateData

case class ProcessingData(channel: Channel, pendingReply: Queue[Promise[Reply]]) extends StateData

class ConnectionStateActor(bootstrap: ClientBootstrap) extends Actor with FSM[ConnectionState, StateData] {

  startWith(Connecting, ConnectingData(Queue.empty))

  when(Connecting) {
    case Event(Connected(channel), ConnectingData(pending)) =>
      val queue = pending.foldLeft(Queue.empty[Promise[Reply]]) {
        case (q, CommandSendAttempt(cmd, promise)) =>
          submitCommand(channel, cmd, promise)
          q.enqueue(promise)
      }
      goto(Processing) using ProcessingData(channel, queue)
    case Event(Disconnected(ex), ConnectingData(pending)) =>
      pending.foreach {
        case CommandSendAttempt(cmd, promise) =>
          promise.failure(ex)
      }
      context.system.scheduler.scheduleOnce(10 seconds, self, ConnectAttempt)
      goto(ConnectionBroken)


    case Event(attempt: CommandSendAttempt, ConnectingData(pending)) =>
      //println("Submit command in connecting")
      stay() using (ConnectingData(pending.enqueue(attempt)))

  }

  when(Processing) {
    case Event(CommandSendAttempt(cmd, promise), ProcessingData(channel, pending)) =>
      submitCommand(channel, cmd, promise)
      stay() using ProcessingData(channel, pending.enqueue(promise))
    case Event(ReplyReceived(r), ProcessingData(channel, promises)) if !promises.isEmpty =>
      val (promise, promisesSoFar) = promises.dequeue
      promise.success(r)
      stay() using ProcessingData(channel, promisesSoFar)
    case Event(Disconnected(ex), ProcessingData(_, pending)) =>
      pending.foreach(p => p.failure(ex))
      self ! ConnectAttempt
      goto(Connecting) using ConnectingData(Queue.empty)
    case Event(msg, data) =>
      println("msg=" + msg + ", data=" + data)
      stay()
  }

  when(ConnectionBroken) {
    case Event(CommandSendAttempt(_, promise), _) =>
      promise.failure(new Exception("Not connected"))
      stay()
    case Event(Connected(channel), _) =>
      goto(Processing) using ProcessingData(channel, Queue.empty)
    case Event(Disconnected(ex), _) =>
      context.system.scheduler.scheduleOnce(10 seconds, self, ConnectAttempt)
      stay()
    case Event(ConnectAttempt, _) =>
      connect()
      stay()
  }

  private def submitCommand(channel: Channel, cmd: RedisCommand, promise: Promise[Reply]) {
    channel.write(cmd).addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if (!future.isSuccess) {
          if (future.getCause != null)
            promise.failure(new Exception("Error sending command", future.getCause))
          else
            promise.failure(new Exception("Error sending command"))
        }
      }
    })
  }

  private def connect() {
    bootstrap.connect().addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if (future.isSuccess)
          self ! Connected(future.getChannel)
        else if (Option(future.getCause).isDefined)
          self ! Disconnected(new Exception("Connection failed", future.getCause))
        else
          self ! Disconnected(new Exception("Connection failed"))
      }
    })
  }

  override def preStart() {
    super.preStart()
    connect()
  }
}
