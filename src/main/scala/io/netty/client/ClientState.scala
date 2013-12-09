package io.netty.client

import scala.collection.immutable.Queue
import ClientState._

sealed trait ClientState[C,R] {

  protected def illegalState(methodName: String) = throw new IllegalArgumentException("Unexpected call of " + methodName + " in " + getClass.getSimpleName)

  def send(cmd: CommandAndResponse[C,R]): StateUpdate[C,R]

  def commandSent(cmd: CommandAndResponse[C,R]): StateUpdate[C,R] = illegalState("commandSent")
  def sendFailure(cause: Throwable, cmd: CommandAndResponse[C,R]): StateUpdate[C,R] = illegalState("sendFailure")

  def responseReceived(response: R): StateUpdate[C,R] = illegalState("responseReceived")
  def receiveFailure(cause: Throwable): StateUpdate[C,R] = illegalState("receiveFailure")

  def connectionEstablished(sender: Sender[C,R], connector: Connector): StateUpdate[C,R] = illegalState("connectionEstablished")
  def connectionBroken(cause: Throwable, connector: Connector): StateUpdate[C,R] = illegalState("connectionBroken")
}

object ClientState {
  type SideEffect = Function0[Unit]
  object DoNothing extends SideEffect {
    def apply() {}
  }

  case class StateUpdate[C,R](sideEffect: SideEffect, newState: ClientState[C,R])
}




trait EventLogger[C,R] extends ClientState[C,R] {
  private def logCall[T](actualCall: => T, method: String, args: Any*): T = {
    println(getClass.getSuperclass.getSimpleName + "." + method + ": " + args.map(_.toString.replace("\n","\\n")))
    actualCall
  }


  abstract override def send(cmd: CommandAndResponse[C,R]): StateUpdate[C,R] =
    logCall(super.send(cmd), "send", cmd)

  abstract override def commandSent(cmd: CommandAndResponse[C,R]): StateUpdate[C,R] =
    logCall(super.commandSent(cmd), "commandSent", cmd)

  abstract override def sendFailure(cause: Throwable, cmd: CommandAndResponse[C,R]): StateUpdate[C,R] =
    logCall(super.sendFailure(cause, cmd), "sendFailure", cause, cmd)

  abstract override def responseReceived(response: R): StateUpdate[C, R] =
    logCall(super.responseReceived(response), "responseReceived", response)

  abstract override def receiveFailure(cause: Throwable): StateUpdate[C, R] =
    logCall(super.receiveFailure(cause), "receiveFailure", cause)

  abstract override def connectionEstablished(sender: Sender[C,R], connector: Connector): StateUpdate[C, R] =
    logCall(super.connectionEstablished(sender, connector), "connectionEstablished")

  abstract override def connectionBroken(cause: Throwable, connector: Connector): StateUpdate[C, R] =
    logCall(super.connectionBroken(cause, connector), "connectionBroken", cause)
}

case class WaitingForConnection[C,R](retries: CommandQueue[C,R]) extends ClientState[C,R] {
  override def send(cmd: CommandAndResponse[C,R]) = StateUpdate(DoNothing, new WaitingForConnection(retries.enqueue(cmd)) with EventLogger[C,R])

  override def connectionEstablished(sender: Sender[C,R], connector: Connector) = if(retries.isEmpty) {
    StateUpdate(DoNothing, new ConnectionEstablished(sender, connector, 0, Queue.empty, Queue.empty) with EventLogger[C,R])
  } else {
    StateUpdate(() => retries.foreach(sender.send), new RetryingFailedSends(sender,connector, retries.size, Queue.empty, Queue.empty, Queue.empty) with EventLogger[C,R])
  }

  override def connectionBroken(cause: Throwable, connector: Connector) = {
    def failPendingAndScheduleReconnect() {
      retries.foreach(_._2.failure(cause))
      connector.scheduleConnect(5000)
    }

    StateUpdate(() => failPendingAndScheduleReconnect(), new ConnectionBroken[C,R](cause) with EventLogger[C,R])
  }
}

case class RetryingFailedSends[C,R](sender: Sender[C,R], connector: Connector,
                               nPendingSends: Int,
                               pendingReplies: CommandQueue[C,R],
                               failedSends: CommandQueue[C,R],
                               incomingCommands: CommandQueue[C,R]) extends ClientState[C,R] {

  def send(cmd: CommandAndResponse[C,R]): StateUpdate[C,R] =
    StateUpdate(DoNothing, new RetryingFailedSends(sender, connector, nPendingSends, pendingReplies, incomingCommands.enqueue(cmd), failedSends) with EventLogger[C,R])

  override def commandSent(cmd: CommandAndResponse[C,R]): StateUpdate[C,R] = if(nPendingSends == 1) {
    StateUpdate(() => incomingCommands.foreach(sender.send), new ConnectionEstablished(sender, connector, incomingCommands.size, pendingReplies.enqueue(cmd), Queue.empty) with EventLogger[C,R])
  } else {
    StateUpdate(DoNothing, new RetryingFailedSends(sender, connector, nPendingSends - 1, pendingReplies.enqueue(cmd), failedSends, incomingCommands) with EventLogger[C,R])
  }

  private def failPendingAndReconnect(cause: Throwable) {
    pendingReplies.foreach(_._2.failure(cause))
    sender.close()
    connector.connectAsync()
  }

  override def sendFailure(cause: Throwable, cmd: CommandAndResponse[C,R]): StateUpdate[C,R] = if(nPendingSends == 1) {
    StateUpdate(() => failPendingAndReconnect(cause), WaitingForConnection(failedSends.enqueue(cmd).enqueue(incomingCommands)))
  } else {
    StateUpdate(DoNothing, RetryingFailedSends(sender, connector, nPendingSends - 1, pendingReplies, failedSends.enqueue(cmd), incomingCommands))
  }
}




case class ConnectionEstablished[C,R](sender: Sender[C,R], connector: Connector, nPendingSends: Int, pendingReplies: CommandQueue[C,R], failedSends: CommandQueue[C,R]) extends ClientState[C,R] {

  def failPendingAndReconnect(cause: Throwable) {
    pendingReplies.foreach(_._2.failure(cause))
    sender.close()
    connector.connectAsync()
  }

  override def send(cmd: CommandAndResponse[C,R]) =
    StateUpdate(() => sender.send(cmd), new ConnectionEstablished(sender, connector, nPendingSends + 1, pendingReplies, failedSends) with EventLogger[C,R])

  override def commandSent(cmd: CommandAndResponse[C,R]) =
    StateUpdate(DoNothing, new ConnectionEstablished(sender, connector, nPendingSends - 1, pendingReplies.enqueue(cmd), failedSends) with EventLogger[C,R])

  override def sendFailure(cause: Throwable, cmd: CommandAndResponse[C,R]) = if(nPendingSends == 1) {
    StateUpdate(() => failPendingAndReconnect(cause), new WaitingForConnection(failedSends.enqueue(cmd)) with EventLogger[C,R])
  } else {
    StateUpdate(DoNothing, new ConnectionEstablished(sender, connector, nPendingSends - 1, pendingReplies, failedSends.enqueue(cmd)) with EventLogger[C,R])
  }

  override def responseReceived(response: R) = {
    val (hd, tl) = pendingReplies.dequeue
    StateUpdate(() => hd._2.success(response), new ConnectionEstablished(sender, connector, nPendingSends, tl, failedSends) with EventLogger[C,R])
  }

  override def receiveFailure(cause: Throwable): StateUpdate[C,R] = if(nPendingSends == 0){
    StateUpdate(() => failPendingAndReconnect(cause), new WaitingForConnection(failedSends) with EventLogger[C,R])
  } else {
    StateUpdate(DoNothing, this)
  }
}

case class ConnectionBroken[C,R](cause: Throwable) extends ClientState[C,R] {
  override def send(cmd: CommandAndResponse[C,R]) = StateUpdate(() => cmd._2.failure(cause), this)

  override def connectionEstablished(sender: Sender[C,R], connector: Connector): StateUpdate[C,R] =
    StateUpdate(DoNothing, new ConnectionEstablished(sender, connector, 0, Queue.empty, Queue.empty) with EventLogger[C,R])

  override def connectionBroken(cause: Throwable, connector: Connector): StateUpdate[C,R] =
    StateUpdate(() => connector.scheduleConnect(5000), this)
}
