package com.redis

import akka.dispatch.Promise
import protocol.Reply
import collection.immutable.Queue

case class PendingCommands(replyPending: Queue[CommandAndReplyPromise], sendPending: Queue[CommandAndReplyPromise]) {
  def commandSent: PendingCommands = {
    require(!sendPending.isEmpty)
    val (sent, sendPendingSoFar) = sendPending.dequeue
    PendingCommands(replyPending.enqueue(sent), sendPendingSoFar)
  }

  def replyReceived(reply: Reply): PendingCommands = {
    require(!replyPending.isEmpty)
    val (CommandAndReplyPromise(_, promise), replyPendingSoFar) = replyPending.dequeue
    promise.success(reply)
    PendingCommands(replyPendingSoFar, sendPending)
  }
}

case class CommandAndReplyPromise(cmd: RedisCommand, replyPromise: Promise[Reply])
