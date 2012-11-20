package com.redis.protocol

import java.util.{Queue => JQueue}

import org.jboss.netty.channel.{MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import akka.dispatch.Promise

class RedisResponseHandler(replyPromises: JQueue[Promise[Reply]]) extends SimpleChannelUpstreamHandler {
  private var state: ReceiveState = Initial

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    state.withLine(e.getMessage.asInstanceOf[String]) match {
      case Right((Some(r), newState)) =>
        Option(replyPromises.poll()) match {
          case Some(promise) => promise.success(r)
          case None => throw new IllegalStateException("No result expected - promise queue is empty")
        }
        state = newState
      case Right((None, newState)) =>
        state = newState
      case Left(e) =>
        throw e

    }
  }
}

sealed trait ReceiveState {
  def withLine(s: String): Either[Exception,(Option[Reply], ReceiveState)] = try {
    Right(parse(s))
  } catch {
    case e: MatchError => Left(new RedisProtocolException("Unexpected reply '" + s + "' for state " + this))
  }

  protected def parse: PartialFunction[String, (Option[Reply], ReceiveState)]

}

object Initial extends ReceiveState {
  protected def parse = {
    case SingleLine(msg) => (Some(SingleLineReply(msg)), Initial)
    case IntegralLine(i) => (Some(IntegralReply(i)), Initial)
    case ErrorLine(e) => (Some(ErrorReply(e)), Initial)
    case BytesNumberLine(-1) => (Some(EmptyBulkReply), Initial)
    case BytesNumberLine(i) => (None, WaitBulkReply(i)(r => (Some(r), Initial)))
    case LinesNumberLine(0) => (Some(MultibulkReply(Array.empty)), Initial)
    case LinesNumberLine(-1) => (Some(EmptyMultiBulkReply), Initial)
    case LinesNumberLine(nReplies) => (None, WaitMultiBulkReply(nReplies, Nil))
  }
}

case class WaitBulkReply(replySize: Int)(f: BulkReply => (Option[Reply], ReceiveState)) extends ReceiveState {
  protected def parse = {
    case ValueLine(msg) => f(BulkReply(msg))
  }
}

case class WaitMultiBulkReply(nRepliesRemaing: Int, replies: List[BulkReply]) extends ReceiveState {
  protected def parse = {
    case BytesNumberLine(i) => (None, WaitBulkReply(i) {
      r =>
        nRepliesRemaing match {
          case 1 => (Some(MultibulkReply((r :: replies).reverse.toArray)), Initial)
          case n => (None, WaitMultiBulkReply(n - 1, r :: replies))
        }
    })

  }
}
