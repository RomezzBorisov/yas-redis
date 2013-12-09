package io.netty.client

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, promise}
import io.netty.channel._
import io.netty.bootstrap.Bootstrap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import io.netty.client.ClientState.{SideEffect, StateUpdate}
import io.netty.channel.ChannelHandler.Sharable


class NettyClient[C,R](bs: Bootstrap) {

  private class SendFutureListener(cmd: CommandAndResponse[C,R]) extends ChannelFutureListener {
    def operationComplete(future: ChannelFuture) {
      if(future.isSuccess) {
        updateState(_.commandSent(cmd))
      } else {
        updateState(_.sendFailure(future.cause(), cmd)).apply()
      }
    }
  }


  private class NettySender(channel: Channel) extends Sender[C,R] {
    def send(cmd: CommandAndResponse[C,R]) {
      channel.writeAndFlush(cmd._1).addListener(new SendFutureListener(cmd))
    }

    def close() {
      channel.close()
    }
  }

  private class NettyConnector extends Connector {

    @Sharable
    private object ReceiveHandler extends ChannelInboundHandlerAdapter {
      override def channelRead(ctx: ChannelHandlerContext, msg: Any) {
        updateState(_.responseReceived(msg.asInstanceOf[R])).apply()
      }

      override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        updateState(_.receiveFailure(cause)).apply()
      }
    }

    @Sharable
    private object ConnectFutureListener extends ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if(future.isSuccess) {
          val channel = future.channel()
          channel.pipeline().addLast(ReceiveHandler)

          val sender = new NettySender(future.channel())
          updateState(_.connectionEstablished(sender, NettyConnector.this)).apply()

        } else {
          val cause = future.cause()
          println("connection failed " + cause)
          updateState(_.connectionBroken(future.cause(), NettyConnector.this)).apply()
        }
      }
    }


    def connectAsync() {
      bs.connect().addListener(ConnectFutureListener)
    }

    def scheduleConnect(delayMillis: Long) {
      bs.group().schedule(new Runnable {
        def run() {
          connectAsync()
        }
      }, delayMillis, TimeUnit.MILLISECONDS)
    }
  }

  private val stateRef = new AtomicReference[ClientState[C,R]](new WaitingForConnection[C,R](Queue.empty) with EventLogger[C,R])
  private val connector = new NettyConnector()
  connector.connectAsync()

  @tailrec
  private def updateState(f: ClientState[C,R] => StateUpdate[C,R]): SideEffect = {
    val oldState = stateRef.get()
    val StateUpdate(res, newState) = f(oldState)
    if(stateRef.compareAndSet(oldState, newState)) {
      res
    } else {
      updateState(f)
    }
  }


  def submitCommand(cmd: C)(implicit ctx: ExecutionContext): Future[R] = {
    val responsePromise = promise[R]()
    updateState(_.send((cmd, responsePromise))).apply()
    responsePromise.future
  }


  def close() {

  }

}


