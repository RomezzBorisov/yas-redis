package io.netty

import scala.concurrent.Promise
import scala.collection.immutable.Queue

package object client {
  type CommandAndResponse[C,R] = (C, Promise[R])
  type CommandQueue[C,R] = Queue[CommandAndResponse[C,R]]


  trait Sender[C,R] {
    def send(cmd: CommandAndResponse[C,R])
    def close()
  }

  trait Connector {
    def connectAsync()
    def scheduleConnect(delayMillis: Long)
  }

}
