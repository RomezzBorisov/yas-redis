package com.redis.connection

import com.redis.{RedisDataOperations, ConnectionConfig, RedisClient}
import scala.concurrent.{ExecutionContext, Future}
import com.redis.protocol.{ResponseDecoder, CommandEncoder, RedisCommand, Reply}
import io.netty.channel.ChannelInitializer
import io.netty.client.NettyClient
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.StringDecoder


class RedisNodeClient(client: NettyClient[RedisCommand, Reply]) extends RedisClient with RedisDataOperations {

  protected def submitCommand(name: String, keys: Array[String], args: Array[String])(implicit ctx: ExecutionContext): Future[Reply] =
    client.submitCommand(RedisCommand(name, args))

  def close() {
    client.close()

  }

}

object RedisNodeClient {

  private class RedisChannelInitHandler(cfg: ConnectionConfig) extends ChannelInitializer[NioSocketChannel] {
    def initChannel(ch: NioSocketChannel) {
      ch.pipeline()
        .addLast(new LineBasedFrameDecoder(cfg.maxLineLength, true, true))
        .addLast(new StringDecoder(CommandEncoder.UTF8))
        .addLast(new ResponseDecoder())
        .addLast(CommandEncoder)
    }
  }

  def apply(eventLoopGroup: NioEventLoopGroup, cfg: ConnectionConfig): RedisNodeClient = {

    val bs = new Bootstrap()
      .group(eventLoopGroup)
      .channel(classOf[NioSocketChannel])
      .remoteAddress(cfg.host, cfg.port)
      .handler(new RedisChannelInitHandler(cfg))

    new RedisNodeClient(new NettyClient[RedisCommand, Reply](bs))
  }


}
