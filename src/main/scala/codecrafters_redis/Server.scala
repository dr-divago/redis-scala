package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.EventLoop

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetSocketAddress, ServerSocket}

object Server {
  def main(args: Array[String]): Unit = {
    println("REDIS CLONE STARTING!")
    val config = Config.fromArgs(args)
    val context = Context(config)
    val eventLoop = new EventLoop(context)
    eventLoop.start()
  }
}
