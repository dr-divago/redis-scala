package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.EventLoop

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.Socket
import java.nio.ByteBuffer

object Server {
  def main(args: Array[String]): Unit = {
    println("REDIS CLONE STARTING!")
    val config = Config.fromArgs(args)
    val eventLoop = new EventLoop(Context(config))
    eventLoop.start()
  }



}
