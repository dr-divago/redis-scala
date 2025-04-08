package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.EventLoop

object Server {
  def main(args: Array[String]): Unit = {
    println("REDIS CLONE STARTING!")
    val config = Config.fromArgs(args)
    val eventLoop = new EventLoop(Context(config))
    eventLoop.start()
  }



}
