package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.EventLoop
import codecrafters_redis.server.RedisServer

object Server {
  def main(args: Array[String]): Unit = {
    println("REDIS CLONE STARTING!")
    val config = Config.fromArgs(args)
    val server = RedisServer(config)
    val eventLoop = new EventLoop(Context(config))
    eventLoop.start()
  }
}
