package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.server.RedisServer

object Server {
  def main(args: Array[String]): Unit = {
    Logger.info("Redis server starting")
    val config = Config.fromArgs(args)
    val server = RedisServer(Context(config))
    server.start()
  }
}
