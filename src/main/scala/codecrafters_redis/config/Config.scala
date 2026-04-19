package codecrafters_redis.config

import codecrafters_redis.Logger

case class Config(dirParam: String = "",
                  dbParam: String = "",
                  port: String = "6379",
                  replicaof: String = "")

object Config {
  def fromArgs(args: Array[String]): Config = {
    if (args.length % 2 != 0) {
      Logger.warn(s"Odd number of arguments (${args.length}); last argument '${args.last}' ignored")
    }
    val argsMap = args.sliding(2, 2).collect {
      case Array(key, value) => (key.stripPrefix("--"), value)
    }.toMap

    Config(
      dirParam = argsMap.getOrElse("dir", ""),
      dbParam = argsMap.getOrElse("dbfilename", ""),
      port = argsMap.getOrElse("port", "6379"),
      replicaof = argsMap.getOrElse("replicaof", "")
    )
  }
}