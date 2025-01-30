package codecrafters_redis

case class Config(dirParam: String = "",
                  dbParam: String = "")

object Config {
  def fromArgs(args: Array[String]): Config = {
    val argsMap = args.sliding(2, 2).collect {
      case Array(key, value) => (key.stripPrefix("--"), value)
    }.toMap

    Config(
      dirParam = argsMap.getOrElse("dir", ""),
      dbParam = argsMap.getOrElse("dbfilename", "")
    )
  }
}