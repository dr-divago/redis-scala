package codecrafters_redis

case class Config(
                          param1: String = "",      // Default values make parameters optional
                          param2: String = ""
                        )

object Config {
  def fromArgs(args: Array[String]): Config = {
    val argsMap = args.sliding(2, 2).collect {
      case Array(key, value) => (key.stripPrefix("--"), value)
    }.toMap

    Config(
      param1 = argsMap.getOrElse("dir", ""),
      param2 = argsMap.getOrElse("dbfilename", "")
    )
  }
}