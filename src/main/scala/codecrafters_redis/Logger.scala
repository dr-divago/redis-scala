package codecrafters_redis

object Logger {
  private val level: Int = sys.env.getOrElse("LOG_LEVEL", "INFO").toUpperCase match {
    case "DEBUG" => 0
    case "INFO"  => 1
    case "WARN"  => 2
    case "ERROR" => 3
    case _       => 1
  }

  def debug(msg: => String): Unit = if (level <= 0) System.err.println(s"[DEBUG] $msg")
  def info(msg: => String): Unit  = if (level <= 1) System.err.println(s"[INFO]  $msg")
  def warn(msg: => String): Unit  = if (level <= 2) System.err.println(s"[WARN]  $msg")
  def error(msg: => String): Unit = if (level <= 3) System.err.println(s"[ERROR] $msg")
  def error(msg: => String, t: Throwable): Unit = if (level <= 3) {
    System.err.println(s"[ERROR] $msg")
    t.printStackTrace(System.err)
  }
}
