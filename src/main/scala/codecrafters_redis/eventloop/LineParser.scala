package codecrafters_redis.eventloop

class LineParser(private val buffer : StringBuilder = new StringBuilder()) {
  private var pos = 0
  def nextLine(): Option[String] = {
    val endOfLineIdx = buffer.indexOf("\r\n", pos)
    if (endOfLineIdx == -1) {
      return None
    }
    val line = buffer.substring(pos, endOfLineIdx)
    println(s"LINE $line")
    pos = endOfLineIdx + 2
    if (pos > buffer.length() / 2) {
      compactBuffer()
    }
    Some(line)
  }

  private def compactBuffer(): Unit = {
    buffer.delete(0, pos)
    pos = 0
  }

  def append(str: String): StringBuilder= {
    buffer.append(str)
  }

  def clear() : Unit = {
    buffer.clear()
    pos = 0
  }

  def remaining() : String = {
    if (pos < buffer.length()) {
      buffer.substring(pos)
    } else {
      ""
    }
  }

  def dataAvailable() : Int = {
    buffer.length() - pos
  }
}
