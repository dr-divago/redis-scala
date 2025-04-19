package codecrafters_redis.eventloop

class LineParser(private val buffer : StringBuilder = new StringBuilder()) {
  private var pos = 0
  def nextLine(): Option[String] = {
    val escaped = buffer.toString().replace("\r", "\\r").replace("\n", "\\n")
    println(s"Buffer = $escaped")
    val endOfLineIdx = buffer.indexOf("\r\n", pos)
    println(s"endofline idx = ${endOfLineIdx} pos = ${pos}")
    if (endOfLineIdx == -1) {
      return None
    }
    val line = buffer.substring(pos, endOfLineIdx)
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
    val res = buffer.append(str)
    res
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

  def skip(bytesToSkip: Int) : Unit = {
    val escaped = buffer.toString().replace("\r", "\\r").replace("\n", "\\n")
    println(s"Current buffer  ${escaped}")
    println(s"current pos ${pos}")
    println(s"size current buffer ${buffer.length}")
    val newPos = pos + bytesToSkip -1
    println(s"new pos = $newPos")
    buffer.substring(newPos)
    pos = newPos
  }

}
