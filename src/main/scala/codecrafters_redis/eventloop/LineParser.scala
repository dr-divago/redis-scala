package codecrafters_redis.eventloop

class LineParser(private val buffer : StringBuilder = new StringBuilder()) {
  private var pos = 0
  def nextLine(): Option[String] = {
    val endOfLineIdx = buffer.indexOf("\r\n", pos)
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
    val escaed = str.replace("\r", "\\r").replace("\n", "\\n")
    println(raw"Added to buffer $escaed")
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

  def dataAvailable() : Int = {
    buffer.length() - pos
  }

  def print() : Unit = {
    println(buffer)
  }
}
