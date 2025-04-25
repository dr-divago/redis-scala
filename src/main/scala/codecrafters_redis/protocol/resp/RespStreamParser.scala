package codecrafters_redis.protocol.resp



import java.nio.ByteBuffer
import scala.annotation.tailrec

sealed trait ParseState
case object WaitingForCommand extends ParseState
case class WaitingForNextElement(currentElements: List[RespValue], remaining : Int, consumedBytes: Int) extends ParseState

object RespStreamParser {
  def parse(buffer: ByteBuffer, state: ParseState) : (ParserResult, ParseState)  = {
    state match {
      case WaitingForNextElement(elements, 0, consumedBytes) => (Parsed(RespArray(elements), consumedBytes), WaitingForCommand)
      case _ if !buffer.hasRemaining => (Incomplete, state)
      case WaitingForCommand =>
        peekFirstChar(buffer) match {
          case '*' => parseRespArray(buffer, state)
          case '+' => parseRespValue(buffer, state, s => RespString(s))
          case ':' => parseRespValue(buffer, state, s => RespInteger(s.toLong))
          case '-' => parseRespValue(buffer, state, s => RespError(s))
        }

      case WaitingForNextElement(elements, remaining, consumedBytes) =>
        val (result, _) = parse(buffer, WaitingForCommand)
        result match {
          case Parsed(value, consumedBytes) =>
            parse(buffer, WaitingForNextElement(elements :+ value, remaining - 1, consumedBytes))
          case Incomplete => (Incomplete, WaitingForNextElement(elements, remaining, consumedBytes))
          case Error(message) => (Error(message), WaitingForCommand)
        }
    }
  }

  private def parseRespArray(buffer: ByteBuffer, state: ParseState) = {
    buffer.get()

    findCRLF(buffer) match {
      case Some(pos) =>
        val numberOfElements = extractElement(buffer, pos)
        numberOfElements.toInt match {
          case 0 => (Parsed(RespArray(List()), 2), WaitingForCommand)
          case -1 => (Parsed(RespNullArray(), 2), WaitingForCommand)
          case _ =>
            buffer.position(buffer.position() + 2)
            val consumedBytes = numberOfElements.length + 2 + 1
            parse(buffer, WaitingForNextElement(List(), numberOfElements.toInt, consumedBytes))
        }
      case None =>
        buffer.position(buffer.position() - 1)
        (Incomplete, state)
    }

  }

  private def parseRespValue(buffer: ByteBuffer, state: ParseState, value : String => RespValue) = {
    buffer.get()
    findCRLF(buffer) match {
      case Some(pos) =>
        val content = extractElement(buffer, pos)
        buffer.position(pos + 2)
        val consumedBytes = content.length + 2 + 1
        (Parsed(value(content), consumedBytes), WaitingForCommand)
      case None =>
        buffer.position(buffer.position() - 1)
        (Incomplete, state)
    }
  }

  private def extractElement(buffer: ByteBuffer, pos: Int): String = {
    val countLength = pos - buffer.position()
    val countBytes = new Array[Byte](countLength)
    buffer.get(countBytes, 0, countLength)
    new String(countBytes, "UTF-8")
  }

  private def peekFirstChar(buffer: ByteBuffer) : Char = {
    buffer.get(buffer.position()).toChar
  }

  private def findCRLF(buffer: ByteBuffer): Option[Int] = {
      val startPos = buffer.position()
      val limit = buffer.limit()
      @tailrec
      def findCRLFHelper(currentPos: Int): Option[Int] = {
        if (currentPos + 1 >= limit || (currentPos - startPos) >= 1024) {
          None
        }
        else if (buffer.get(currentPos) == '\r' && buffer.get(currentPos + 1) == '\n') {
          Some(currentPos)
        }
        else {
          findCRLFHelper(currentPos + 1)
        }
      }

      findCRLFHelper(buffer.position())
  }
}
