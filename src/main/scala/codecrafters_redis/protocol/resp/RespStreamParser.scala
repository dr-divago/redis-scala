package codecrafters_redis.protocol.resp



import java.nio.ByteBuffer
import scala.annotation.tailrec

sealed trait ParseState
case object WaitingForCommand extends ParseState
case class WaitingForNextElement(currentElements: List[RespValue], remaining : Int, consumedBytes: Int) extends ParseState

object RespStreamParser {
  def parse(buffer: ByteBuffer, state: ParseState) : (ParserResult, ParseState)  = {
    if (!buffer.hasRemaining) {
      (Incomplete, state)
    } else {
      state match {
        case WaitingForCommand =>
          peekFirstChar(buffer) match {
            case '*' =>
              buffer.get()

              findCRLF(buffer) match {
                case Some(pos) =>
                  val numberOfElements = extractNumberOfElement(buffer, pos)
                  buffer.position(buffer.position() + 2)
                  val consumedBytes = numberOfElements.toString.length + 2 + 1
                  val newState = WaitingForNextElement(List(), numberOfElements, consumedBytes)
                  (Incomplete, newState)
                case None =>
                  buffer.position(buffer.position() - 1)
                  (Incomplete, state)
              }
          }
        case WaitingForNextElement(elements, remaining, consumedBytes) if remaining == 0 => (Parsed(RespArray(elements), consumedBytes), WaitingForCommand)

      }
    }
  }

  private def extractNumberOfElement(buffer: ByteBuffer, pos: Int): Int = {
    val countLength = pos - buffer.position()
    val countBytes = new Array[Byte](countLength)
    buffer.get(countBytes, 0, countLength)
    new String(countBytes, "UTF-8").toInt
  }

  def peekFirstChar(buffer: ByteBuffer) : Char = {
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
