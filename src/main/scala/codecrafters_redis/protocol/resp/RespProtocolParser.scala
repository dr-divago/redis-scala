package codecrafters_redis.protocol.resp

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec

sealed trait RespValue
case class RespString(value: String) extends RespValue
case class RespInteger(value: Long) extends RespValue
case class RespBulkString(value: Array[Byte]) extends RespValue
case class RespError(value: String) extends RespValue
case class RespNullBulkString() extends RespValue
case class RespNullArray() extends RespValue
case class RespArray(value: Int) extends RespValue
case class RespArrayHeader(value: Int) extends RespValue


sealed trait ParserResult
case class Parsed(value: RespValue, consumedBytes: Int) extends ParserResult
case object Incomplete extends ParserResult
case class Error(message: String) extends ParserResult

object RespProtocolParser {

  private val MAX_LINE_LENGTH = 1024

  def parse(buffer: ByteBuffer) : ParserResult = {
    if (!buffer.hasRemaining) {
      return Incomplete
    }
    val firstByte = buffer.get(buffer.position())
      firstByte match {
        case '+' => parseSimpleString(buffer)
        case '-' => parseError(buffer)
        case ':' => parseInteger(buffer)
        case '$' => parseBulkString(buffer)
        case '*' => parseArray(buffer)
        case _ => Error("Unknown first byte")
      }
  }

  private def findCRLF(buffer: ByteBuffer): Int = {
    val startPos = buffer.position()
    val limit = buffer.limit()
    @tailrec
    def findCRLFHelper(currentPos: Int): Int = {
      if (currentPos + 1 >= limit || (currentPos - startPos) >= MAX_LINE_LENGTH) {
        -1
      }
      else if (buffer.get(currentPos) == '\r' && buffer.get(currentPos + 1) == '\n') {
        currentPos
      }
      else {
        findCRLFHelper(currentPos + 1)
      }
    }

    findCRLFHelper(buffer.position())
  }



  private def parseLine(buffer: ByteBuffer, typeByte : Char): ParserResult = {
    val initialPos = buffer.position()
    val idxCRLF = findCRLF(buffer)
    if (idxCRLF == -1) {
      buffer.position(initialPos)
      return Incomplete
    }
    val contentLength = idxCRLF - initialPos - 1
    if (contentLength < 0) {
      buffer.position(initialPos)
      return Error("Invalid line length")
    }

    val contentBytes = new Array[Byte](contentLength)
    buffer.position(initialPos + 1)
    buffer.get(contentBytes, 0, contentLength)
    val contentString = new String(contentBytes, StandardCharsets.UTF_8)
    buffer.position(idxCRLF + 2)
    val consumedBytes = idxCRLF + 2

    typeByte match {
      case '+' => Parsed(RespString(contentString), consumedBytes)
      case '-' => Parsed(RespError(contentString), consumedBytes)
      case ':' => Parsed(RespInteger(contentString.toLong), consumedBytes)
      case '*' => Parsed(RespArrayHeader(contentString.toInt), consumedBytes)
      case '$' => Parsed(RespInteger(contentString.toLong), consumedBytes)
      case _ => Error("Unknown first byte")
    }
  }

  private def parseArray(buffer: ByteBuffer): ParserResult = {
    parseArrayHeader(buffer)
  }

  private def parseArrayHeader(buffer: ByteBuffer) : ParserResult = {
    val countLines = parseLine(buffer, '*')
    countLines match {
      case Parsed(RespArrayHeader(value), consumedBytes) if value == -1 => Parsed(RespNullArray(), consumedBytes )
      case Parsed(RespArrayHeader(value), _) if value < 0 => Error("Invalid array header")
      case Parsed(RespArrayHeader(value), consumedBytes) => Parsed(RespArrayHeader(value), consumedBytes)
      case Incomplete => Incomplete
      case _ => Error("Unknown first byte")
    }
  }

  private def parseBulkString(buffer: ByteBuffer) : ParserResult = {
    val originalPos = buffer.position()
    val countLines = parseLine(buffer, '$')
    countLines match {
      case Parsed(RespInteger(length), consumedBytes) if length == -1 => Parsed(RespNullBulkString(), consumedBytes )
      case Parsed(RespInteger(length), _) if length < 0 => Error("Invalid bulk string header")
      case Parsed(RespInteger(length), consumedBytes) =>
        if (buffer.remaining() < length.toInt + 2) {
          buffer.position(originalPos)
          Incomplete
        }
        else {
          val contentBytes = new Array[Byte](length.toInt)
          buffer.get(contentBytes, 0, length.toInt)
          val nextChar = buffer.get(buffer.position())
          if (nextChar != '\r')
            Error("Invalid missing CR")
          val nextChar2 = buffer.get(buffer.position() + 1)
          if (nextChar2 != '\n')
            Error("Invalid missing LF")
          val totalConsumed = consumedBytes + length.toInt + 2
          buffer.position(originalPos + totalConsumed)
          Parsed(RespBulkString(contentBytes), totalConsumed)
        }
    }
  }

  private def parseInteger(buffer: ByteBuffer) : ParserResult = {
    parseLine(buffer, ':')
  }

  private def parseError(buffer: ByteBuffer) : ParserResult = {
    parseLine(buffer, '-')
  }

  private def parseSimpleString(buffer: ByteBuffer) : ParserResult = {
    parseLine(buffer, '+')
  }

}
