package codecrafters_redis.eventloop

import codecrafters_redis.Logger
import codecrafters_redis.command.{Command, Event}
import codecrafters_redis.protocol.resp.{
  Incomplete, ParseState, Parsed, RespArray, RespBulkString, RespStreamParser, WaitingForCommand,
  Error => ParseError
}

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.util.Try

case class Connection(socketChannel: SocketChannel, key: SelectionKey) {
  private val buffer: ByteBuffer = ByteBuffer.allocate(1024)
  private var leftover: Array[Byte] = Array.empty

  def readIntoBuffer(): Try[Int] = {
    buffer.compact()
    Try(socketChannel.read(buffer))
  }

  def finishConnectOnChannel(): Try[Boolean] = Try(socketChannel.finishConnect())

  def extractBytesFromBuffer(): Option[Array[Byte]] = {
    buffer.flip()
    if (buffer.hasRemaining) {
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      Some(bytes)
    } else {
      None
    }
  }

  def process(data: Array[Byte]): List[Command] = {
    val combined = if (leftover.nonEmpty) leftover ++ data else data
    leftover = Array.empty
    val buf = ByteBuffer.wrap(combined)

    @tailrec
    def loop(state: ParseState, acc: List[Command]): List[Command] = {
      if (!buf.hasRemaining) acc
      else RespStreamParser.parse(buf, state) match {
        case (Parsed(RespArray(elements), _), _) =>
          val tokens = elements.collect { case RespBulkString(b) => new String(b, StandardCharsets.UTF_8) }.toVector
          loop(WaitingForCommand, acc ++ Command.fromArgs(tokens).toList)
        case (Incomplete, _) =>
          if (buf.hasRemaining) {
            leftover = new Array[Byte](buf.remaining())
            buf.get(leftover)
          }
          acc
        case (ParseError(_), _) => acc
        case (_, nextState) => loop(nextState, acc)
      }
    }

    loop(WaitingForCommand, List.empty)
  }

  def processResponse(data: Array[Byte]): List[Event] = List.empty[Event]

  def write(data: Array[Byte]): Int = socketChannel.write(ByteBuffer.wrap(data))

  def close(): Unit = {
    Logger.info(s"Closing connection ${socketChannel.getRemoteAddress}")
    socketChannel.close()
    key.cancel()
  }

  def skipBytes(bytes: Int): Unit = {}
}
