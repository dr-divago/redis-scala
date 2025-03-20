package codecrafters_redis.eventloop

import codecrafters_redis.command.{Command, Event, RdbDataReceived}
import codecrafters_redis.config.Context
import codecrafters_redis.protocol._

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import scala.annotation.tailrec

case class Connection(socketChannel: SocketChannel, context: Context) {
  private val buffer: ByteBuffer = ByteBuffer.allocate(1024)
  private val lineParser: LineParser = new LineParser()
  private var parsingState : ParseState = WaitingForCommand()

  private val byteAccumulator = new scala.collection.mutable.ArrayBuffer[Byte]()

  private sealed trait ReplicationState
  private case object WaitingForFullResync extends ReplicationState
  private case object WaitingForDimension extends ReplicationState
  private case class ProcessingRdbData(dim: Int, received: Int) extends ReplicationState
  private case object HandshakeComplete extends ReplicationState

  private var replicationState: ReplicationState = WaitingForFullResync

  def readData(key: SelectionKey): Array[Byte] = {
    if (buffer.position() > 0) {
      buffer.flip()
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      byteAccumulator.appendAll(bytes)
      buffer.clear()
    } else if (buffer.position() == buffer.limit()) {
      buffer.clear()
    }

    val byteRead = socketChannel.read(buffer)
    if (byteRead == -1) {
      socketChannel.close()
      key.cancel()
    } else if (byteRead > 0) {
      buffer.flip()
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      byteAccumulator.appendAll(bytes)
      buffer.clear()
    }

    if (byteAccumulator.nonEmpty) {
      val result = byteAccumulator.toArray
      byteAccumulator.clear()
      result
    } else {
      Array.empty[Byte]
    }
  }


  def process(data: String): List[Command] = {
    lineParser.append(data)

    @tailrec
    def processUntilCommand(currentState: ParseState, events : List[Command] = List.empty) : List[Command] = {
      process(currentState) match {
        case Parsed(value, nextState) =>
          val event = Command.parse(Parsed(value, nextState))
          processUntilCommand(nextState, events ++ event.toList)
        case Continue(nextState) =>
          parsingState = nextState
          events
      }
    }

    processUntilCommand(parsingState)

  }

  def processResponse(data: String): List[Event] = {
    replicationState match {
      case ProcessingRdbData(dim, received) =>
        val bytes = data.getBytes
        val newReceived = received + bytes.length

        if (newReceived >= dim) {
          // Handshake complete
          replicationState = HandshakeComplete
          List(RdbDataReceived(bytes))
        } else {
          // Still collecting RDB data
          replicationState = ProcessingRdbData(dim, newReceived)
          List(RdbDataReceived(bytes))
        }
      case _ =>
        lineParser.append(data)

        @tailrec
        def processUntilCommand(currentState: ParseState, events : List[Event] = List.empty) : List[Event] = {
          process(currentState) match {
            case Parsed(value, nextState) =>
              val event = Event.parse(Parsed(value, nextState))
              processUntilCommand(nextState, events ++ event.toList)
            case Continue(nextState) =>
              parsingState = nextState
              events
          }
        }

        processUntilCommand(parsingState)

    }

  }

  def process(parserState: ParseState) : ParserResult = {
    @tailrec
    def processNextLine(currentState: ParseState): ParserResult = {
      lineParser.nextLine() match {
        case Some(line) =>
          val result = ProtocolParser.parse(line, currentState)

          result match {
            case parsed: Parsed => parsed
            case Continue(nextState) => processNextLine(nextState)
          }

        case None => Continue(currentState)
      }
    }
    processNextLine(parserState)
  }

  def write(data : Array[Byte]): Int = socketChannel.write(ByteBuffer.wrap(data))
}



object Connection {
  def apply(socketChannel: SocketChannel, initialState: ParseState = WaitingForCommand(), context: Context) : Connection = {
    val connection = Connection(socketChannel, initialState, context)
    connection
  }
}

