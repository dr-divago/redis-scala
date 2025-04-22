package codecrafters_redis.eventloop

import codecrafters_redis.command.{Command, DimensionReplication, Event}
import codecrafters_redis.protocol._

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import scala.annotation.tailrec
import scala.util.Try

case class Connection(socketChannel: SocketChannel, key: SelectionKey) {
  private val buffer: ByteBuffer = ByteBuffer.allocate(1024)
  private val lineParser: LineParser = new LineParser()
  private var parsingState : ParseState = WaitingForCommand()

  def readIntoBuffer() : Try[Int] = {
    buffer.compact()
    Try(socketChannel.read(buffer))
  }

  def extractBytesFromBuffer(): Option[Array[Byte]] = {
    buffer.flip()
    if (buffer.remaining() > 0) {
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      buffer.compact()
      Some(bytes)
    } else {
      buffer.compact()
      None
    }
  }

  def finishConnectOnChannel(): Try[Boolean] = Try(socketChannel.finishConnect())


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
        lineParser.append(data)
        parseProtocolHandshake()
  }


  // Parse the handshake protocol (FULLRESYNC, dimension)
  private def parseProtocolHandshake(): List[Event] = {
    @tailrec
    def parseLines(state: ParseState, acc: List[Event] = List.empty): List[Event] = {
      lineParser.nextLine() match {
        case Some(line) =>
          // Parse the line
          val result = ProtocolParser.parse(line, state)

          // Handle the result based on type
          result match {
            case Parsed(value, nextState) =>
              // Only extract event for Parsed results
              val event = Event.parse(Parsed(value, nextState))

              // Check for state transitions and decide whether to continue
              event match {
                case Some(DimensionReplication(_)) =>
                  acc ++ event.toList

                case _ =>
                  parseLines(nextState, acc ++ event.toList)
              }

            case Continue(nextState) =>
              // For Continue results, just keep parsing with the next state
              parseLines(nextState, acc)
          }

        case None =>
          // No more lines to parse
          acc
      }
    }

    parseLines(parsingState)
  }

  def close(): Unit = {
    println("Closing connection")
    socketChannel.close()
    key.cancel()
  }

  // Parse normal commands (after handshake complete)
  private def parseNormalCommands(): List[Event] = {
    println("PARSE NORMAL COMMAND AFTER HANDSHAKE")
    @tailrec
    def parseLines(state: ParseState, acc: List[Event] = List.empty): List[Event] = {
      lineParser.nextLine() match {
        case Some(line) =>
          println(s"Parse the line : ${line}")
          val result = ProtocolParser.parse(line, state)

          // Handle the result based on type
          result match {
            case Parsed(value, nextState) =>
              // Extract event for Parsed results
              val event = Event.parse(Parsed(value, nextState))
              parseLines(nextState, acc ++ event.toList)

            case Continue(nextState) =>
              // For Continue results, just keep parsing with the next state
              parseLines(nextState, acc)
          }

        case None =>
          println("No more lines to parse")
          acc
      }
    }

    parseLines(parsingState)
  }


  def process(parserState: ParseState) : ParserResult = {
    @tailrec
    def processNextLine(currentState: ParseState): ParserResult = {
      lineParser.nextLine() match {
        case Some(line) =>
          println(s"line = ${line} going to parse")
          val result = ProtocolParser.parse(line, currentState)

          result match {
            case parsed: Parsed =>
              println(s"Parsed $parsed")
              parsed
            case Continue(nextState) => processNextLine(nextState)
          }

        case None => Continue(currentState)
      }
    }
    processNextLine(parserState)
  }

  def write(data : Array[Byte]): Int = socketChannel.write(ByteBuffer.wrap(data))

  def getLastData: String = {
    println(s"lineparser = ${lineParser}")
    lineParser.remaining()
  }

  def skipBytes(bytes: Int) : Unit = {
    println(s"lineparser = ${lineParser}")
    lineParser.skip(bytes)
  }
}



object Connection {
  def apply(socketChannel: SocketChannel, initialState: ParseState = WaitingForCommand(), key: SelectionKey) : Connection = {
    val connection = Connection(socketChannel, initialState, key)
    connection
  }
}

