package codecrafters_redis.eventloop

import codecrafters_redis.command.{Command, DimensionReplication, Event, FullResync, RdbDataReceived}
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
    // Reset the flag
    var handshakeJustCompleted = false

    // For binary RDB data processing
    def processRdbData(dim: Int, received: Int, data: Array[Byte]): (List[Event], Int) = {
      val dataLength = data.length
      val bytesNeeded = dim - received

      if (dataLength <= bytesNeeded) {
        println("All this data is part of RDB")
        val newReceived = received + dataLength

        if (newReceived >= dim) {
          println("RDB file complete - handshake finished")
          replicationState = HandshakeComplete
          handshakeJustCompleted = true
          (List(RdbDataReceived(data)), 0) // No excess data
        } else {
          println(" Still need more RDB data")
          replicationState = ProcessingRdbData(dim, newReceived)
          (List(RdbDataReceived(data)), 0) // No excess data
        }
      } else {
        println(" We have more data than needed for RDB - split it")
        val rdbPortion = data.take(bytesNeeded)
        val excessBytes = dataLength - bytesNeeded

        println("Mark handshake complete")
        replicationState = HandshakeComplete
        handshakeJustCompleted = true

        println(s"Add excess data to lineParser for continued parsing")
        if (excessBytes > 0) {
          val excessData = data.drop(bytesNeeded)
          val newData = new String(excessData)
          println(s"new line added ${newData}")
          lineParser.append(new String(excessData))
        }

        (List(RdbDataReceived(rdbPortion)), excessBytes)
      }
    }

    // Initial state handling
    val initialEvents = replicationState match {
      case ProcessingRdbData(dim, received) =>
        // Handle binary data
        val (events, excessBytes) = processRdbData(dim, received, data.getBytes)

        println(s"$handshakeJustCompleted and ${excessBytes}")
        if (handshakeJustCompleted && excessBytes > 0) {
          // Process any excess data as normal commands
          val remainingEvents = parseNormalCommands()
          events ++ remainingEvents
        } else {
          events
        }

      case _ =>
        // Append data for text protocol parsing
        lineParser.append(data)
        parseProtocolHandshake()
    }

    // If handshake just completed during RDB processing, we've already handled excess data
    // If handshake just completed during protocol parsing, we need to parse any remaining data
    if (handshakeJustCompleted && replicationState == HandshakeComplete && initialEvents.nonEmpty) {
      initialEvents
    } else {
      initialEvents
    }
  }

  def def protocolHandshake() : Unit = {
    replicationState match {
      case
    }
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
                case Some(FullResync) if replicationState == WaitingForFullResync =>
                  replicationState = WaitingForDimension
                  acc ++ event.toList

                case Some(DimensionReplication(dim)) if replicationState == WaitingForDimension =>
                  replicationState = ProcessingRdbData(dim, 0)
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
          val result = ProtocolParser.parse(line, currentState)

          result match {
            case parsed: Parsed =>
              lineParser.print()
              parsed
            case Continue(nextState) => processNextLine(nextState)
          }

        case None => Continue(currentState)
      }
    }
    processNextLine(parserState)
  }

  def write(data : Array[Byte]): Int = socketChannel.write(ByteBuffer.wrap(data))

  def isReplicationHandshakeComplete: Boolean = replicationState == HandshakeComplete
}



object Connection {
  def apply(socketChannel: SocketChannel, initialState: ParseState = WaitingForCommand(), context: Context) : Connection = {
    val connection = Connection(socketChannel, initialState, context)
    connection
  }
}

