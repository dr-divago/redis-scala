package codecrafters_redis.eventloop

import codecrafters_redis.command.{Command, Event}
import codecrafters_redis.config.Context
import codecrafters_redis.protocol._

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
import scala.collection.mutable

case class Connection(socketChannel: SocketChannel, context: Context) {
  private val buffer: ByteBuffer = ByteBuffer.allocate(1024)
  private val lineParser: LineParser = new LineParser()
  private var parsingState : ParseState = WaitingForCommand()

  private val byteAccumulator = new scala.collection.mutable.ArrayBuffer[Byte]()

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


  def process(data: String): Option[Command] = {
    val parserResult = process(data, parsingState)
    val nextState = parserResult match {
      case Parsed(_, nextState) => nextState
      case Continue(nextState) => nextState
    }
    parsingState = nextState
    Command.parse(parserResult)
  }

  def processResponse(data: String): Option[Event] = {
    val parserResult = process(data, parsingState)
    val nextState = parserResult match {
      case Parsed(_, nextState) => nextState
      case Continue(nextState) => nextState
    }
    parsingState = nextState
    Event.parse(parserResult)
  }

  def process(data: String, parserState: ParseState): ParserResult = {
    lineParser.append(data)

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



  private def parseLine(line: String, parseState: ParseState, replicaChannels: mutable.ArrayBuffer[SocketChannel]): ParseState = {
    ProtocolParser.parse(line, parseState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case "PSYNC"      =>  handlePSyncCommand(value, replicaChannels)
          case "FULLRESYNC" =>  handleFullResync(value)
        }
        nextState
      case Continue(nextState) => nextState
    }
  }


  private def handleFullResync(value: Vector[String]): Unit = {
    println("*****************")
    println("FULL RESYNC")
    println("******************")
  }

  private def handlePSyncCommand(value: Vector[String], replicaChannels: mutable.ArrayBuffer[SocketChannel]): Unit = {
    val masterId = context.getMasterId
    socketChannel.write(ByteBuffer.wrap(s"+FULLRESYNC $masterId ${context.getMasterReplOffset}\r\n".getBytes))

    socketChannel.write(ByteBuffer.wrap("$88\r\n".getBytes))
    val byesFile = Files.readAllBytes(Paths.get("empty.rdb"))
    socketChannel.write(ByteBuffer.wrap(byesFile))

    replicaChannels += socketChannel
    println(s"New replica connected: ${socketChannel.socket().getInetAddress} ${socketChannel.socket().getPort}")
  }


  private def propagateToReplicas(value:  Vector[String], replicaChannels : mutable.ArrayBuffer[SocketChannel]) = {
    println(s"PropagaToReplicas ${replicaChannels.length}")
    val command = buildSetCommand(value)
    val buffer = ByteBuffer.wrap(command.getBytes)

    val failedChannels = mutable.ArrayBuffer[SocketChannel]()

    for (ch <- replicaChannels) {
      try {
        ch.write(buffer)
        buffer.rewind()
      } catch {
        case _: IOException =>
          println(s"Failed to propagate to replica ${ch.socket().getInetAddress}")
          try {
            ch.close()
          } catch {
            case _ : IOException =>
          }
          failedChannels += ch
      }
    }
    replicaChannels --= failedChannels
  }


  private def buildSetCommand(value: Vector[String]): String = {
    val commandParts = value.map(part => s"$$${part.length}\r\n$part\r\n")
    s"*${value.length}\r\n${commandParts.mkString}"
  }

  def write(data : Array[Byte]): Int = socketChannel.write(ByteBuffer.wrap(data))
}



object Connection {
  def apply(socketChannel: SocketChannel, initialState: ParseState = WaitingForCommand(), context: Context) : Connection = {
    val connection = Connection(socketChannel, initialState, context)
    connection
  }
}

