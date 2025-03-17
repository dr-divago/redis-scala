package codecrafters_redis.eventloop

import codecrafters_redis.command.Command
import codecrafters_redis.config.Context
import codecrafters_redis.db.{ExpiresIn, NeverExpires}
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

  private def getData: String = {
    buffer.flip()
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    new String(bytes)
  }

  def readData(key: SelectionKey) : String = {
    val sb = new StringBuilder()
    if (buffer.position() > 0) {
      sb.append(getData)
      buffer.clear()
    } else if (buffer.position() == buffer.limit()) {
      buffer.clear()
    }

    val byteRead = socketChannel.read(buffer)
    if (byteRead == -1) {
      socketChannel.close()
      key.cancel()
    }
    else if (byteRead > 0) {
      sb.append(getData)

    }
    if (byteRead != -1) {
      buffer.clear()
    }
    sb.mkString
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


  private def parseReplicationCommand(line: String, parseState: ParseState, replicationState: ReplicationState): (ParseState, ReplicationState) = {
    println(s"****Parse Replica Command with $line and state ${replicationState.state}")

    if (replicationState.isHandshakeDone) {
      println("Handshake complete, returning to normal command processing")
      return (WaitingForCommand(), replicationState)
    }

    ProtocolParser.parse(line, parseState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case _ =>
            val (newState, action) = ProtocolManager.processEvent(replicationState, ResponseReceived(value.head))
            ProtocolManager.executeAction(action, newState.context)
            (nextState, newState)
        }
      case Continue(nextState) =>
        (nextState, replicationState)
    }
  }

  private def parseLine(line: String, parseState: ParseState, replicaChannels: mutable.ArrayBuffer[SocketChannel]): ParseState = {
    ProtocolParser.parse(line, parseState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case "PING"       =>  socketChannel.write(ByteBuffer.wrap("+PONG\r\n".getBytes))
          case "ECHO"       =>  socketChannel.write(ByteBuffer.wrap(("$"+value(1).length+"\r\n"+value(1) + "\r\n").getBytes))
          case "SET"        =>  handleSetCommand(value, replicaChannels)
          case "GET"        =>  handleGetCommand(value(1))
          case "CONFIG"     =>  handleConfigGet(value)
          case "KEYS"       =>  handleKeysCommand(value)
          case "INFO"       =>  handleInfoCommand(value)
          case "REPLCONF"   =>  handleReplConfCommand(value)
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

  private def handleReplConfCommand(value: Vector[String]) = {
    socketChannel.write(ByteBuffer.wrap("+OK\r\n".getBytes))
  }

  private def handleInfoCommand(value: Vector[String]) = {
    val role = context.getReplication
    val masterId = context.getMasterIdStr
    val replicationId = s"master_repl_offset:${context.getMasterReplOffset}"
    val allResp = s"$role\n${masterId}\n$replicationId\n"
    val resp = s"$$${allResp.length}\r\n$allResp\r\n"

    socketChannel.write(ByteBuffer.wrap(resp.getBytes))
  }

  private def handleKeysCommand(value: Vector[String]) = {
    /*
    val keys = inMemoryDB.keys()
    val responseMsg = buildKeyCommand(keys)
    val fullMsg = ByteBuffer.wrap(responseMsg.getBytes)
    socketChannel.write(fullMsg)

     */
  }

  private def buildKeyCommand(keys : Iterable[String]): String = {
    val sizeOfArrayResponse = s"*${keys.size}\r\n"
    val elements = keys.map(key => s"$$${key.length}\r\n${key}\r\n").mkString
    sizeOfArrayResponse + elements.mkString
  }

  private def handleConfigGet(value: Vector[String]) = {
    val conf = value(2) match {
      case "dir" => context.config.dirParam
      case "dbfilename" => context.config.dbParam
    }
    socketChannel.write(ByteBuffer.wrap(("*2\r\n$"+value(2).length+"\r\n"+value(2)+"\r\n$"+conf.length+"\r\n"+conf+"\r\n").getBytes))

  }

  private def handleGetCommand(key: String): Unit = {
    /*
    val value : Option[String] = inMemoryDB.get(key)
    value match {
      case Some(v) => socketChannel.write(ByteBuffer.wrap(("$" + v.length + "\r\n" + v + "\r\n").getBytes))
      case None => socketChannel.write(ByteBuffer.wrap("$-1\r\n".getBytes))
    }

     */
  }

  private def handleSetCommand(value: Vector[String], replicaChannel : mutable.ArrayBuffer[SocketChannel]) = {
    /*
    if (context.config.replicaof.nonEmpty) {
      println("I am in replica")
    }
    value.length match {
      case 3 =>
        inMemoryDB.add(value(1), value(2), NeverExpires())
        socketChannel.write(ByteBuffer.wrap("+OK\r\n".getBytes))
      case 5 =>
        val expireAt = value(3)
        val milliseconds = value(4)
        socketChannel.write(ByteBuffer.wrap("+OK\r\n".getBytes))
        inMemoryDB.add(value(1), value(2), ExpiresIn(milliseconds.toLong))
    }
    propagateToReplicas(value, replicaChannel)

     */
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

  def write(data : String): Int = socketChannel.write(ByteBuffer.wrap(data.getBytes))
}



object Connection {
  def apply(socketChannel: SocketChannel, initialState: ParseState = WaitingForCommand(), context: Context) : Connection = {
    val connection = Connection(socketChannel, initialState, context)
    connection
  }
}

