package codecrafters_redis.eventloop

import codecrafters_redis.config.Context
import codecrafters_redis.db.{ExpiresIn, InMemoryDB, NeverExpires}
import codecrafters_redis.protocol.{Continue, Parsed, ProtocolParser}

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
import scala.collection.mutable

case class Connection(socketChannel: SocketChannel, context: Context) {
  private val buffer: ByteBuffer = ByteBuffer.allocate(1024)
  private val lineParser: LineParser = new LineParser()
  private val tasks: mutable.Queue[Task] = mutable.Queue.empty
  private val replicaChannels = mutable.ArrayBuffer[SocketChannel]()
  private val inMemoryDB = context.getDB

  def addTask(task : Task) : Unit = {
    tasks.enqueue(task)
  }

  def nextTask() : Option[Task] = {
    if (tasks.isEmpty) None
    else Some(tasks.dequeue())
  }

  def addData(data: String) : Unit = {
    lineParser.append(data)
  }

  def readDataFromClient(key: SelectionKey): Unit = {
    buffer.compact()


    val byteRead = socketChannel.read(buffer)
    if (byteRead == -1) {
      socketChannel.close()
      key.cancel()
      //connections.dropWhile(_.socketChannel == client)
    }
    else {
      buffer.flip()
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      val data = new String(bytes)
      addData(data)

      @tailrec
      def processLine() : Unit = {
        lineParser.nextLine() match {
          case Some(l) =>
            nextTask() match {
              case Some(task) => parseLine(l, task)
              case None => println(s"No task found for client ${socketChannel.socket().getInetAddress}")
            }
            processLine()
          case None =>
        }
      }
      processLine()

    }
  }

  private def parseLine(line: String, task: Task): Unit = {
    ProtocolParser.parse(line, task.currentState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case "PING"       =>  socketChannel.write(ByteBuffer.wrap("+PONG\r\n".getBytes))
          case "ECHO"       =>  socketChannel.write(ByteBuffer.wrap(("$"+value(1).length+"\r\n"+value(1) + "\r\n").getBytes))
          case "SET"        =>  handleSetCommand(value)
          case "GET"        =>  handleGetCommand(value(1))
          case "CONFIG"     =>  handleConfigGet(value)
          case "KEYS"       =>  handleKeysCommand(value)
          case "INFO"       =>  handleInfoCommand(value)
          case "REPLCONF"   =>  handleReplConfCommand(value)
          case "PSYNC"      =>  handlePSyncCommand(value)
          case "FULLRESYNC" =>  handleFullResync(value)
        }
      case Continue(nextState) =>
      //taskQueue.addTask(new Task(task.connection, nextState))
    }
  }


  private def handleFullResync(value: Vector[String]) = {
    println("*****************")
    println("FULL RESYNC")
    println("******************")
  }

  private def handlePSyncCommand(value: Vector[String]) = {
    val masterId = context.getMasterId
    socketChannel.write(ByteBuffer.wrap(s"+FULLRESYNC $masterId ${context.getMasterReplOffset}\r\n".getBytes))

    socketChannel.write(ByteBuffer.wrap("$88\r\n".getBytes))
    val byesFile = Files.readAllBytes(Paths.get("empty.rdb"))
    socketChannel.write(ByteBuffer.wrap(byesFile))

    replicaChannels += socketChannel
    println(s"New replica connected: ${socketChannel.socket().getInetAddress}")
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
    val keys = inMemoryDB.keys()
    val responseMsg = buildKeyCommand(keys)
    val fullMsg = ByteBuffer.wrap(responseMsg.getBytes)
    socketChannel.write(fullMsg)
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
    val value : Option[String] = inMemoryDB.get(key)
    value match {
      case Some(v) => socketChannel.write(ByteBuffer.wrap(("$" + v.length + "\r\n" + v + "\r\n").getBytes))
      case None => socketChannel.write(ByteBuffer.wrap("$-1\r\n".getBytes))
    }
  }

  private def handleSetCommand(value: Vector[String]) = {
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
    propagateToReplicas(value)
  }

  private def propagateToReplicas(value:  Vector[String]) = {
    val command = buildSetCommand(value)
    val buffer = ByteBuffer.wrap(command.getBytes)

    val failedChannels = mutable.ArrayBuffer[SocketChannel]()

    for (ch <- replicaChannels) {
      try {
        ch.write(buffer)
        buffer.rewind()
      } catch {
        case e : IOException =>
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

}



object Connection {
  def apply(socketChannel: SocketChannel, initialTask: Task, context: Context) : Connection = {
    val connection = Connection(socketChannel, context)
    connection.addTask(initialTask)
    connection
  }
}

