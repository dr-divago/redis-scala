package codecrafters_redis.eventloop

import codecrafters_redis.config.Context
import codecrafters_redis.db.{ExpiresIn, NeverExpires}
import codecrafters_redis.protocol._

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.ConcurrentHashMap

class EventLoop(context: Context) {
  private val taskQueue: TaskQueue = TaskQueue()
  private val clientBuffers = new ConcurrentHashMap[SocketChannel, StringBuilder]()
  private val inMemoryDB = context.getDB

  def start(): Unit = {
    val serverSocket = ServerSocketChannel.open()
    val selector = Selector.open()
    serverSocket.configureBlocking(false)
    serverSocket.bind(new InetSocketAddress("localhost", context.getPort))
    serverSocket.register(selector, SelectionKey.OP_ACCEPT)
    while (true) {
      if (selector.select() > 0) {
        val keys = selector.selectedKeys()
        val iterator = keys.iterator()
        while (iterator.hasNext) {
          iterator.next() match {
            case key if key.isAcceptable => acceptClient(selector, key)
            case key if key.isReadable => readData(key)
          }
          iterator.remove()
        }
      }
    }
  }

  private def acceptClient(selector: Selector, key: SelectionKey): Unit = {
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT: ${client.socket().getInetAddress}")
    client.configureBlocking(false)
    client.register(selector, SelectionKey.OP_READ)
    taskQueue.addTask(new Task(client.socket(), WaitingForCommand()))
  }

  private def readData(key: SelectionKey): Unit = {
    val buffer = ByteBuffer.allocate(1024)
    val client = key.channel().asInstanceOf[SocketChannel]
    try {
      val bytesRead = client.read(buffer)
      if (bytesRead == -1) {
        client.close()
        key.cancel()
      }
      else if (bytesRead > 0) {
        buffer.flip()
        val data = new String(buffer.array(), 0, buffer.limit())

        val clientBuffer = clientBuffers.computeIfAbsent(client, _ => new StringBuilder())
        clientBuffer.append(data)

        var endLineIndex = clientBuffer.indexOf("\r\n")
        while (endLineIndex != -1) {
          val line = clientBuffer.substring(0, endLineIndex)
          clientBuffer.delete(0, endLineIndex + 2)

          taskQueue.nextTask(client.socket()) match {
            case Some(task) => parseLine(client, line, task)
            case None => println(s"No task found for client ${client.socket().getInetAddress}")
          }
          endLineIndex = clientBuffer.indexOf("\r\n")
        }
      }
    } catch {
      case _: IOException =>
        client.close()
        key.cancel()
    } finally {
      buffer.clear()
    }
  }

  private def parseLine(client: SocketChannel, line: String, task: Task): Unit = {
    taskQueue.removeFirstTask(client.socket())
    ProtocolParser.parse(line, task.currentState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case "PING"     =>  client.write(ByteBuffer.wrap("+PONG\r\n".getBytes))
          case "ECHO"     =>  client.write(ByteBuffer.wrap(("$"+value(1).length+"\r\n"+value(1) + "\r\n").getBytes))
          case "SET"      =>  handleSetCommand(client, value)
          case "GET"      =>  handleGetCommand(client, value(1))
          case "CONFIG"   =>  handleConfigGet(client, value)
          case "KEYS"     =>  handleKeysCommand(client, value)
          case "INFO"     =>  handleInfoCommand(client, value)
          case "REPLCONF" =>  handleReplConfCommand(client, value)
          case "PSYNC"    =>  handlePSyncCommand(client, value)
        }
        taskQueue.addTask(new Task(task.socket, nextState))
      case Continue(nextState) => taskQueue.addTask(new Task(task.socket, nextState))
    }
  }

  private def handlePSyncCommand(client: SocketChannel, value: Vector[String]) = {
    val masterId = context.getMasterId
    client.write(ByteBuffer.wrap(s"+FULLRESYNC $masterId ${context.getMasterReplOffset}\r\n".getBytes))
  }

  private def handleReplConfCommand(client: SocketChannel, value: Vector[String]) = {
    client.write(ByteBuffer.wrap("+OK\r\n".getBytes))
  }

  private def handleInfoCommand(client: SocketChannel, value: Vector[String]) = {
    val role = context.getReplication
    val masterId = context.getMasterIdStr
    val replicationId = s"master_repl_offset:${context.getMasterReplOffset}"
    val allResp = s"${role}\n${masterId}\n$replicationId\n"
    val resp = s"$$${allResp.length}\r\n$allResp\r\n"

    client.write(ByteBuffer.wrap(resp.getBytes))
  }

  private def handleKeysCommand(client: SocketChannel, value: Vector[String]) = {
    val keys = inMemoryDB.keys()
    val responseMsg = buildKeyCommand(keys)
    val fullMsg = ByteBuffer.wrap(responseMsg.getBytes)
    client.write(fullMsg)
  }

  private def buildKeyCommand(keys : Iterable[String]): String = {
    val sizeOfArrayResponse = s"*${keys.size}\r\n"
    val elements = keys.map(key => s"$$${key.length}\r\n${key}\r\n").mkString
    sizeOfArrayResponse + elements.mkString
  }

  private def handleConfigGet(client: SocketChannel, value: Vector[String]) = {
    val conf = value(2) match {
      case "dir" => context.config.dirParam
      case "dbfilename" => context.config.dbParam
    }
    client.write(ByteBuffer.wrap(("*2\r\n$"+value(2).length+"\r\n"+value(2)+"\r\n$"+conf.length+"\r\n"+conf+"\r\n").getBytes))

  }

  private def handleGetCommand(client: SocketChannel, key: String): Unit = {
    val value : Option[String] = inMemoryDB.get(key)
    value match {
      case Some(v) => client.write(ByteBuffer.wrap(("$" + v.length + "\r\n" + v + "\r\n").getBytes))
      case None => client.write(ByteBuffer.wrap("$-1\r\n".getBytes))
    }
  }

  private def handleSetCommand(client: SocketChannel, value: Vector[String]) = {
    value.length match {
      case 3 =>
        inMemoryDB.add(value(1), value(2), NeverExpires())
        client.write(ByteBuffer.wrap("+OK\r\n".getBytes))
      case 5 =>
        val expireAt = value(3)
        val milliseconds = value(4)
        client.write(ByteBuffer.wrap("+OK\r\n".getBytes))
        inMemoryDB.add(value(1), value(2), ExpiresIn(milliseconds.toLong))
    }
  }
}
