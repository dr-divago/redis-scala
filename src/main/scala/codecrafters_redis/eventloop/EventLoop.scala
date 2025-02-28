package codecrafters_redis.eventloop

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.db.{ExpiresIn, NeverExpires}
import codecrafters_redis.protocol._

import java.io.{IOException, PrintWriter}
import java.net.{InetSocketAddress, Socket}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

class EventLoop(context: Context) {
  private val taskQueue: TaskQueue = TaskQueue()
  private var connections = List[Connection]()
  private val inMemoryDB = context.getDB
  private val replicaChannels = mutable.ArrayBuffer[SocketChannel]()

  def start(): Unit = {
    val selector = Selector.open()
    if (context.config.replicaof.nonEmpty) {
      println("Replica")
      setupReplication(context.config, selector)
    }
    else {
      val serverSocket = ServerSocketChannel.open()
      serverSocket.configureBlocking(false)
      serverSocket.bind(new InetSocketAddress("localhost", context.getPort))
      serverSocket.register(selector, SelectionKey.OP_ACCEPT)
    }

    mainEventLoop(selector)

  }

  private def mainEventLoop(selector: Selector): Unit = {
    while (true) {
      if (selector.select() > 0) {
        val keys = selector.selectedKeys()
        val iterator = keys.iterator()
        while (iterator.hasNext) {
          iterator.next() match {
            case key if key.isAcceptable => acceptClient(key)
            case key if key.isReadable => readData(key)
            case key if key.isConnectable => connectClient(key)
            case key if key.isWritable => writeData(key)
          }
          iterator.remove()
        }
      }
    }
  }

  private def writeData(key: SelectionKey) = {
    println("Write data")
  }

  private def connectClient(key: SelectionKey) = {
    val channel = key.channel().asInstanceOf[SocketChannel]

    if (channel.finishConnect()) {
      println("Connected to master")
      key.interestOps(SelectionKey.OP_WRITE)
    }

  }

  private def setupReplication(config: Config, selector: Selector): Unit = {
    val masterIpPort = config.replicaof.split(" ")
    val masterChannel = SocketChannel.open()
    masterChannel.configureBlocking(false)

    val connected = masterChannel.connect(new InetSocketAddress(masterIpPort(0), masterIpPort(1).toInt))

    if (connected) {
      println("connected to server")
      masterChannel.register(selector, SelectionKey.OP_WRITE)
    }
    else {
      println("Not connected yet")
      masterChannel.register(selector, SelectionKey.OP_CONNECT)
    }

    /*
    val masterSocket = new Socket(masterIpPort(0), masterIpPort(1).toInt)
    val port = config.port
    val out = new PrintWriter(masterSocket.getOutputStream)
    val in = masterSocket.getInputStream

    val buffer = ByteBuffer.allocate(17)

    out.write("*1\r\n$4\r\nPING\r\n")
    out.flush()
    var bytesRead = 0
    while (bytesRead < 7) {
      val count = in.read(buffer.array(), 0, 7)
      if (count == -1) throw new IOException("Connection closed")
      bytesRead += count
    }
    val pingResponse = new String(buffer.array(), 0, 7)
    if (!pingResponse.equals("+PONG\r\n")) {
      throw new Exception(s"Expected +PONG but got ${pingResponse} ${pingResponse.length}")
    }

    out.write(s"*3\r\n$$8\r\nREPLCONF\r\n$$14\r\nlistening-port\r\n$$${port.length}\r\n$port\r\n")
    out.flush()
    in.read(buffer.array(), 7, 5)
    val ok1Response = new String(buffer.array(), 7, 5)
    if (ok1Response != "+OK\r\n") {
      throw new Exception(s"Expected +OK but got ${ok1Response} ")
    }

    out.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    out.flush()
    in.read(buffer.array(), 12, 5)
    val ok2Response = new String(buffer.array(), 12, 5)
    if (ok2Response != "+OK\r\n") {
      throw new Exception(s"Expected +OK but go ${ok2Response}")
    }

    out.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
    out.flush()

    // Read FULLRESYNC response
    println("REPLICA INIT BUFFER")
    val fullResyncBuffer = ByteBuffer.allocate(1024)
    val fullResyncBytes = in.read(fullResyncBuffer.array())
    val fullResyncResponse = new String(fullResyncBuffer.array(), 0, fullResyncBytes)

    if (!fullResyncResponse.startsWith("+FULLRESYNC")) {
      throw new Exception(s"Expected FULLRESYNC but got ${fullResyncResponse}")
    }
    println("REPLICA READ FULLRESYNC")


    // Read RDB file
    // First read the RDB file size
    val rdbSizeBuffer = new StringBuilder()
    var char = in.read().toChar
    while (char != '\r') {
      rdbSizeBuffer.append(char)
      char = in.read().toChar
    }
    in.read() // consume \n

    val rdbSize = rdbSizeBuffer.toString().substring(1).toInt // Remove $ from size
    val rdbBuffer = ByteBuffer.allocate(rdbSize)
    var totalRead = 0
    while (totalRead < rdbSize) {
      val read = in.read(rdbBuffer.array(), totalRead, rdbSize - totalRead)
      if (read == -1) throw new Exception("Unexpected end of RDB file")
      totalRead += read
    }




    println("Handshake completed successfully, RDB file received")

    in.read(buffer.array(), 12, 5)
    val fullResyncResponse = new String(buffer.array(), 12, 5)
    if (ok2Response != "+OK\r\n") {
      throw new Exception(s"Expected +OK but go ${ok2Response}")
    }



     */



  }

  private def acceptClient(key: SelectionKey): Unit = {
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT: ${client.socket().getInetAddress}")
    client.configureBlocking(false)
    client.register(key.selector(), SelectionKey.OP_READ)
    val connection = Connection(client)
    taskQueue.addTask(new Task(connection, WaitingForCommand()))
  }

  private def readData(key: SelectionKey): Unit = {
    val client = key.channel().asInstanceOf[SocketChannel]

    connections.find(_.socketChannel == client) match {
      case Some(connection) => readDataFromClient(key, client, connection)
      case None =>
        println("No connection found")
        val connection = Connection(client)
        connections = connections.appended(connection)
    }

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

  private def readDataFromClient(key: SelectionKey, client: SocketChannel, connection: Connection) = {
    val buffer = connection.buffer
    buffer.compact()

    val byteRead = client.read(buffer)
    if (byteRead == -1) {
      client.close()
      key.cancel()
      connections.dropWhile(_.socketChannel == client)
    }
    else {
      buffer.flip()
    }
  }

  private def parseLine(client: SocketChannel, line: String, task: Task): Unit = {
    taskQueue.removeFirstTask(client.socket())
    ProtocolParser.parse(line, task.currentState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case "PING"       =>  client.write(ByteBuffer.wrap("+PONG\r\n".getBytes))
          case "ECHO"       =>  client.write(ByteBuffer.wrap(("$"+value(1).length+"\r\n"+value(1) + "\r\n").getBytes))
          case "SET"        =>  handleSetCommand(client, value)
          case "GET"        =>  handleGetCommand(client, value(1))
          case "CONFIG"     =>  handleConfigGet(client, value)
          case "KEYS"       =>  handleKeysCommand(client, value)
          case "INFO"       =>  handleInfoCommand(client, value)
          case "REPLCONF"   =>  handleReplConfCommand(client, value)
          case "PSYNC"      =>  handlePSyncCommand(client, value)
          case "FULLRESYNC" =>  handleFullResync(client, value)
        }
        taskQueue.addTask(new Task(task.connection, nextState))
      case Continue(nextState) => taskQueue.addTask(new Task(task.connection, nextState))
    }
  }

  private def handleFullResync(client: SocketChannel, value: Vector[String]) = {
    println("*****************")
    println("FULL RESYNC")
    println("******************")
  }

  private def handlePSyncCommand(client: SocketChannel, value: Vector[String]) = {
    val masterId = context.getMasterId
    client.write(ByteBuffer.wrap(s"+FULLRESYNC $masterId ${context.getMasterReplOffset}\r\n".getBytes))

    client.write(ByteBuffer.wrap("$88\r\n".getBytes))
    val byesFile = Files.readAllBytes(Paths.get("empty.rdb"))
    client.write(ByteBuffer.wrap(byesFile))

    replicaChannels += client
    println(s"New replica connected: ${client.socket().getInetAddress}")
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
    if (context.config.replicaof.nonEmpty) {
      println("I am in replica")
    }
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
