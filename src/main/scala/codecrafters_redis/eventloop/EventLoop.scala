package codecrafters_redis.eventloop

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.db.{ExpiresIn, NeverExpires}
import codecrafters_redis.protocol._

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class EventLoop(context: Context) {
  private var connections = TrieMap[SocketChannel, Connection]()
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
    connection.addTask(new Task(WaitingForCommand()))
    connections.addOne(client, connection)
  }

  private def readData(key: SelectionKey): Unit = {
    val client = key.channel().asInstanceOf[SocketChannel]

    connections.get(client) match {
      case Some(connection) => connection.readDataFromClient(key)
      case None =>
        println("No connection found")
        val connection = Connection(client)
        connections = connections.addOne(client, connection)
    }



    /*
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

     */
  }










}
