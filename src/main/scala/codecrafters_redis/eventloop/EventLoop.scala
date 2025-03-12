package codecrafters_redis.eventloop

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.protocol._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import scala.collection.concurrent.TrieMap

class EventLoop(context: Context) {
  private var connections = TrieMap[SocketChannel, Connection]()

  def start(): Unit = {

    val selector = Selector.open()
    val serverSocket = ServerSocketChannel.open()
    serverSocket.configureBlocking(false)
    serverSocket.bind(new InetSocketAddress("localhost", context.getPort))
    serverSocket.register(selector, SelectionKey.OP_ACCEPT)

    val initialReplicationState =
      if (context.config.replicaof.nonEmpty) {
        println("Replica")
        Some(setupReplication(context.config, selector))
      }
      else {
        None
      }

    mainEventLoop(selector, initialReplicationState)

  }

  private def mainEventLoop(selector: Selector, initialReplicationState : Option[ReplicationState]): Unit = {
    var replicationState = initialReplicationState
    while (true) {
      if (selector.select() > 0) {
        val keys = selector.selectedKeys()
        val iterator = keys.iterator()
        while (iterator.hasNext) {
          iterator.next() match {
            case key if key.isAcceptable => acceptClient(key)
            case key if key.isReadable =>
              replicationState = readData(key, replicationState)
            case key if key.isConnectable =>
              replicationState = connectClient(key, replicationState)
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

  private def connectClient(key: SelectionKey, replicationState: Option[ReplicationState]): Option[ReplicationState] = {
    val channel = key.channel().asInstanceOf[SocketChannel]

    val isMasterChannel = replicationState.exists(_.context.masterChannel.contains(channel))

    if (channel.finishConnect()) {
      if (isMasterChannel) {
        println("Connected to master")
        replicationState.map { state =>
          val (newState, action) = ProtocolManager.processEvent(state, ConnectionEstablished)
          println(s"Context port ${newState.context.masterChannel.get.socket().getPort}")
          ProtocolManager.executeAction(action, newState.context)
          key.interestOps(SelectionKey.OP_READ)
          newState
        }
      } else {
        println("Connected to some other client")
        key.interestOps(SelectionKey.OP_READ)
        replicationState
      }
    }
    else {
      replicationState
    }
  }

  private def setupReplication(config: Config, selector: Selector): ReplicationState = {
    val masterIpPort = config.replicaof.split(" ")
    val masterChannel = SocketChannel.open()
    masterChannel.configureBlocking(false)

    println(s"Connecting to master with ip ${masterIpPort(0)} port ${masterIpPort(1).toInt}")

    val connected = masterChannel.connect(new InetSocketAddress(masterIpPort(0), masterIpPort(1).toInt))
    val initialState = ProtocolManager(masterChannel, context.getPort)


    if (connected) {
      println("connected to server")
      val (newState, action) = ProtocolManager.processEvent(initialState, ConnectionEstablished)
      ProtocolManager.executeAction(action, newState.context)
      masterChannel.register(selector, SelectionKey.OP_READ)
      newState
    }
    else {
      println("Not connected yet")
      masterChannel.register(selector, SelectionKey.OP_CONNECT)
      initialState
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
    println("acceptClient")
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT: ${client.socket().getInetAddress}")
    client.configureBlocking(false)
    client.register(key.selector(), SelectionKey.OP_READ)
    val connection = Connection(client, new Task(WaitingForCommand()), context)
    connections.addOne(client, connection)
  }

  private def readData(key: SelectionKey, replicationState: Option[ReplicationState]): Option[ReplicationState] = {
    val client = key.channel().asInstanceOf[SocketChannel]

    val isMasterChannel = replicationState.exists(_.context.masterChannel.contains(client))
    if (isMasterChannel) {
      val buffer = ByteBuffer.allocate(1024)
      val bytesRead = client.read(buffer)

      if (bytesRead > 0) {
        buffer.flip()
        val response = new String(buffer.array(), 0, buffer.limit())
        println(s"Received from master: $response")

        replicationState.map{ state =>
          val (newState, action) = ProtocolManager.processEvent(state,ResponseReceived(response))
          ProtocolManager.executeAction(action, newState.context)
          newState
        }
      } else if (bytesRead < 0) {
        println("Master connection closed")
        key.cancel()
        client.close()

        replicationState.map { state =>
          val (newState, _) = ProtocolManager.processEvent(state, ConnectionClosed)
          newState
        }
      } else {
        replicationState
      }
    } else {
      connections.get(client) match {
        case Some(connection) => connection.readDataFromClient(key)
        case None =>
          println("No connection found")
          val connection = Connection(client, new Task(WaitingForCommand()), context)
          connections = connections.addOne(client, connection)
      }
      replicationState
    }
  }
}
