package codecrafters_redis.server

import codecrafters_redis.command.{ConnectionEstablished, Psync}
import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.{Connection, EventLoop, EventSource, NIOEventSource, ProtocolManager, ReplicationState}
import codecrafters_redis.server.processor.{EventProcessor, ResultHandler}

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.collection.concurrent.TrieMap
import scala.util.Success

case class ServerContext(connections: TrieMap[SocketChannel, Connection])

trait ServerOperations {
  def handleNewClient(clientChannel: SocketChannel, key: SelectionKey): EventResult
  def handleConnectionReadable(connection: Connection): EventResult
  def handleConnectionEstablished(connection: Connection): EventResult
}

sealed trait ServerMode {
  def resultHandler: ResultHandler
}
case class MasterMode(context: Context) extends ServerMode {
  override def resultHandler: ResultHandler = new MasterResultHandler(context)
}

case class ReplicaMode(replicationState: ReplicationState) extends ServerMode {
  override def resultHandler: ResultHandler = new ReplicaResultHandler(replicationState)
}

class MasterResultHandler(context: Context) extends ResultHandler {
  override def handle(result: EventResult): Unit = {
    result match {
      case DataReceived(connection, data) =>
        println(s"Received data on master: $data")
        val dataStr = new String(data)
        val commandOpt = connection.process(dataStr)

        println(s"COMMAND : $commandOpt")

        commandOpt.head match {
          case Psync =>
            commandOpt.foreach { cmd => connection.write(cmd.execute(context)) }
            connection.write(Files.readAllBytes(Paths.get("empty.rdb")))
            context.replicaChannels :+= connection.socketChannel
          case _ => commandOpt.foreach { cmd => connection.write(cmd.execute(context)) }
        }
      case NoDataReceived(connection) => println(s"No data for $connection")
      case ConnectionClosed(connection) =>
        connection.close()
        println(s"Connection closed for $connection")
      case ConnectionAccepted(connection) => println(s"Connection accepted for $connection")
    }
  }
}

class ReplicaResultHandler(replicationState: ReplicationState) extends ResultHandler {
  override def handle(result: EventResult): Unit = {
    result match {
      case DataReceived(connection, data) => println(s"Received data on replica: $data")
      case _ =>
    }
  }
}

case class RedisServer(context: Context, var mode: ServerMode, eventSource: EventSource) extends ServerOperations {
  private val serverContext = ServerContext(connections = TrieMap[SocketChannel, Connection]())
  private val processor = new RedisEventProcessor()
  private val handler = mode match {
    case MasterMode(context) => new MasterResultHandler(context)
    case ReplicaMode(state) => new ReplicaResultHandler(state)
  }

  private val eventLoop: EventLoop = eventSource.subscribe(processor, handler)

  def start(): Unit = {
    eventLoop.start()
  }

  override def handleNewClient(clientChannel: SocketChannel, key: SelectionKey): EventResult = {
    val connection = Connection(clientChannel, key)
    serverContext.connections.put(clientChannel, connection)
    ConnectionAccepted(connection)
  }

  override def handleConnectionReadable(connection: Connection): EventResult = {
    connection.readIntoBuffer() match {
      case Success(bytesRead) if bytesRead > 0 =>
          connection.extractBytesFromBuffer() match {
            case Some(data) => DataReceived(connection, data)
            case None => NoDataReceived(connection)
          }
      case Success(0) => NoDataReceived(connection)
      case Success(-1) => ConnectionClosed(connection)
      case _ => ConnectionClosed(connection)
    }
  }

  override def handleConnectionEstablished(connection: Connection): EventResult = {
    connection.finishConnectOnChannel() match {
      case Success(true) =>
        mode match {
          case ReplicaMode(state) if state.context.connection.socketChannel.eq(connection.socketChannel) =>
            val (newState, actions) = ProtocolManager.processEvent(state, List(ConnectionEstablished))
            ProtocolManager.executeAction(actions, newState.context)
            mode = ReplicaMode(newState)
          case _ => println("Not a replica")
        }
        ConnectionAccepted(connection)
      case Success(false) => ConnectionClosed(connection)
      case _ => ConnectionClosed(connection)
    }
  }


  private class RedisEventProcessor extends EventProcessor {
    def process(event: SocketEvent): EventResult = event match {
      case AcceptEvent(key) =>
        val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
        val clientChannel = serverChannel.accept()
        if (clientChannel != null) {
          clientChannel.configureBlocking(false)
          clientChannel.register(key.selector(), SelectionKey.OP_READ)
          handleNewClient(clientChannel, key)
        } else {
          println("No client channel")
          Completed
        }
      case ReadEvent(key) =>
        val connection = serverContext.connections.get(key.channel().asInstanceOf[SocketChannel])
        if (connection.isDefined) {
          handleConnectionReadable(connection.get)
        } else {
          println("No connection found")
          Completed
        }
      case ConnectEvent(key) =>
        val connection = serverContext.connections.get(key.channel().asInstanceOf[SocketChannel])
        if (connection.isDefined) {
          handleConnectionEstablished(connection.get)
        } else {
          Completed
        }
    }
  }
}

object RedisServer {
  def apply(context: Context): RedisServer = {

    val serverContext = ServerContext(connections = TrieMap[SocketChannel, Connection]())

    val selector = Selector.open()
    val serverSocket = ServerSocketChannel.open()
    serverSocket.configureBlocking(false)
    serverSocket.bind(new InetSocketAddress("localhost", context.getPort))
    serverSocket.register(selector, SelectionKey.OP_ACCEPT)

    val eventSource = NIOEventSource(selector)
    val initMode: ServerMode = if (context.isReplica) {
      val replicationState = createReplicationState(
        context.config,
        selector,
        serverContext
      )
      ReplicaMode(replicationState)
    }
    else {
      MasterMode(context)
    }
    new RedisServer(context, initMode, eventSource)
  }

  private def createReplicationState(config: Config, selector: Selector, serverContext: ServerContext) = {
    val masterIpPort = config.replicaof.split(" ")
    val masterChannel = SocketChannel.open()
    masterChannel.configureBlocking(false)

    println(s"Connecting to master with ip ${masterIpPort(0)} port ${masterIpPort(1).toInt}")

    val connected = masterChannel.connect(new InetSocketAddress(masterIpPort(0), masterIpPort(1).toInt))

    val key = if (connected) {
      println("connected to server immediately")
      masterChannel.register(selector, SelectionKey.OP_READ)
    } else {
      println("Not connected yet, registering for OP_CONNECT")
      masterChannel.register(selector, SelectionKey.OP_CONNECT)
    }


    val connectionToMaster = Connection(masterChannel,key)
    serverContext.connections.addOne(masterChannel, connectionToMaster)

    val initialState = ProtocolManager(connectionToMaster)

    if (connected) {
      println("connected to server")
      val (newState, actions) = ProtocolManager.processEvent(initialState, List(ConnectionEstablished))
      ProtocolManager.executeAction(actions, newState.context)
      newState
    }
    else {
      println("Not connected yet")
      initialState
    }
  }
}