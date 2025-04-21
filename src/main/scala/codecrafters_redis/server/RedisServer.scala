package codecrafters_redis.server

import codecrafters_redis.command.ConnectionEstablished
import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.{Connection, FunctionalEventLoop, ProtocolManager, ReplicationState}
import codecrafters_redis.server.processor.{EventLoop, EventSource, RedisEventProcessor, RedisResultHandler}

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import scala.collection.concurrent.TrieMap
import scala.util.Success

case class ServerContext(connections: TrieMap[SocketChannel, Connection])

trait ServerOperations {
  def handleNewClient(clientChannel: SocketChannel): EventResult
  def handleConnectionReadable(connection: Connection): EventResult
  def handleConnectionEstablished(connection: Connection): EventResult
  def processData(connection: Connection, data: Array[Byte]): EventResult
  def handleConnectionClosed(connection: Connection): EventResult
  def getConnections: TrieMap[SocketChannel, Connection]
}

trait RedisServerBehaviour {

}
sealed trait ServerMode {
  def behaviour : RedisServerBehaviour
}
case object MasterMode extends ServerMode {
  override def behaviour: RedisServerBehaviour = new RedisServerBehaviour {

  }
}
case class ReplicaMode(replicationState: ReplicationState) extends ServerMode {
  override def behaviour: RedisServerBehaviour = new RedisServerBehaviour {

  }
}




case class RedisServer(var mode: ServerMode, eventSource: EventSource) extends ServerOperations {
  private val serverContext = ServerContext(connections = TrieMap[SocketChannel, Connection]())
  private val processor = new RedisEventProcessor()
  private val handler = new RedisResultHandler()

  private val eventLoop: EventLoop = eventSource.subscribe(processor, handler)

  def start(): Unit = {
    eventLoop.start()
  }

  override def getConnections: TrieMap[SocketChannel, Connection] = serverContext.connections

  override def handleNewClient(clientChannel: SocketChannel): EventResult = {
    val connection = Connection(clientChannel)
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

  override def processData(connection: Connection, data: Array[Byte]): EventResult =
    mode match {
      case MasterMode => Completed
      case ReplicaMode(state) => Completed
    }

  override def handleConnectionClosed(connection: Connection): EventResult =
    mode match {
      case ReplicaMode(state) if state.context.connection.socketChannel.eq(connection.socketChannel) =>
        mode = MasterMode
        ConnectionAccepted(connection)
      case _ => ConnectionClosed(connection)
    }

  lazy val eventProcessor  : EventProcessor = {
    new EventProcessor {
      override def processEvent(event: SocketEvent): EventResult = event match {
        case AcceptEvent(key) =>
          val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
          val clientChannel = serverChannel.accept()
          if (clientChannel != null) {
            clientChannel.configureBlocking(false)
            clientChannel.register(key.selector(), SelectionKey.OP_READ)
            handleNewClient(clientChannel)
          } else {
            println("No client channel")
            Completed
          }
        case ReadEvent(key) =>
          val connection = getConnections.get(key.channel().asInstanceOf[SocketChannel])
          if (connection.isDefined) {
            handleConnectionReadable(connection.get)
          } else {
            println("No connection found")
            Completed
          }
        case ConnectEvent(key) =>
          val connection = getConnections.get(key.channel().asInstanceOf[SocketChannel])
          if (connection.isDefined) {
            handleConnectionEstablished(connection.get)
          } else {
            Completed
          }
      }



      override def handleResult(result: EventResult): Unit = {
        result match {
          case ConnectionAccepted(connection) => getConnections.addOne(connection.socketChannel, connection)
          case DataReceived(connection, data) => println("Process data")
          case ConnectionClosed(channel) => println("Connection closed")
          case NoDataReceived(_) => println("No data")
        }
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

    val initMode = if (context.isReplica) {
      val replicationState = createReplicationState(
        context.config,
        selector,
        serverContext
      )
      ReplicaMode(replicationState)
    }
    else {
      MasterMode
    }
    new RedisServer(initMode, new FunctionalEventLoop(selector, eventProcessor))
  }

  private def createReplicationState(config: Config, selector: Selector, serverContext: ServerContext) = {
    val masterIpPort = config.replicaof.split(" ")
    val masterChannel = SocketChannel.open()
    masterChannel.configureBlocking(false)

    println(s"Connecting to master with ip ${masterIpPort(0)} port ${masterIpPort(1).toInt}")

    val connected = masterChannel.connect(new InetSocketAddress(masterIpPort(0), masterIpPort(1).toInt))

    val connectionToMaster = Connection(masterChannel)
    serverContext.connections.addOne(masterChannel, connectionToMaster)

    val initialState = ProtocolManager(connectionToMaster)

    if (connected) {
      println("connected to server")
      val (newState, actions) = ProtocolManager.processEvent(initialState, List(ConnectionEstablished))
      ProtocolManager.executeAction(actions, newState.context)
      masterChannel.register(selector, SelectionKey.OP_READ)
      newState
    }
    else {
      println("Not connected yet")
      masterChannel.register(selector, SelectionKey.OP_CONNECT)
      initialState
    }
  }
}