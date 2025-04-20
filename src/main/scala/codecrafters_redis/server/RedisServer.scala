package codecrafters_redis.server

import codecrafters_redis.command.{ConnectionEstablished, Psync, RdbDataReceived}
import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.{Connection, EventLoop, ParseRDBFile, ProtocolManager, ReplicationState}

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.collection.concurrent.TrieMap

case class ServerContext(connections: TrieMap[SocketChannel, Connection])

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
    def setupReplication(config: Config, selector: Selector): ReplicationState = {
      val masterIpPort = config.replicaof.split(" ")
      val masterChannel = SocketChannel.open()
      masterChannel.configureBlocking(false)


      println(s"Connecting to master with ip ${masterIpPort(0)} port ${masterIpPort(1).toInt}")

      val connected = masterChannel.connect(new InetSocketAddress(masterIpPort(0), masterIpPort(1).toInt))

      val connectionToMaster = Connection(masterChannel)
      connections.addOne(masterChannel, connectionToMaster)

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
}


case class RedisServer(mode: ServerMode, eventLoop: EventLoop) {
  private val serverContext = ServerContext(
    connections = TrieMap[SocketChannel, Connection]()
  )

  val eventProcessor: EventProcessor = new EventProcessor {
    override def processEvent(event: SocketEvent): EventResult = event match {
      case AcceptEvent(key) => acceptClient(key)
      case ReadEvent(key) => readData(key)
      case ConnectEvent(key) => connectClient(key)
      case WriteEvent(key) => writeData(key)
    }

    override def handleResult(result: EventResult): Unit = {
      result match {
        case ConnectionAccepted(connection) =>
          connections.addOne(connection.socketChannel, connection)
        case DataReceived(connection, data) =>
          println(s"Data received : ${new String(data)}")
          processReceivedData(connection, data)
        case ConnectionClosed(channel) =>
          connections.remove(channel)
        case NoDataReceived(_) =>
          println("No data")
      }
    }
  }

  private def writeData(key: SelectionKey) : EventResult = ???

  private def connectClient(key: SelectionKey): EventResult = ???

  private def processReceivedData(connection: Connection, data: Array[Byte]) : EventResult = {
    mode match {
      case MasterMode => processMasterData(connection, data)
      case ReplicaMode(state) => processReplicaData(connection, data, state)
    }
  }

  private def processReplicaData(connection: Connection, data: Array[Byte], state: ReplicationState): EventResult = {
    Completed
  }

  private def processMasterData(connection: Connection, data: Array[Byte]): EventResult = {
    val commandOpt = connection.process(new String(data))

    commandOpt.head match {
      case Psync =>
        commandOpt.foreach { cmd => connection.write(cmd.execute(eventLoop.context)) }
        connection.write(Files.readAllBytes(Paths.get("empty.rdb")))
        eventLoop.context.replicaChannels :+= connection.socketChannel
      case _ => commandOpt.foreach { cmd => connection.write(cmd.execute(eventLoop.context)) }
    }
    Completed
  }

  private def readData(key: SelectionKey): EventResult = {
    connections.get(key.channel().asInstanceOf[SocketChannel]) match {
      case Some(connection) =>
        val dataReceived = connection.readData(key)
        DataReceived(connection, dataReceived)
        case None =>
        NoDataReceived(key.channel().asInstanceOf[SocketChannel])
    }
  }

  private def acceptClient(key: SelectionKey): EventResult = {
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT IP : ${client.socket().getInetAddress} PORT: ${client.socket().getPort}")
    client.configureBlocking(false)
    client.register(key.selector(), SelectionKey.OP_READ)
    val conn = Connection(client)
    ConnectionAccepted(conn)
  }
}

object RedisServer {
  def apply(context: Context): RedisServer = {

    val serverContext = ServerContext(
      connections = TrieMap[SocketChannel, Connection]()
    )

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
    new RedisServer(initMode, new EventLoop(context))
  }

  private def createReplicationState(config: Config, selector: Selector, serverContext: ServerContext) = ???
}