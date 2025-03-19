package codecrafters_redis.eventloop

import codecrafters_redis.command.{ConnectionEstablished, Psync}
import codecrafters_redis.config.{Config, Context}

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
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

  private def mainEventLoop(selector: Selector, initialReplicationState: Option[ReplicationState]): Unit = {
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
              println(s"EVENT LOOP $replicationState")
            case key if key.isConnectable =>
              replicationState = connectClient(key, replicationState)
          }
          iterator.remove()
        }
      }
    }
  }

  private def connectClient(key: SelectionKey, replicationState: Option[ReplicationState]): Option[ReplicationState] = {
    val channel = key.channel().asInstanceOf[SocketChannel]

    val isMasterChannel = replicationState.exists(_.context.masterChannel.contains(channel))

    if (channel.finishConnect()) {
      if (isMasterChannel) {
        println("Connected to master")
        replicationState.map { state =>
          val (newState, actions) = ProtocolManager.processEvent(state, List(ConnectionEstablished))
          ProtocolManager.executeAction(actions, newState.context)
          key.interestOps(SelectionKey.OP_READ)
          val connectionToMaster = Connection(newState.context.masterChannel.get, context)
          connections.addOne(newState.context.masterChannel.get, connectionToMaster)
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
      val (newState, actions) = ProtocolManager.processEvent(initialState, List(ConnectionEstablished))
      ProtocolManager.executeAction(actions, newState.context)
      masterChannel.register(selector, SelectionKey.OP_READ)
      val connectionToMaster = Connection(masterChannel, context)
      connections.addOne(masterChannel, connectionToMaster)
      newState
    }
    else {
      println("Not connected yet")
      masterChannel.register(selector, SelectionKey.OP_CONNECT)
      initialState
    }
  }

  private def acceptClient(key: SelectionKey): Unit = {
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT IP : ${client.socket().getInetAddress} PORT: ${client.socket().getPort}")
    client.configureBlocking(false)
    client.register(key.selector(), SelectionKey.OP_READ)
    val connection = Connection(client, context)
    connections.addOne(client, connection)
  }

  private def readData(key: SelectionKey, replicationState: Option[ReplicationState]): Option[ReplicationState] = {
    val client = key.channel().asInstanceOf[SocketChannel]

    connections.get(client) match {
      case Some(connection) if replicationState.exists(rs => rs.isMasterConnection(client) && !rs.isHandshakeDone) =>
        println(s"**** REPLICATION HANDSHAKE DONE = ${replicationState.get.isHandshakeDone}")
        val data  = connection.readData(key)
        println(s"READ DATA from socket ${data.length}")
        val dataStr = new String(data)
        val events = connection.processResponse(dataStr)
        println(s"EVENTS RECEIVED $events")

        val (newState, actions) = ProtocolManager.processEvent(replicationState.get, events)
        ProtocolManager.executeAction(actions, newState.context)
        Some(newState)

      case Some(connection) =>
        println("****NORMAL CONNECTION FLOW****")
        val data = connection.readData(key)
        if (data.nonEmpty) {
          val dataStr = new String(data)
          val commandOpt = connection.process(dataStr)

          commandOpt.head match {
            case Psync =>
              commandOpt.foreach { cmd => connection.write(cmd.execute(context)) }
              connection.write(Files.readAllBytes(Paths.get("empty.rdb")))
              context.replicaChannels :+= connection.socketChannel
            case _ => commandOpt.foreach { cmd => connection.write(cmd.execute(context)) }
          }
        }
        replicationState

      case None =>
        println("No connection found")
        val connection = Connection(client, context)
        connections = connections.addOne(client, connection)
        replicationState
    }
  }
}