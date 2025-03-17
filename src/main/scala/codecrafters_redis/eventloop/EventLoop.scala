package codecrafters_redis.eventloop

import codecrafters_redis.command.{Command, Get, Set}
import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.db.{ExpiresIn, NeverExpires}
import codecrafters_redis.protocol._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class EventLoop(context: Context) {
  private var connections = TrieMap[SocketChannel, Connection]()
  private val replicaChannels = mutable.ArrayBuffer[SocketChannel]()

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
              println(s"EVENT LOOP ${replicationState}")
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
          val (newState, action) = ProtocolManager.processEvent(state, ConnectionEstablished)
          println(s"Context port ${newState.context.masterChannel.get.socket().getPort}")
          ProtocolManager.executeAction(action, newState.context)
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
      val (newState, action) = ProtocolManager.processEvent(initialState, ConnectionEstablished)
      ProtocolManager.executeAction(action, newState.context)
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
    println("acceptClient")
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT: ${client.socket().getInetAddress}")
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
        val data : String = connection.readData(key)
        val commandOpt = connection.process(data)

        commandOpt.foreach { cmd => connection.write(cmd.execute())}
        replicationState

      case Some(connection) =>
        println("****NORMAL CONNECTION FLOW****")
        val data : String = connection.readData(key)
        val commandOpt = connection.process(data)

        /*
        commandOpt match {
          case Some(Set(key, value, expiry)) if expiry.isDefined => context.getDB.add(key, value, ExpiresIn(expiry.get))
          case Some(Set(key, value, _)) => context.getDB.add(key, value, NeverExpires())
          case Some(Get(value)) => context.getDB.get(value)
          case _ =>
        }

         */

        commandOpt.foreach { cmd => connection.write(cmd.execute(context.getDB))}
        replicationState
        //connection.readDataFromClient(key, replicaChannels)

      case None =>
        println("No connection found")
        val connection = Connection(client, context)
        connections = connections.addOne(client, connection)
        replicationState
    }
  }

  private def handleHandshakeCompletion(connection: Connection): Unit = {
    println("Handshake completed - transitioning to normal operation mode")
    // Reset the connection's state for normal operation
  }
}
