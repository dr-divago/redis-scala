package codecrafters_redis.server

import codecrafters_redis.command.{Psync, RdbDataReceived}
import codecrafters_redis.config.Context
import codecrafters_redis.eventloop.{Connection, EventLoop, ParseRDBFile, ReplicationState}

import java.nio.channels.{SelectionKey, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.collection.concurrent.TrieMap

sealed trait ServerMode
case object MasterMode extends ServerMode
case class ReplicaMode(replicationState: ReplicationState) extends ServerMode {


}


case class RedisServer(mode: ServerMode, eventLoop: EventLoop) {
  private val connections = TrieMap[SocketChannel, Connection]()

  val eventProcessor = new EventProcessor {
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
        case NoDataReceived(channel) =>
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
    if (context.isReplica) {

    }
    new RedisServer(MasterMode, new EventLoop(context))

  }
}