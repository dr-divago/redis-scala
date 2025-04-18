package codecrafters_redis.server

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.{Connection, EventLoop, ReplicationState}

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, ServerSocketChannel, SocketChannel}
import scala.collection.concurrent.TrieMap

sealed trait ServerMode
case object MasterMode extends ServerMode
case class ReplicaMode(replicationState: ReplicationState) extends ServerMode {


}


case class RedisServer(mode: ServerMode, eventLoop: EventLoop) {
  private var connections = TrieMap[SocketChannel, Connection]()

  val eventProcessor: EventProcessor = {
    case AcceptEvent(key) => acceptClient(key)
    case ReadEvent(key) => readData(key)
    case ConnectEvent(key) => connectClient(key)
    case WriteEvent(key) => writeData(key)
  }

  private def writeData(key: SelectionKey) : EventResult = ???

  private def connectClient(key: SelectionKey): EventResult = ???

  private def processReceivedData(channel: SocketChannel, data: ByteBuffer) : EventResult = ???

  private def readData(key: SelectionKey): EventResult = ???

  private def acceptClient(key: SelectionKey): EventResult = {
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT IP : ${client.socket().getInetAddress} PORT: ${client.socket().getPort}")
    client.configureBlocking(false)
    client.register(key.selector(), SelectionKey.OP_READ)
    val conn = Connection(client)
    ConnectionAccepted(conn)
    //connections.addOne(client, connection)
  }
}

object RedisServer {
  def apply(context: Context): RedisServer = {
    if (context.isReplica) {

    }
    new RedisServer(MasterMode, new EventLoop(context))

  }
}