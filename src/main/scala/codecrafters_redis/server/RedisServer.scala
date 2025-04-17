package codecrafters_redis.server

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.{Connection, EventLoop, ReplicationState}

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
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

  private def acceptClient(key: SelectionKey): EventResult = ???
}

object RedisServer {
  def apply(context: Context): RedisServer = {
    if (context.isReplica) {

      new RedisServer(ReplicaMode())
    }

  }
}