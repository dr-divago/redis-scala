package codecrafters_redis.server.processor

import codecrafters_redis.server.{AcceptEvent, Completed, ConnectEvent, EventResult, ReadEvent, SocketEvent}

import java.nio.channels.{SelectionKey, ServerSocketChannel, SocketChannel}

trait EventProcessor {
  def process(event: SocketEvent): EventResult
}

class RedisEventProcessor extends EventProcessor {
  def process(event: SocketEvent): EventResult = event match {
      case AcceptEvent(key) =>
        val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
        val clientChannel = serverChannel.accept()
        if (clientChannel != null) {
          clientChannel.configureBlocking(false)
          clientChannel.register(key.selector(), SelectionKey.OP_READ)
          //handleNewClient(clientChannel)
        } else {
          println("No client channel")
          Completed
        }
      case ReadEvent(key) =>
        val connection = getConnections.get(key.channel().asInstanceOf[SocketChannel])
        if (connection.isDefined) {
          //handleConnectionReadable(connection.get)
        } else {
          println("No connection found")
          Completed
        }
      case ConnectEvent(key) =>
        val connection = getConnections.get(key.channel().asInstanceOf[SocketChannel])
        if (connection.isDefined) {
          //handleConnectionEstablished(connection.get)
        } else {
          Completed
        }
    }
}
