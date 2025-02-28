package codecrafters_redis.eventloop

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

case class Connection(socketChannel: SocketChannel) {
val buffer: ByteBuffer = ByteBuffer.allocate(1024)
}

