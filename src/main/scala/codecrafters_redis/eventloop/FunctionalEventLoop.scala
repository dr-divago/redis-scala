package codecrafters_redis.eventloop

import codecrafters_redis.server._

import java.nio.channels.{SelectionKey, Selector}

class FunctionalEventLoop(selector: Selector, processor: EventProcessor) {
  def start() : Unit = {
    while (true) {
      if (selector.select() > 0) {
        val keys = selector.selectedKeys()
        val iterator = keys.iterator()
        while (iterator.hasNext) {
          val keys = iterator.next()
          val event = keyToEvent(keys)
          processor.process(event)
          iterator.remove()
        }
      }
    }
  }

  private def keyToEvent(key: SelectionKey): SocketEvent = {
    key match {
      case key if key.isAcceptable => AcceptEvent(key)
      case key if key.isReadable => ReadEvent(key)
      case key if key.isConnectable => ConnectEvent(key)
      case key if key.isWritable => WriteEvent(key)
      case _ => throw new Exception("Unknown event")
    }
  }
}
