package codecrafters_redis.server.processor

import codecrafters_redis.eventloop.EventLoop
import codecrafters_redis.server.SocketEvent

import java.nio.channels.{SelectionKey, Selector}

trait EventSource {
  def subscribe(processor: EventProcessor, handler: ResultHandler): EventLoop
}

trait EventLoop {
  def start(): Unit
}

class SelectorEventSource(selector: Selector) extends EventSource {
  def subscribe(processor: EventProcessor, handler: ResultHandler): EventLoop = {
    new SelectorEventLoop(selector, processor, handler)
  }

  private class SelectorEventLoop(
                                   selector: Selector,
                                   processor: EventProcessor,
                                   handler: ResultHandler
                                 ) extends EventLoop {
    def start(): Unit = {
      while (true) {
        if (selector.select() > 0) {
          val keys = selector.selectedKeys()
          val iterator = keys.iterator()
          while (iterator.hasNext) {
            val key = iterator.next()
            val event = keyToEvent(key)
            val result = processor.process(event)
            handler.handle(result)
            iterator.remove()
          }
        }
      }
    }

    private def keyToEvent(key: SelectionKey): SocketEvent = {
      // Convert key to event
    }
  }
}
