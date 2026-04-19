package codecrafters_redis.eventloop

import codecrafters_redis.Logger
import codecrafters_redis.eventloop.NIOEventSource.keyToEvent
import codecrafters_redis.server._
import codecrafters_redis.server.processor.{EventProcessor, ResultHandler}

import java.nio.channels.{SelectionKey, Selector}

trait EventSource {
  def subscribe(processor: EventProcessor, handler: ResultHandler): EventLoop
}

trait EventLoop {
  def start(): Unit
}

class NIOEventSource(selector: Selector) extends EventSource{

  def subscribe(processor: EventProcessor, handler: ResultHandler) : EventLoop = {
    new NioEventLoop(selector, processor, handler)
  }

  private class NioEventLoop(
                            selector: Selector,
                             processor: EventProcessor,
                             handler: ResultHandler
                            ) extends EventLoop {
    def start(): Unit = {
      while (true) {
        if (selector.select(1000) > 0) {
          val keys = selector.selectedKeys()
          val iterator = keys.iterator()
          while (iterator.hasNext) {
            val key = iterator.next()
            iterator.remove()
            try {
              val event = keyToEvent(key)
              val result = processor.process(event)
              handler.handle(result)
            } catch {
              case e: Exception =>
                val ch = try { key.channel() } catch { case _: Exception => null }
                Logger.error(s"Error processing event on channel $ch: ${e.getMessage}", e)
            }
          }
        }
      }
    }
  }


}

object NIOEventSource {
  def apply(selector: Selector) : NIOEventSource = {
    new NIOEventSource(selector)
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
