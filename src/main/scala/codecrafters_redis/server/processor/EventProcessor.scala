package codecrafters_redis.server.processor

import codecrafters_redis.server.{EventResult, SocketEvent}

trait EventProcessor {
  def process(event: SocketEvent): EventResult
}

