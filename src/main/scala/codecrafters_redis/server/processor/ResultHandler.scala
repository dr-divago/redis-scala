package codecrafters_redis.server.processor

import codecrafters_redis.server.{DataReceived, EventResult}

trait ResultHandler {
  def handle(result: EventResult): Unit
}

