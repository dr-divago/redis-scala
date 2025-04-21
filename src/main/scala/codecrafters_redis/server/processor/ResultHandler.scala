package codecrafters_redis.server.processor

import codecrafters_redis.server.EventResult

trait ResultHandler {
  def handle(result: EventResult): Unit
}

class RedisResultHandler extends ResultHandler {
  def handle(result: EventResult): Unit = {
    // Handle the result directly (store in Redis, etc.)
  }
}

