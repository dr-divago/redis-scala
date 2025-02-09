package codecrafters_redis.db

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

sealed trait Expiration
case class ExpiresAt(time : Long) extends Expiration
case class NeverExpires() extends Expiration


class InMemoryDB {
  private val db = new ConcurrentHashMap[String, String]()
  private val scheduler = Executors.newScheduledThreadPool(1)

  def add(key: String, value: String, expiration: Expiration): Unit = {
    db.put(key, value)
    expiration match {
      case ExpiresAt(time) =>
        scheduler.schedule(new Runnable {
          override def run(): Unit = {
            db.remove(key)
          }
        }, time, TimeUnit.MILLISECONDS)
      case NeverExpires() =>
    }
  }

  def get(key: String): Option[String] = Option(db.get(key))

  def shutdown(): Unit = {
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        scheduler.shutdownNow()
    }
  }
}
